"""Storage backends for local and hosted control-room deployments."""

from __future__ import annotations

import json
import os
import secrets
import shutil
import tempfile
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from seci_fdre_v_model.runtime import repo_root, resolve_seed_source_config_path
from seci_fdre_v_model.web.auth import AuthenticatedUser, is_admin, normalize_email
from seci_fdre_v_model.web.models import BackgroundJob, RunArtifactIndex, RunRecord, WorkspaceState
from seci_fdre_v_model.web.services import (
    StudyCancelledError,
    create_run_snapshot,
    ensure_workspace_ready,
    execute_run_snapshot,
)


def build_storage_backend(
    workspace_root: str | Path | None,
    *,
    source_config_path: str | Path | None,
    multi_user: bool = False,
) -> "LocalStorageBackend | CloudStorageBackend":
    backend = (os.environ.get("SECI_FDRE_V_STORAGE_BACKEND") or "local").strip().lower()
    if backend == "aws":
        return CloudStorageBackend.from_env(source_config_path=source_config_path)
    if backend not in {"", "local"}:
        raise ValueError(f"Unsupported SECI_FDRE_V_STORAGE_BACKEND: {backend}")
    return LocalStorageBackend(
        workspace_root=workspace_root,
        source_config_path=source_config_path,
        multi_user=multi_user,
    )


class LocalStorageBackend:
    name = "local"
    uses_external_worker = False
    supports_sharing = False

    def __init__(
        self,
        *,
        workspace_root: str | Path | None,
        source_config_path: str | Path | None,
        multi_user: bool = False,
    ) -> None:
        self.workspace_root = _resolve_base_workspace_root(workspace_root)
        self.source_config_path = source_config_path
        self.multi_user = multi_user

    def workspace_for_user(self, user: AuthenticatedUser | None) -> WorkspaceState:
        root = self.workspace_root
        if self.multi_user and user is not None:
            root = root / "users" / user.user_key
        return ensure_workspace_ready(root, source_config_path=self.source_config_path)

    def sync_workspace_after_mutation(self, user: AuthenticatedUser | None, state: WorkspaceState) -> None:
        return None


class CloudStorageBackend:
    name = "aws"
    uses_external_worker = True
    supports_sharing = True

    def __init__(
        self,
        *,
        bucket: str,
        table_prefix: str,
        region_name: str,
        source_config_path: str | Path | None,
        cache_root: str | Path | None = None,
    ) -> None:
        if not bucket:
            raise ValueError("SECI_FDRE_V_S3_BUCKET is required when SECI_FDRE_V_STORAGE_BACKEND=aws.")
        try:
            import boto3
        except ImportError as exc:  # pragma: no cover - exercised only when aws mode lacks deps
            raise RuntimeError("Install boto3 to use SECI_FDRE_V_STORAGE_BACKEND=aws.") from exc

        self.bucket = bucket
        self.table_prefix = table_prefix
        self.region_name = region_name
        self.source_config_path = source_config_path or resolve_seed_source_config_path()
        self.cache_root = Path(cache_root or os.environ.get("SECI_FDRE_V_CLOUD_CACHE") or tempfile.gettempdir()) / "seci-fdre-v-cloud"
        self.cache_root.mkdir(parents=True, exist_ok=True)
        self.s3 = boto3.client("s3", region_name=region_name)
        self.ddb = boto3.resource("dynamodb", region_name=region_name)
        self.users_table = self.ddb.Table(f"{table_prefix}Users")
        self.runs_table = self.ddb.Table(f"{table_prefix}Runs")
        self.artifact_sets_table = self.ddb.Table(f"{table_prefix}ArtifactSets")
        self.shares_table = self.ddb.Table(f"{table_prefix}Shares")

    @classmethod
    def from_env(cls, *, source_config_path: str | Path | None) -> "CloudStorageBackend":
        return cls(
            bucket=os.environ.get("SECI_FDRE_V_S3_BUCKET") or "",
            table_prefix=os.environ.get("SECI_FDRE_V_DDB_TABLE_PREFIX") or "SeciFdreV",
            region_name=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "ap-south-1",
            source_config_path=source_config_path,
        )

    def workspace_for_user(self, user: AuthenticatedUser | None) -> WorkspaceState:
        user = _require_user(user)
        self.ensure_user(user)
        self.import_pending_shares(user)
        root = self._user_workspace_cache(user.user_key)
        self._download_prefix(f"users/{user.user_key}/workspace/", root)
        remote_exists = self._object_exists(f"users/{user.user_key}/workspace/config/project.yaml")
        state = ensure_workspace_ready(root, source_config_path=self.source_config_path)
        if not remote_exists:
            self.sync_workspace_after_mutation(user, state)
        return state

    def sync_workspace_after_mutation(self, user: AuthenticatedUser | None, state: WorkspaceState) -> None:
        user = _require_user(user)
        self.ensure_user(user)
        self._upload_tree(state.config_dir, f"users/{user.user_key}/workspace/config")
        self._upload_tree(state.inputs_dir, f"users/{user.user_key}/workspace/inputs")

    def ensure_user(self, user: AuthenticatedUser) -> None:
        now = _iso_now()
        self.users_table.update_item(
            Key={"user_key": user.user_key},
            UpdateExpression=(
                "SET auth0_sub = :sub, email = :email, display_name = :name, "
                "updated_at = :now, created_at = if_not_exists(created_at, :now)"
            ),
            ExpressionAttributeValues=_ddb_safe(
                {
                    ":sub": user.subject,
                    ":email": user.email,
                    ":name": user.name,
                    ":now": now,
                }
            ),
        )

    def current_job(self, user: AuthenticatedUser | None) -> BackgroundJob | None:
        user = _require_user(user)
        for item in self._runs_for_owner(user.user_key):
            if str(item.get("status")) in {"queued", "running", "cancelling"}:
                return _background_job_from_run_item(item)
        return None

    def queue_study(self, user: AuthenticatedUser | None, *, study_profile: str) -> BackgroundJob:
        user = _require_user(user)
        if self.current_job(user) is not None:
            raise RuntimeError("A study is already running. Stop it before starting another job.")
        state = self.workspace_for_user(user)
        run_id, run_dir, _config_path, _package_dir = create_run_snapshot(state, study_profile=study_profile)
        artifact_set_id = f"aset-{run_id}-{secrets.token_hex(4)}"
        s3_prefix = f"artifact-sets/{artifact_set_id}"
        run_json_path = run_dir / "run.json"
        run_payload = json.loads(run_json_path.read_text(encoding="utf-8"))
        run_payload.update(
            {
                "status": "queued",
                "artifact_set_id": artifact_set_id,
                "owner_key": user.user_key,
                "owner_email": user.email,
                "stage": "Queued",
                "pct": 0.0,
                "detail": f"Study queued ({study_profile} config snapshot).",
                "updated_at": _iso_now(),
            }
        )
        run_json_path.write_text(json.dumps(run_payload, indent=2, sort_keys=True), encoding="utf-8")
        self._upload_tree(run_dir, s3_prefix)

        artifact_item = {
            "artifact_set_id": artifact_set_id,
            "owner_key": user.user_key,
            "s3_prefix": s3_prefix,
            "ref_count": 1,
            "status": "queued",
            "artifacts": [],
            "created_at": run_payload["started_at"],
            "updated_at": run_payload["updated_at"],
        }
        run_item = {
            "owner_key": user.user_key,
            "run_id": run_id,
            "artifact_set_id": artifact_set_id,
            "status": "queued",
            "plant_name": run_payload.get("plant_name", ""),
            "started_at": run_payload.get("started_at"),
            "finished_at": None,
            "error": None,
            "summary_metrics": {},
            "artifacts": [],
            "stage": "Queued",
            "pct": 0.0,
            "detail": run_payload["detail"],
            "study_profile": study_profile,
            "owner_email": user.email,
            "updated_at": run_payload["updated_at"],
        }
        self.artifact_sets_table.put_item(Item=_ddb_safe(artifact_item), ConditionExpression="attribute_not_exists(artifact_set_id)")
        self.runs_table.put_item(Item=_ddb_safe(run_item), ConditionExpression="attribute_not_exists(owner_key) AND attribute_not_exists(run_id)")
        return _background_job_from_run_item(run_item)

    def request_cancel(self, user: AuthenticatedUser | None) -> BackgroundJob:
        user = _require_user(user)
        job = self.current_job(user)
        if job is None or job.run_id is None:
            raise RuntimeError("No study is currently running.")
        response = self.runs_table.update_item(
            Key={"owner_key": user.user_key, "run_id": job.run_id},
            UpdateExpression=(
                "SET #status = :status, #stage = :stage, detail = :detail, "
                "updated_at = :now, cancel_requested = :true"
            ),
            ExpressionAttributeNames={"#status": "status", "#stage": "stage"},
            ExpressionAttributeValues=_ddb_safe(
                {
                    ":status": "cancelling",
                    ":stage": "Cancelling",
                    ":detail": "Cancellation requested. Waiting for the worker to stop.",
                    ":now": _iso_now(),
                    ":true": True,
                }
            ),
            ReturnValues="ALL_NEW",
        )
        return _background_job_from_run_item(_plain(response["Attributes"]))

    def list_run_records(self, user: AuthenticatedUser | None) -> list[RunRecord]:
        user = _require_user(user)
        records = [self._record_from_run_item(item, materialize=False) for item in self._runs_for_owner(user.user_key)]
        records.sort(key=lambda item: item.started_at, reverse=True)
        return records

    def get_latest_run_record(self, user: AuthenticatedUser | None) -> RunRecord | None:
        records = self.list_run_records(user)
        return records[0] if records else None

    def get_run_record(
        self,
        user: AuthenticatedUser | None,
        run_id: str,
        *,
        owner_key: str | None = None,
    ) -> RunRecord:
        user = _require_user(user)
        owner = owner_key or user.user_key
        if owner != user.user_key and not is_admin(user):
            raise FileNotFoundError(f"Run not found: {run_id}")
        response = self.runs_table.get_item(Key={"owner_key": owner, "run_id": run_id}, ConsistentRead=True)
        item = _plain(response.get("Item"))
        if not item:
            raise FileNotFoundError(f"Run not found: {run_id}")
        return self._record_from_run_item(item, materialize=True)

    def delete_run(self, user: AuthenticatedUser | None, run_id: str) -> None:
        user = _require_user(user)
        response = self.runs_table.get_item(Key={"owner_key": user.user_key, "run_id": run_id}, ConsistentRead=True)
        item = _plain(response.get("Item"))
        if not item:
            raise FileNotFoundError(f"Run not found: {run_id}")
        artifact_set_id = str(item["artifact_set_id"])
        self.runs_table.delete_item(Key={"owner_key": user.user_key, "run_id": run_id})
        remaining = self._decrement_artifact_ref(artifact_set_id)
        if remaining <= 0:
            artifact = self._get_artifact_set(artifact_set_id)
            if artifact:
                self._delete_prefix(str(artifact.get("s3_prefix") or f"artifact-sets/{artifact_set_id}"))
            self.artifact_sets_table.delete_item(Key={"artifact_set_id": artifact_set_id})

    def copy_run_to_email(
        self,
        user: AuthenticatedUser | None,
        run_id: str,
        recipient_email: str,
        *,
        source_owner_key: str | None = None,
    ) -> dict[str, Any]:
        user = _require_user(user)
        target_email = normalize_email(recipient_email)
        if not target_email:
            raise ValueError("Recipient email is required.")
        owner = source_owner_key or user.user_key
        source = self.get_run_record(user, run_id, owner_key=owner)
        if source.status != "completed":
            raise ValueError("Only completed runs can be copied.")
        if owner != user.user_key and not is_admin(user):
            raise PermissionError("Only the owner or an admin can copy this run.")
        artifact_set_id = str(source.artifact_set_id)
        target_user = self._find_user_by_email(target_email)
        new_run_id = _new_id()
        now = _iso_now()
        copied_from = {
            "owner_key": owner,
            "owner_email": source.owner_email,
            "run_id": run_id,
            "copied_at": now,
        }
        self._increment_artifact_ref(artifact_set_id)
        try:
            if target_user:
                self.runs_table.put_item(
                    Item=_ddb_safe(
                        {
                            "owner_key": target_user["user_key"],
                            "run_id": new_run_id,
                            "artifact_set_id": artifact_set_id,
                            "status": "completed",
                            "plant_name": source.plant_name,
                            "started_at": now,
                            "finished_at": now,
                            "error": None,
                            "summary_metrics": source.summary_metrics,
                            "artifacts": _artifact_payloads(source.artifacts),
                            "owner_email": target_user["email"],
                            "copied_from": copied_from,
                            "updated_at": now,
                        }
                    ),
                    ConditionExpression="attribute_not_exists(owner_key) AND attribute_not_exists(run_id)",
                )
                return {"status": "copied", "recipient_email": target_email, "run_id": new_run_id}

            share_id = f"share-{_new_id()}"
            self.shares_table.put_item(
                Item=_ddb_safe(
                    {
                        "share_id": share_id,
                        "target_email": target_email,
                        "status": "pending",
                        "source_owner_key": owner,
                        "source_run_id": run_id,
                        "artifact_set_id": artifact_set_id,
                        "recipient_run_id": new_run_id,
                        "plant_name": source.plant_name,
                        "summary_metrics": source.summary_metrics,
                        "artifacts": _artifact_payloads(source.artifacts),
                        "copied_from": copied_from,
                        "created_at": now,
                        "updated_at": now,
                    }
                ),
                ConditionExpression="attribute_not_exists(share_id)",
            )
            return {"status": "pending", "recipient_email": target_email, "share_id": share_id}
        except Exception:
            self._decrement_artifact_ref(artifact_set_id)
            raise

    def import_pending_shares(self, user: AuthenticatedUser) -> int:
        imported = 0
        for share in self._pending_shares_for_email(user.email):
            now = _iso_now()
            self.runs_table.put_item(
                Item=_ddb_safe(
                    {
                        "owner_key": user.user_key,
                        "run_id": share["recipient_run_id"],
                        "artifact_set_id": share["artifact_set_id"],
                        "status": "completed",
                        "plant_name": share.get("plant_name", ""),
                        "started_at": now,
                        "finished_at": now,
                        "error": None,
                        "summary_metrics": share.get("summary_metrics", {}),
                        "artifacts": share.get("artifacts", []),
                        "owner_email": user.email,
                        "copied_from": share.get("copied_from"),
                        "updated_at": now,
                    }
                ),
                ConditionExpression="attribute_not_exists(owner_key) AND attribute_not_exists(run_id)",
            )
            self.shares_table.update_item(
                Key={"share_id": share["share_id"]},
                UpdateExpression="SET #status = :status, imported_owner_key = :owner, imported_at = :now, updated_at = :now",
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues=_ddb_safe({":status": "imported", ":owner": user.user_key, ":now": now}),
            )
            imported += 1
        return imported

    def admin_run_records(self, user: AuthenticatedUser | None) -> list[RunRecord]:
        user = _require_user(user)
        if not is_admin(user):
            raise PermissionError("Admin access required.")
        records = [self._record_from_run_item(_plain(item), materialize=False) for item in _scan_all(self.runs_table)]
        records.sort(key=lambda item: item.started_at, reverse=True)
        return records

    def claim_next_queued_run(self) -> dict[str, Any] | None:
        queued = sorted(
            (_plain(item) for item in _scan_all(self.runs_table) if str(item.get("status")) == "queued"),
            key=lambda item: str(item.get("started_at") or ""),
        )
        for item in queued:
            try:
                response = self.runs_table.update_item(
                    Key={"owner_key": item["owner_key"], "run_id": item["run_id"]},
                    ConditionExpression="#status = :queued",
                    UpdateExpression=(
                        "SET #status = :running, #stage = :stage, pct = :pct, "
                        "detail = :detail, updated_at = :now"
                    ),
                    ExpressionAttributeNames={"#status": "status", "#stage": "stage"},
                    ExpressionAttributeValues=_ddb_safe(
                        {
                            ":queued": "queued",
                            ":running": "running",
                            ":stage": "Starting",
                            ":pct": 1.0,
                            ":detail": "Worker claimed the run.",
                            ":now": _iso_now(),
                        }
                    ),
                    ReturnValues="ALL_NEW",
                )
                return _plain(response["Attributes"])
            except Exception:
                continue
        return None

    def execute_claimed_run(self, item: dict[str, Any]) -> RunRecord:
        item = _plain(item)
        owner_key = str(item["owner_key"])
        run_id = str(item["run_id"])
        artifact_set_id = str(item["artifact_set_id"])
        artifact = self._get_artifact_set(artifact_set_id)
        if not artifact:
            raise FileNotFoundError(f"Artifact set not found: {artifact_set_id}")
        root = self.cache_root / "worker" / run_id
        if root.exists():
            shutil.rmtree(root)
        run_dir = root / "runs" / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        self._download_prefix(str(artifact["s3_prefix"]) + "/", run_dir)
        (run_dir / "package").mkdir(parents=True, exist_ok=True)
        (run_dir / "run.json").write_text(json.dumps(item, indent=2, sort_keys=True), encoding="utf-8")
        state = WorkspaceState(
            root=root,
            config_dir=root / "config",
            config_path=root / "config" / "project.yaml",
            inputs_dir=root / "inputs",
            runs_dir=root / "runs",
            metadata_path=root / "inputs" / "metadata.json",
            source_config_path=Path(self.source_config_path),
        )

        def progress(stage: str, pct: float, detail: str) -> None:
            latest = self.runs_table.get_item(Key={"owner_key": owner_key, "run_id": run_id}, ConsistentRead=True).get("Item", {})
            if str(latest.get("status")) == "cancelling":
                raise StudyCancelledError("Cancelled by user.")
            self._update_cloud_run(owner_key, run_id, status="running", stage=stage, pct=pct, detail=detail)

        try:
            record = execute_run_snapshot(
                state,
                run_id=run_id,
                run_dir=run_dir,
                config_path=run_dir / "config" / "project.yaml",
                package_dir=run_dir / "package",
                progress_callback=progress,
            )
            self._upload_tree(run_dir, str(artifact["s3_prefix"]))
            artifacts = _artifact_payloads(record.artifacts)
            self._update_cloud_run(
                owner_key,
                run_id,
                status=record.status,
                stage="Completed",
                pct=100.0,
                detail=f"Run {run_id} completed.",
                finished_at=record.finished_at,
                error=record.error,
                summary_metrics=record.summary_metrics,
                artifacts=artifacts,
            )
            self.artifact_sets_table.update_item(
                Key={"artifact_set_id": artifact_set_id},
                UpdateExpression="SET #status = :status, artifacts = :artifacts, updated_at = :now",
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues=_ddb_safe({":status": "completed", ":artifacts": artifacts, ":now": _iso_now()}),
            )
            return self.get_run_record(
                AuthenticatedUser(subject=f"worker:{owner_key}", email=str(item.get("owner_email") or ""), name="worker", user_key=owner_key),
                run_id,
            )
        except Exception as exc:
            status = "cancelled" if isinstance(exc, StudyCancelledError) else "failed"
            self._update_cloud_run(
                owner_key,
                run_id,
                status=status,
                stage=status.title(),
                pct=float(item.get("pct") or 0.0),
                detail=str(exc),
                finished_at=_iso_now(),
                error=str(exc),
            )
            self.artifact_sets_table.update_item(
                Key={"artifact_set_id": artifact_set_id},
                UpdateExpression="SET #status = :status, updated_at = :now",
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues=_ddb_safe({":status": status, ":now": _iso_now()}),
            )
            raise

    def _runs_for_owner(self, owner_key: str) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        kwargs: dict[str, Any] = {
            "KeyConditionExpression": _key("owner_key").eq(owner_key),
            "ConsistentRead": True,
        }
        while True:
            response = self.runs_table.query(**kwargs)
            items.extend(response.get("Items", []))
            last_key = response.get("LastEvaluatedKey")
            if not last_key:
                return [_plain(item) for item in items]
            kwargs["ExclusiveStartKey"] = last_key

    def _record_from_run_item(self, item: dict[str, Any], *, materialize: bool) -> RunRecord:
        item = _plain(item)
        artifact_set_id = str(item.get("artifact_set_id") or "")
        run_dir = self.cache_root / "artifact_sets" / artifact_set_id
        if materialize and artifact_set_id:
            artifact = self._get_artifact_set(artifact_set_id)
            if artifact:
                self._download_prefix(str(artifact.get("s3_prefix")) + "/", run_dir)
        package_dir = run_dir / "package"
        artifacts = [
            RunArtifactIndex(
                relative_path=str(artifact["relative_path"]),
                absolute_path=package_dir / str(artifact["relative_path"]),
                size_kb=float(artifact.get("size_kb", 0.0)),
                modified_at=str(artifact.get("modified_at", "")),
                is_tabular=bool(artifact.get("is_tabular", False)),
            )
            for artifact in item.get("artifacts", [])
        ]
        return RunRecord(
            run_id=str(item["run_id"]),
            run_dir=run_dir,
            package_dir=package_dir,
            config_path=run_dir / "config" / "project.yaml",
            status=str(item.get("status", "unknown")),
            plant_name=str(item.get("plant_name", "")),
            started_at=str(item.get("started_at", "")),
            finished_at=item.get("finished_at"),
            artifacts=artifacts,
            summary_metrics=dict(item.get("summary_metrics", {})),
            error=item.get("error"),
            owner_key=item.get("owner_key"),
            owner_email=item.get("owner_email"),
            artifact_set_id=artifact_set_id,
            copied_from=item.get("copied_from"),
        )

    def _update_cloud_run(
        self,
        owner_key: str,
        run_id: str,
        *,
        status: str,
        stage: str,
        pct: float,
        detail: str,
        finished_at: str | None = None,
        error: str | None = None,
        summary_metrics: dict[str, Any] | None = None,
        artifacts: list[dict[str, Any]] | None = None,
    ) -> None:
        names = {"#status": "status", "#stage": "stage"}
        values: dict[str, Any] = {
            ":status": status,
            ":stage": stage,
            ":pct": float(pct),
            ":detail": detail,
            ":now": _iso_now(),
        }
        assignments = ["#status = :status", "#stage = :stage", "pct = :pct", "detail = :detail", "updated_at = :now"]
        if finished_at is not None:
            values[":finished_at"] = finished_at
            assignments.append("finished_at = :finished_at")
        if error is not None:
            values[":error"] = error
            assignments.append("error = :error")
        if summary_metrics is not None:
            values[":summary_metrics"] = summary_metrics
            assignments.append("summary_metrics = :summary_metrics")
        if artifacts is not None:
            values[":artifacts"] = artifacts
            assignments.append("artifacts = :artifacts")
        self.runs_table.update_item(
            Key={"owner_key": owner_key, "run_id": run_id},
            UpdateExpression="SET " + ", ".join(assignments),
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=_ddb_safe(values),
        )

    def _get_artifact_set(self, artifact_set_id: str) -> dict[str, Any] | None:
        response = self.artifact_sets_table.get_item(Key={"artifact_set_id": artifact_set_id}, ConsistentRead=True)
        item = response.get("Item")
        return _plain(item) if item else None

    def _increment_artifact_ref(self, artifact_set_id: str) -> None:
        self.artifact_sets_table.update_item(
            Key={"artifact_set_id": artifact_set_id},
            UpdateExpression="SET updated_at = :now ADD ref_count :one",
            ExpressionAttributeValues=_ddb_safe({":one": 1, ":now": _iso_now()}),
        )

    def _decrement_artifact_ref(self, artifact_set_id: str) -> int:
        response = self.artifact_sets_table.update_item(
            Key={"artifact_set_id": artifact_set_id},
            UpdateExpression="SET updated_at = :now ADD ref_count :minus_one",
            ExpressionAttributeValues=_ddb_safe({":minus_one": -1, ":now": _iso_now()}),
            ReturnValues="UPDATED_NEW",
        )
        return int(_plain(response["Attributes"]).get("ref_count", 0))

    def _find_user_by_email(self, email: str) -> dict[str, Any] | None:
        for item in _scan_all(self.users_table):
            plain = _plain(item)
            if plain.get("email") == email:
                return plain
        return None

    def _pending_shares_for_email(self, email: str) -> list[dict[str, Any]]:
        return [
            _plain(item)
            for item in _scan_all(self.shares_table)
            if item.get("target_email") == email and item.get("status") == "pending"
        ]

    def _user_workspace_cache(self, user_key: str) -> Path:
        return self.cache_root / "users" / user_key / "workspace"

    def _object_exists(self, key: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except Exception:
            return False

    def _upload_tree(self, local_root: Path, prefix: str) -> None:
        if not local_root.exists():
            return
        for path in local_root.rglob("*"):
            if path.is_file():
                key = f"{prefix.rstrip('/')}/{path.relative_to(local_root).as_posix()}"
                self.s3.upload_file(str(path), self.bucket, key)

    def _download_prefix(self, prefix: str, local_root: Path) -> None:
        local_root.mkdir(parents=True, exist_ok=True)
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = str(obj["Key"])
                if key.endswith("/"):
                    continue
                relative = key[len(prefix) :].lstrip("/")
                if not relative:
                    continue
                target = local_root / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                self.s3.download_file(self.bucket, key, str(target))

    def _delete_prefix(self, prefix: str) -> None:
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix.rstrip("/") + "/"):
            objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
            if objects:
                self.s3.delete_objects(Bucket=self.bucket, Delete={"Objects": objects})


def _resolve_base_workspace_root(workspace_root: str | Path | None) -> Path:
    if workspace_root is not None:
        return Path(workspace_root).expanduser().resolve()
    env_root = os.environ.get("SECI_FDRE_V_WORKSPACE")
    if env_root:
        return Path(env_root).expanduser().resolve()
    return (repo_root() / ".workspace").resolve()


def _require_user(user: AuthenticatedUser | None) -> AuthenticatedUser:
    if user is None:
        raise PermissionError("Authentication is required.")
    return user


def _background_job_from_run_item(item: dict[str, Any]) -> BackgroundJob:
    item = _plain(item)
    return BackgroundJob(
        run_id=str(item.get("run_id") or ""),
        status=str(item.get("status") or "unknown"),
        stage=str(item.get("stage") or item.get("status") or "Queued"),
        pct=float(item.get("pct") or 0.0),
        detail=str(item.get("detail") or ""),
        completed_cases=None,
        total_cases=None,
        current_case_id=None,
        started_at=item.get("started_at"),
        updated_at=item.get("updated_at"),
        finished_at=item.get("finished_at"),
        error=item.get("error"),
        cancel_requested=bool(item.get("cancel_requested", False)),
        study_profile=item.get("study_profile"),
    )


def _artifact_payloads(artifacts: list[RunArtifactIndex]) -> list[dict[str, Any]]:
    return [
        {
            "relative_path": artifact.relative_path,
            "size_kb": artifact.size_kb,
            "modified_at": artifact.modified_at,
            "is_tabular": artifact.is_tabular,
        }
        for artifact in artifacts
    ]


def _scan_all(table: Any) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    kwargs: dict[str, Any] = {}
    while True:
        response = table.scan(**kwargs)
        items.extend(response.get("Items", []))
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            return items
        kwargs["ExclusiveStartKey"] = last_key


def _new_id() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y%m%d-%H%M%S") + f"-{secrets.token_hex(3)}"


def _iso_now() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat()


def _ddb_safe(value: Any) -> Any:
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, dict):
        return {key: _ddb_safe(item) for key, item in value.items() if item is not None}
    if isinstance(value, list):
        return [_ddb_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_ddb_safe(item) for item in value]
    return value


def _plain(value: Any) -> Any:
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    if isinstance(value, dict):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_plain(item) for item in value]
    return value


def _key(name: str) -> Any:
    from boto3.dynamodb.conditions import Key

    return Key(name)
