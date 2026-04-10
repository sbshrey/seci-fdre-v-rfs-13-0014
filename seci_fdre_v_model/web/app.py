"""Flask control room for managing local SECI FDRE-V studies."""

from __future__ import annotations

import argparse
import re
import threading
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from flask import (
    Flask,
    Response,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)

from seci_fdre_v_model.profile_templates import SUPPORTED_TENDER_PROFILES
from seci_fdre_v_model.web.models import BackgroundJob
from seci_fdre_v_model.web.services import (
    StudyCancelledError,
    artifact_label,
    build_dataset_chart_cards,
    chart_dataset_options,
    create_run_snapshot,
    dataset_label,
    delete_run_record,
    default_preview_artifact,
    ensure_workspace_ready,
    execute_run_snapshot,
    generate_active_inputs,
    get_latest_run_record,
    get_run_record,
    list_managed_inputs,
    list_run_records,
    load_energy_table,
    load_metric_cards,
    load_project_config,
    load_small_table,
    load_table_preview,
    resolve_run_artifact,
    save_project_form,
    store_uploaded_input,
    update_run_status,
)

CONFIG_SELECT_OPTIONS = {
    "simulation.preprocessing.frequency": [("1m", "1 minute")],
    "simulation.preprocessing.gap_fill": [
        ("linear_interpolate", "Linear interpolate"),
        ("zero", "Zero fill"),
    ],
    "simulation.preprocessing.simulation_dtype": [
        ("float32", "float32"),
        ("float64", "float64"),
    ],
    "simulation.load.profile_mode": [
        ("template", "Template"),
        ("flat", "Flat"),
    ],
    "simulation.load.profile_template_id": [
        (template_id, template.source_doc)
        for template_id, template in SUPPORTED_TENDER_PROFILES.items()
    ],
}


class StudyJobManager:
    def __init__(self, workspace_factory: Any) -> None:
        self._workspace_factory = workspace_factory
        self._lock = threading.Lock()
        self._job: BackgroundJob | None = None
        self._thread: threading.Thread | None = None
        self._cancel_event: threading.Event | None = None

    def current_job(self) -> BackgroundJob | None:
        with self._lock:
            return self._job

    def start(self, *, dump_sections: bool = False) -> BackgroundJob:
        with self._lock:
            if self._job is not None and self._job.is_active:
                raise RuntimeError("A study is already running. Stop it before starting another job.")
            state = self._workspace_factory()
            run_id, run_dir, config_path, package_dir = create_run_snapshot(state)
            job = BackgroundJob(
                run_id=run_id,
                status="queued",
                stage="Queued",
                pct=0.0,
                detail="Study queued in the background.",
                completed_cases=None,
                total_cases=None,
                current_case_id=None,
                started_at=_iso_now(),
                updated_at=_iso_now(),
                finished_at=None,
                error=None,
                cancel_requested=False,
            )
            cancel_event = threading.Event()
            self._job = job
            self._cancel_event = cancel_event

        def emit(stage: str, pct: float, detail: str) -> None:
            if cancel_event.is_set():
                raise StudyCancelledError("Cancelled by user.")
            self._update_job(status="running", stage=stage, pct=pct, detail=detail, error=None)

        def worker() -> None:
            try:
                if cancel_event.is_set():
                    raise StudyCancelledError("Cancelled by user.")
                self._update_job(status="running", stage="Starting", pct=1.0, detail="Preparing the study package.")
                record = execute_run_snapshot(
                    state,
                    run_id=run_id,
                    run_dir=run_dir,
                    config_path=config_path,
                    package_dir=package_dir,
                    progress_callback=emit,
                    dump_sections=dump_sections,
                )
                self._update_job(
                    status=record.status,
                    stage="Completed",
                    pct=100.0,
                    detail=f"Run {record.run_id} completed.",
                    finished_at=record.finished_at or _iso_now(),
                    error=record.error,
                )
            except StudyCancelledError as exc:
                try:
                    update_run_status(
                        state,
                        run_id,
                        status="cancelled",
                        finished_at=_iso_now(),
                        error=str(exc),
                    )
                except Exception:
                    pass
                self._update_job(
                    status="cancelled",
                    stage="Cancelled",
                    pct=self.current_job().pct if self.current_job() else 0.0,
                    detail="Run cancelled by user.",
                    finished_at=_iso_now(),
                    error=str(exc),
                )
            except Exception as exc:
                try:
                    record = get_run_record(state, run_id)
                    finished_at = record.finished_at or _iso_now()
                    error = record.error or str(exc)
                except Exception:
                    finished_at = _iso_now()
                    error = str(exc)
                self._update_job(
                    status="failed",
                    stage="Failed",
                    pct=self.current_job().pct if self.current_job() else 0.0,
                    detail=error,
                    finished_at=finished_at,
                    error=error,
                )
            finally:
                with self._lock:
                    self._thread = None
                    self._cancel_event = None

        thread = threading.Thread(target=worker, daemon=True)
        with self._lock:
            self._thread = thread
        thread.start()
        return job

    def request_cancel(self) -> BackgroundJob:
        with self._lock:
            if self._job is None or not self._job.is_active:
                raise RuntimeError("No study is currently running.")
            if self._cancel_event is not None:
                self._cancel_event.set()
            self._job = replace(
                self._job,
                status="cancelling",
                stage="Cancelling",
                detail="Cancellation requested. Waiting for the current step to stop.",
                completed_cases=None,
                total_cases=None,
                current_case_id=None,
                cancel_requested=True,
                updated_at=_iso_now(),
            )
            return self._job

    def delete(self, run_id: str) -> None:
        with self._lock:
            if self._job is not None and self._job.run_id == run_id and self._job.is_active:
                raise RuntimeError("Stop the running job before deleting it.")
        state = self._workspace_factory()
        delete_run_record(state, run_id)
        with self._lock:
            if self._job is not None and self._job.run_id == run_id:
                self._job = None

    def _update_job(
        self,
        *,
        status: str | None = None,
        stage: str | None = None,
        pct: float | None = None,
        detail: str | None = None,
        finished_at: str | None = None,
        error: str | None = None,
    ) -> None:
        with self._lock:
            if self._job is None:
                return
            next_stage = stage or self._job.stage
            next_detail = detail or self._job.detail
            completed_cases, total_cases, current_case_id = _parse_case_progress(next_stage, next_detail)
            self._job = replace(
                self._job,
                status=status or self._job.status,
                stage=next_stage,
                pct=float(self._job.pct if pct is None else pct),
                detail=next_detail,
                completed_cases=completed_cases,
                total_cases=total_cases,
                current_case_id=current_case_id,
                finished_at=finished_at if finished_at is not None else self._job.finished_at,
                error=error if error is not None else self._job.error,
                updated_at=_iso_now(),
            )


def _iso_now() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat()


def _parse_case_progress(stage: str, detail: str) -> tuple[int | None, int | None, str | None]:
    if stage not in {"Sensitivity cases", "Sensitivity cross"}:
        return None, None, None
    match = re.search(r"Processed\s+(.+?)\s+\((\d+)/(\d+)\)", detail)
    if not match:
        return None, None, None
    case_id, completed, total = match.groups()
    return int(completed), int(total), case_id


def create_app(
    workspace_root: str | Path | None = None,
    *,
    source_config_path: str | Path | None = None,
) -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.config["SECRET_KEY"] = "seci-fdre-v-local"
    app.config["WORKSPACE_ROOT"] = str(workspace_root) if workspace_root is not None else None
    app.config["SOURCE_CONFIG_PATH"] = str(source_config_path) if source_config_path is not None else None

    def workspace():
        return ensure_workspace_ready(
            app.config.get("WORKSPACE_ROOT"),
            source_config_path=app.config.get("SOURCE_CONFIG_PATH"),
        )

    job_manager = StudyJobManager(workspace)

    @app.context_processor
    def inject_nav_context() -> dict[str, Any]:
        state = workspace()
        return {
            "workspace_root": str(state.root),
            "current_job": job_manager.current_job(),
        }

    @app.get("/health")
    def health() -> Response:
        return jsonify({"status": "ok"})

    @app.get("/")
    def index() -> str | Response:
        state = workspace()
        latest = get_latest_run_record(state)
        if latest is None:
            return render_dashboard(None)
        return redirect(url_for("run_dashboard", run_id=latest.run_id))

    @app.get("/config")
    def config_page() -> str:
        state = workspace()
        project = load_project_config(state)
        return render_template(
            "config.html",
            active_page="config",
            project=project,
            select_options=CONFIG_SELECT_OPTIONS,
        )

    @app.post("/config/save")
    def save_config() -> Response:
        state = workspace()
        try:
            save_project_form(state, request.form.to_dict())
            flash("Configuration saved.", "success")
        except Exception as exc:
            flash(f"Failed to save configuration: {exc}", "error")
        return redirect(url_for("config_page"))

    @app.get("/inputs")
    def inputs_page() -> str:
        state = workspace()
        return render_template(
            "inputs.html",
            active_page="inputs",
            managed_inputs=list_managed_inputs(state),
        )

    @app.post("/inputs/upload/<input_key>")
    def upload_input(input_key: str) -> Response:
        state = workspace()
        try:
            upload = request.files.get("file")
            store_uploaded_input(state, input_key, upload)
            flash("Input file uploaded.", "success")
        except Exception as exc:
            flash(f"Upload failed: {exc}", "error")
        return redirect(url_for("inputs_page"))

    @app.get("/inputs/download/<input_key>")
    def download_input(input_key: str) -> Response:
        state = workspace()
        managed_input = next((item for item in list_managed_inputs(state) if item.key == input_key), None)
        if managed_input is None or not managed_input.exists:
            flash("Input file not found.", "error")
            return redirect(url_for("inputs_page"))
        return send_file(managed_input.absolute_path, as_attachment=True, download_name=managed_input.canonical_name)

    @app.post("/runs/generate")
    def generate_inputs() -> Response:
        state = workspace()
        try:
            generate_active_inputs(state)
            flash("Tender-derived input files generated in the workspace.", "success")
        except Exception as exc:
            flash(f"Failed to generate input files: {exc}", "error")
        return redirect(request.referrer or url_for("inputs_page"))

    @app.post("/runs/study")
    def start_study_job() -> Response:
        try:
            job = job_manager.start(dump_sections=False)
            flash(f"Study started in background. Run ID: {job.run_id}", "success")
        except Exception as exc:
            flash(str(exc), "error")
        return _redirect_back()

    @app.post("/jobs/current/cancel")
    def cancel_current_job() -> Response:
        try:
            job = job_manager.request_cancel()
            flash(f"Cancellation requested for run {job.run_id}.", "success")
        except Exception as exc:
            flash(str(exc), "error")
        return _redirect_back()

    @app.post("/runs/<run_id>/delete")
    def delete_run(run_id: str) -> Response:
        try:
            job_manager.delete(run_id)
            flash(f"Deleted run {run_id}.", "success")
        except Exception as exc:
            flash(str(exc), "error")
        next_target = request.form.get("next")
        if next_target:
            return redirect(next_target)
        return redirect(url_for("runs_page"))

    @app.get("/api/job-status")
    def api_job_status() -> Response:
        job = job_manager.current_job()
        if job is None:
            return jsonify({"job": None, "can_start": True})
        return jsonify(
            {
                "job": {
                    "run_id": job.run_id,
                    "status": job.status,
                    "stage": job.stage,
                    "pct": job.pct,
                    "detail": job.detail,
                    "completed_cases": job.completed_cases,
                    "total_cases": job.total_cases,
                    "current_case_id": job.current_case_id,
                    "started_at": job.started_at,
                    "updated_at": job.updated_at,
                    "finished_at": job.finished_at,
                    "error": job.error,
                    "cancel_requested": job.cancel_requested,
                    "is_active": job.is_active,
                    "run_url": url_for("run_dashboard", run_id=job.run_id) if job.run_id else None,
                    "delete_url": url_for("delete_run", run_id=job.run_id) if job.run_id else None,
                },
                "can_start": not job.is_active,
            }
        )

    @app.get("/runs")
    def runs_page() -> str:
        state = workspace()
        return render_template(
            "runs.html",
            active_page="runs",
            run_records=list_run_records(state),
        )

    @app.get("/runs/<run_id>")
    def run_dashboard(run_id: str) -> str | Response:
        state = workspace()
        try:
            record = get_run_record(state, run_id)
        except FileNotFoundError:
            flash("Run not found.", "error")
            return redirect(url_for("runs_page"))
        return render_dashboard(record)

    @app.get("/runs/<run_id>/artifacts/<path:relative_path>")
    def download_artifact(run_id: str, relative_path: str) -> Response:
        state = workspace()
        try:
            record = get_run_record(state, run_id)
            path = resolve_run_artifact(record, relative_path)
        except Exception:
            flash("Artifact not found.", "error")
            return redirect(url_for("runs_page"))
        return send_file(path, as_attachment=True, download_name=Path(relative_path).name)

    @app.get("/api/charts/<run_id>/<path:dataset>")
    def api_charts(run_id: str, dataset: str) -> Response:
        state = workspace()
        record = get_run_record(state, run_id)
        cards = _safe_call(lambda: build_dataset_chart_cards(record, dataset), default=[])
        return jsonify(
            [
                {"title": card.title, "subtitle": card.subtitle, "svg": card.svg}
                for card in cards
                if card.svg
            ]
        )

    def render_dashboard(record: Any) -> str:
        state = workspace()
        run_records = list_run_records(state)
        if record is None:
            return render_template(
                "dashboard.html",
                active_page="dashboard",
                run_records=run_records,
                selected_run=None,
                metric_cards=[],
                energy_table=[],
                compliance_rows=[],
                case_rows=[],
                cross_rows=[],
            chart_cards=[],
            chart_options=[],
            selected_chart_dataset=None,
            preview=None,
            preview_artifact=None,
            can_start_study=job_manager.current_job() is None or not job_manager.current_job().is_active,
            artifact_label=artifact_label,
            dataset_label=dataset_label,
        )

        preview_artifact = request.args.get("artifact") or default_preview_artifact(record)
        selected_chart_dataset = request.args.get("chart_dataset")
        chart_options = chart_dataset_options(record)
        if not selected_chart_dataset and chart_options:
            selected_chart_dataset = chart_options[0].relative_path
        page = int(request.args.get("page", "1"))
        page_size = int(request.args.get("page_size", "25"))
        preview = _safe_call(
            lambda: load_table_preview(record, preview_artifact, page=page, page_size=page_size),
            default=None,
        ) if preview_artifact else None
        chart_cards = _safe_call(
            lambda: build_dataset_chart_cards(record, selected_chart_dataset),
            default=[],
        ) if selected_chart_dataset else []
        return render_template(
            "dashboard.html",
            active_page="dashboard",
            run_records=run_records,
            selected_run=record,
            metric_cards=_safe_call(lambda: load_metric_cards(record), default=[]),
            energy_table=_safe_call(lambda: load_energy_table(record), default=[]),
            compliance_rows=_safe_call(
                lambda: load_small_table(record, "base_case_profile_compliance_monthly.csv", limit=12),
                default=[],
            ),
            case_rows=_safe_call(lambda: load_small_table(record, "cases_table.csv", limit=12), default=[]),
            cross_rows=_safe_call(
                lambda: load_small_table(record, "sensitivity_cross_table.csv", limit=12),
                default=[],
            ),
            chart_cards=[card for card in chart_cards if card.svg],
            chart_options=chart_options,
            selected_chart_dataset=selected_chart_dataset,
            preview=preview,
            preview_artifact=preview_artifact,
            can_start_study=job_manager.current_job() is None or not job_manager.current_job().is_active,
            artifact_label=artifact_label,
            dataset_label=dataset_label,
        )

    return app


def _redirect_back() -> Response:
    next_target = request.form.get("next")
    if next_target:
        return redirect(next_target)
    return redirect(request.referrer or url_for("index"))


def _safe_call(fn: Any, *, default: Any) -> Any:
    try:
        return fn()
    except Exception:
        return default


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the SECI FDRE-V control room.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=5000, type=int)
    parser.add_argument("--workspace", default=None)
    parser.add_argument("--source-config", default=None)
    args = parser.parse_args(argv)

    app = create_app(args.workspace, source_config_path=args.source_config)
    app.run(host=args.host, port=args.port, debug=False)
    return 0
