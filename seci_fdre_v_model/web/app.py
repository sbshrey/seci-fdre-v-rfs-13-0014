"""Flask control room for managing local SECI FDRE-V studies."""

from __future__ import annotations

import argparse
import json
import threading
from pathlib import Path
from queue import Empty, Queue
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
    stream_with_context,
    url_for,
)

from seci_fdre_v_model.web.services import (
    artifact_label,
    build_dataset_chart_cards,
    chart_dataset_options,
    dataset_label,
    default_preview_artifact,
    ensure_workspace_ready,
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
    run_study,
    save_project_form,
    store_uploaded_input,
)


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

    @app.context_processor
    def inject_nav_context() -> dict[str, Any]:
        state = workspace()
        return {
            "workspace_root": str(state.root),
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
    def run_study_stream() -> Response:
        state = workspace()

        def generate():
            queue: Queue[tuple[str, Any, Any, Any]] = Queue()

            def worker() -> None:
                try:
                    def emit(stage: str, pct: float, detail: str) -> None:
                        queue.put(("progress", stage, pct, detail))

                    record = run_study(state, progress_callback=emit, dump_sections=False)
                    queue.put(("done", record.run_id, None, None))
                except Exception as exc:
                    queue.put(("error", str(exc), None, None))

            thread = threading.Thread(target=worker, daemon=True)
            thread.start()

            while True:
                try:
                    item = queue.get(timeout=0.25)
                except Empty:
                    yield ""
                    continue
                kind = item[0]
                if kind == "progress":
                    _, stage, pct, detail = item
                    yield json.dumps({"stage": stage, "pct": pct, "detail": detail}) + "\n"
                elif kind == "done":
                    _, run_id, _, _ = item
                    yield json.dumps(
                        {
                            "done": True,
                            "redirect": url_for("run_dashboard", run_id=run_id),
                        }
                    ) + "\n"
                    break
                else:
                    _, error, _, _ = item
                    yield json.dumps({"error": error}) + "\n"
                    break

            thread.join()

        return Response(stream_with_context(generate()), mimetype="application/x-ndjson")

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
        cards = build_dataset_chart_cards(record, dataset)
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
        preview = (
            load_table_preview(record, preview_artifact, page=page, page_size=page_size)
            if preview_artifact
            else None
        )
        chart_cards = (
            build_dataset_chart_cards(record, selected_chart_dataset)
            if selected_chart_dataset
            else []
        )
        return render_template(
            "dashboard.html",
            active_page="dashboard",
            run_records=run_records,
            selected_run=record,
            metric_cards=load_metric_cards(record),
            energy_table=load_energy_table(record),
            compliance_rows=load_small_table(record, "base_case_profile_compliance_monthly.csv", limit=12),
            case_rows=load_small_table(record, "cases_table.csv", limit=12),
            cross_rows=load_small_table(record, "sensitivity_cross_table.csv", limit=12),
            chart_cards=[card for card in chart_cards if card.svg],
            chart_options=chart_options,
            selected_chart_dataset=selected_chart_dataset,
            preview=preview,
            preview_artifact=preview_artifact,
            artifact_label=artifact_label,
            dataset_label=dataset_label,
        )

    return app


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
