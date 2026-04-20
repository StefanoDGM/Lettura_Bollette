from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from flask import Flask, abort, redirect, render_template, request, send_file, url_for
from werkzeug.utils import secure_filename

from run_full_pipeline import full_pipeline

ROOT = Path(__file__).resolve().parents[2]
RUNS_DIR = ROOT / "web_runs"
LOGO_PATH = ROOT / "Logo-Energon-orizz-RGB.jpg"
ALLOWED_SUFFIXES = {".pdf"}
DEFAULT_APP_PATH = "/Lettura_Bollette_Energon"
REPORT_METADATA = {
    "csv": ("Report Estrazione CSV", "estrazione_tutti_mesi.csv"),
    "xlsx": ("Report Estrazione Excel", "estrazione_tutti_mesi.xlsx"),
    "aggregated": ("Report Aggregato Mensile", "bollette_raggruppate.xlsx"),
}


def normalize_app_path(raw_path: str | None) -> str:
    candidate = (raw_path or "").strip() or DEFAULT_APP_PATH
    if candidate == "/":
        return "/"
    if not candidate.startswith("/"):
        candidate = f"/{candidate}"
    candidate = candidate.rstrip("/")
    return candidate or DEFAULT_APP_PATH


def join_app_path(app_path: str, suffix: str) -> str:
    suffix = suffix if suffix.startswith("/") else f"/{suffix}"
    if app_path == "/":
        return suffix
    return f"{app_path}{suffix}"


def create_app() -> Flask:
    app_path = normalize_app_path(os.environ.get("ENERGON_WEB_PATH"))
    app = Flask(
        __name__,
        template_folder="templates",
        static_folder="static",
        static_url_path=join_app_path(app_path, "/static"),
    )
    app.config["ENERGON_WEB_PATH"] = app_path
    RUNS_DIR.mkdir(parents=True, exist_ok=True)

    @app.get(app_path)
    def index():
        job_id = request.args.get("job_id", "").strip()
        result = load_result(job_id) if job_id else None
        return render_template("index.html", result=result, platform_status=get_platform_status(result))

    if app_path != "/":
        @app.get("/")
        def root_redirect():
            return redirect(url_for("index"))

    @app.get(join_app_path(app_path, "/brand/logo"))
    def brand_logo():
        if not LOGO_PATH.exists():
            abort(404)
        return send_file(LOGO_PATH)

    @app.post(join_app_path(app_path, "/run"))
    def run_pipeline_view():
        uploaded_files = request.files.getlist("pdf_files")
        job_id = datetime.now().strftime("%Y%m%d-%H%M%S") + "-" + uuid4().hex[:6]
        job_dir = RUNS_DIR / job_id
        input_dir = job_dir / "input"
        output_dir = job_dir / "output"
        input_dir.mkdir(parents=True, exist_ok=True)
        output_dir.mkdir(parents=True, exist_ok=True)

        saved_files: list[str] = []
        skipped_files: list[str] = []
        for uploaded in uploaded_files:
            original_name = Path(uploaded.filename or "").name
            if not original_name:
                continue
            suffix = Path(original_name).suffix.lower()
            if suffix not in ALLOWED_SUFFIXES:
                skipped_files.append(original_name)
                continue

            safe_name = secure_filename(original_name) or f"upload_{len(saved_files) + 1}.pdf"
            destination = input_dir / safe_name
            uploaded.save(destination)
            saved_files.append(safe_name)

        if not saved_files:
            result = {
                "job_id": job_id,
                "error": "Carica almeno un file PDF valido per avviare la pipeline.",
                "saved_files": [],
                "skipped_files": skipped_files,
                "downloads": [],
                "warnings": [],
                "metrics": {"pdf_count": 0, "extracted_rows": 0, "aggregated_months": 0, "warning_months": 0},
            }
            persist_result(job_dir, result)
            return redirect(url_for("index", job_id=job_id), code=303)

        pipeline_result = full_pipeline(input_dir, output_dir=output_dir)
        result = build_result_payload(job_id, saved_files, skipped_files, pipeline_result)
        persist_result(job_dir, result)
        return redirect(url_for("index", job_id=job_id), code=303)

    @app.get(join_app_path(app_path, "/download/<job_id>/<report_key>"))
    def download_report(job_id: str, report_key: str):
        result = load_result(job_id)
        if result is None:
            abort(404)
        report = result.get("reports", {}).get(report_key)
        if not report:
            abort(404)

        report_path = Path(report["path"])
        if not report_path.exists():
            abort(404)

        return send_file(report_path, as_attachment=True, download_name=report["filename"])

    return app


def load_result(job_id: str) -> dict | None:
    if not job_id:
        return None
    manifest_path = RUNS_DIR / job_id / "result.json"
    if not manifest_path.exists():
        return None
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def persist_result(job_dir: Path, result: dict) -> None:
    job_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = job_dir / "result.json"
    manifest_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")


def build_result_payload(
    job_id: str,
    saved_files: list[str],
    skipped_files: list[str],
    pipeline_result: dict | None,
) -> dict:
    payload = {
        "job_id": job_id,
        "saved_files": saved_files,
        "skipped_files": skipped_files,
        "downloads": [],
        "warnings": [],
        "confidence": [],
        "reports": {},
        "metrics": {
            "pdf_count": len(saved_files),
            "extracted_rows": 0,
            "aggregated_months": 0,
            "warning_months": 0,
        },
        "platform_alerts": [],
    }

    if pipeline_result is None:
        payload["error"] = "La pipeline non ha prodotto dati. Controlla i PDF caricati."
        return payload

    extracted_df = pipeline_result.get("extracted")
    aggregated_df = pipeline_result.get("aggregated")
    warning_df = pipeline_result.get("warnings")
    files = pipeline_result.get("files", {})
    payload["platform_alerts"] = list(dict.fromkeys(pipeline_result.get("platform_alerts", []) or []))

    payload["metrics"]["extracted_rows"] = int(len(extracted_df)) if extracted_df is not None else 0
    payload["metrics"]["aggregated_months"] = int(len(aggregated_df)) if aggregated_df is not None else 0
    payload["metrics"]["warning_months"] = int(len(warning_df)) if warning_df is not None else 0

    if aggregated_df is not None and not aggregated_df.empty:
        confidence_columns = [
            column for column in [
                "anno",
                "mese",
                "affidabilita_mese",
                "confidenza_percent",
                "confidenza_motivo",
                "warning_count",
                "importo_confidenza_percent",
                "importo_confidenza_motivo",
                "importo_affidabilita",
                "consumo_confidenza_percent",
                "consumo_confidenza_motivo",
                "consumo_affidabilita",
            ]
            if column in aggregated_df.columns
        ]
        payload["confidence"] = aggregated_df.loc[:, confidence_columns].fillna("").to_dict(orient="records")

    for key, (label, default_name) in REPORT_METADATA.items():
        report_path = files.get(key)
        if not report_path:
            continue
        report_path = Path(report_path)
        if not report_path.exists():
            continue

        download_info = {
            "key": key,
            "label": label,
            "filename": report_path.name if report_path.name else default_name,
            "path": str(report_path),
            "size_kb": round(report_path.stat().st_size / 1024, 1),
            "url": url_for("download_report", job_id=job_id, report_key=key),
        }
        payload["downloads"].append(download_info)
        payload["reports"][key] = {
            "filename": download_info["filename"],
            "path": download_info["path"],
        }

    if warning_df is not None and not warning_df.empty:
        payload["warnings"] = warning_df.fillna("").to_dict(orient="records")

    return payload


def get_platform_status(result: dict | None = None) -> dict | None:
    alerts: list[str] = []
    env_warning = os.environ.get("OPENAI_PLATFORM_BUDGET_WARNING", "").strip()
    if env_warning:
        alerts.append(env_warning)
    env_status = os.environ.get("OPENAI_PLATFORM_BUDGET_STATUS", "").strip().lower()
    if env_status in {"low", "critical", "warning"} and not alerts:
        alerts.append("Budget OpenAI segnalato come basso dall'ambiente di esecuzione.")

    if result:
        alerts.extend(result.get("platform_alerts", []) or [])

    alerts = list(dict.fromkeys(alert for alert in alerts if alert))
    if not alerts:
        return None

    severity = "warning"
    if any("esaurito" in alert.lower() or "fermarsi" in alert.lower() for alert in alerts):
        severity = "error"
    return {
        "severity": severity,
        "messages": alerts,
    }
