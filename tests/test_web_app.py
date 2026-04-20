import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

from flask import url_for

from src.web.app import create_app


class TestWebAppRouting(unittest.TestCase):
    @staticmethod
    def _extract_job_id(location: str) -> str:
        parsed = urlparse(location)
        return parse_qs(parsed.query)["job_id"][0]

    def test_prefixed_deploy_namespaces_static_brand_and_routes(self):
        with patch.dict(os.environ, {"ENERGON_WEB_PATH": "/Lettura_Bollette_Energon"}):
            app = create_app()

        with app.test_request_context():
            self.assertEqual(url_for("index"), "/Lettura_Bollette_Energon")
            self.assertEqual(url_for("run_pipeline_view"), "/Lettura_Bollette_Energon/run")
            self.assertEqual(
                url_for("download_report", job_id="job-1", report_key="csv"),
                "/Lettura_Bollette_Energon/download/job-1/csv",
            )
            self.assertEqual(url_for("static", filename="app.css"), "/Lettura_Bollette_Energon/static/app.css")
            self.assertEqual(url_for("brand_logo"), "/Lettura_Bollette_Energon/brand/logo")

    def test_root_deploy_supports_subdomain_homepage(self):
        with patch.dict(os.environ, {"ENERGON_WEB_PATH": "/"}):
            app = create_app()

        with app.test_request_context():
            self.assertEqual(url_for("index"), "/")
            self.assertEqual(url_for("run_pipeline_view"), "/run")
            self.assertEqual(
                url_for("download_report", job_id="job-2", report_key="xlsx"),
                "/download/job-2/xlsx",
            )
            self.assertEqual(url_for("static", filename="app.css"), "/static/app.css")
            self.assertEqual(url_for("brand_logo"), "/brand/logo")

        self.assertNotIn("root_redirect", app.view_functions)

    def test_run_without_files_redirects_to_job_result_page(self):
        with tempfile.TemporaryDirectory() as tmpdir, patch.dict(
            os.environ,
            {"ENERGON_WEB_PATH": "/Lettura_Bollette_Energon"},
        ), patch("src.web.app.RUNS_DIR", Path(tmpdir)):
            app = create_app()
            client = app.test_client()

            response = client.post("/Lettura_Bollette_Energon/run", data={}, follow_redirects=False)

            self.assertEqual(response.status_code, 303)
            self.assertIn("/Lettura_Bollette_Energon?job_id=", response.headers["Location"])

            job_id = self._extract_job_id(response.headers["Location"])
            manifest_path = Path(tmpdir) / job_id / "result.json"
            self.assertTrue(manifest_path.exists())

            result = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(result["job_id"], job_id)
            self.assertIn("Carica almeno un file PDF valido", result["error"])

    def test_successful_run_redirects_to_job_result_page(self):
        with tempfile.TemporaryDirectory() as tmpdir, patch.dict(
            os.environ,
            {"ENERGON_WEB_PATH": "/Lettura_Bollette_Energon"},
        ), patch("src.web.app.RUNS_DIR", Path(tmpdir)), patch(
            "src.web.app.full_pipeline",
            return_value={"extracted": None, "aggregated": None, "warnings": None, "files": {}, "platform_alerts": []},
        ) as mocked_pipeline:
            app = create_app()
            client = app.test_client()

            response = client.post(
                "/Lettura_Bollette_Energon/run",
                data={"pdf_files": (io.BytesIO(b"%PDF-1.4\nfake pdf\n"), "fattura.pdf")},
                content_type="multipart/form-data",
                follow_redirects=False,
            )

            self.assertEqual(response.status_code, 303)
            self.assertIn("/Lettura_Bollette_Energon?job_id=", response.headers["Location"])

            job_id = self._extract_job_id(response.headers["Location"])
            manifest_path = Path(tmpdir) / job_id / "result.json"
            self.assertTrue(manifest_path.exists())
            mocked_pipeline.assert_called_once()


if __name__ == "__main__":
    unittest.main()
