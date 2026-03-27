import base64
import tempfile
import unittest
from pathlib import Path

from src.ai.gpt_client import JSON_SCHEMA, build_pdf_input_content


class TestGptClient(unittest.TestCase):
    def test_build_pdf_input_content_normalizes_extension(self):
        pdf_bytes = b"%PDF-1.4\nfake pdf content\n"

        with tempfile.NamedTemporaryFile(suffix=".PDF", delete=False) as tmp:
            tmp.write(pdf_bytes)
            tmp_path = Path(tmp.name)

        try:
            content = build_pdf_input_content(tmp_path)
        finally:
            tmp_path.unlink(missing_ok=True)

        self.assertEqual(content["type"], "input_file")
        self.assertTrue(content["filename"].endswith(".pdf"))
        self.assertNotIn(".PDF", content["filename"])
        self.assertEqual(
            content["file_data"],
            f"data:application/pdf;base64,{base64.b64encode(pdf_bytes).decode('utf-8')}",
        )

    def test_json_schema_contains_new_consumption_fields(self):
        properties = JSON_SCHEMA["schema"]["properties"]["rows"]["items"]["properties"]
        required = JSON_SCHEMA["schema"]["properties"]["rows"]["items"]["required"]

        self.assertIn("consumo_dettaglio_riga", properties)
        self.assertIn("manca_dettaglio_consumo", properties)
        self.assertIn("consumo_dettaglio_riga", required)
        self.assertIn("manca_dettaglio_consumo", required)


if __name__ == "__main__":
    unittest.main()
