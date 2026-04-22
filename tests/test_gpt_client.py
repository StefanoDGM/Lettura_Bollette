import base64
import json
import tempfile
import unittest
from pathlib import Path

from src.ai.gpt_client import JSON_SCHEMA, build_pdf_input_content
from src.parser.bolletta_parser import parse_gpt_response


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

    def test_json_schema_contains_only_public_logic_fields(self):
        properties = JSON_SCHEMA["schema"]["properties"]["rows"]["items"]["properties"]
        required = JSON_SCHEMA["schema"]["properties"]["rows"]["items"]["required"]

        self.assertIn("consumo_dettaglio_riga", properties)
        self.assertIn("manca_dettaglio_consumo", properties)
        self.assertIn("tipo_componente", properties)
        self.assertIn("riferimento_ricalcolo_da", properties)
        self.assertIn("riferimento_ricalcolo_a", properties)
        self.assertIn("presenza_ricalcolo", properties)
        self.assertIn("ricalcolo_aggregato_multi_mese", properties)
        self.assertIn("tipo_ricalcolo", properties)
        self.assertIn("consumo_dettaglio_riga", required)
        self.assertIn("manca_dettaglio_consumo", required)
        self.assertIn("tipo_componente", required)
        self.assertIn("riferimento_ricalcolo_da", required)
        self.assertIn("riferimento_ricalcolo_a", required)
        self.assertIn("presenza_ricalcolo", required)
        self.assertIn("ricalcolo_aggregato_multi_mese", required)
        self.assertIn("tipo_ricalcolo", required)
        self.assertNotIn("blocco_ricalcolo_aggregato", properties)
        self.assertNotIn("ricalcolo_spalmabile", properties)
        self.assertNotIn("manca_dettaglio_ricalcolo", properties)
        self.assertNotIn("dettaglio_ricostruzione_presente", properties)
        self.assertNotIn("totale_documento_puo_non_coincidere_con_mese_corrente", properties)
        self.assertNotIn("categoria_parser", properties)

    def test_parser_sets_defaults_for_public_logic_fields(self):
        raw = json.dumps(
            {
                "rows": [
                    {
                        "nome_cliente": "Cliente",
                        "pdr": "",
                        "data_inizio": "01/01/2024",
                        "data_fine": "31/01/2024",
                        "consumo_totale": "100",
                        "consumo_dettaglio_riga": "",
                        "dettaglio_voce": "Quota energia",
                        "importo": "10.00",
                        "imponibile_mese": "10.00",
                        "manca_dettaglio": "no",
                        "manca_dettaglio_consumo": "si",
                    }
                ]
            }
        )

        rows = parse_gpt_response(raw, "test.pdf")

        self.assertEqual(rows[0]["tipo_componente"], "")
        self.assertEqual(rows[0]["riferimento_ricalcolo_da"], "")
        self.assertEqual(rows[0]["riferimento_ricalcolo_a"], "")
        self.assertEqual(rows[0]["presenza_ricalcolo"], "")
        self.assertEqual(rows[0]["ricalcolo_aggregato_multi_mese"], "")
        self.assertEqual(rows[0]["tipo_ricalcolo"], "")

    def test_parser_keeps_original_imponibile_value(self):
        raw = json.dumps(
            {
                "rows": [
                    {
                        "nome_cliente": "Cliente",
                        "data_inizio": "01/07/2025",
                        "data_fine": "31/07/2025",
                        "dettaglio_voce": "Voce A",
                        "importo": "100.00",
                        "imponibile_mese": "110.00",
                        "manca_dettaglio": "no",
                        "manca_dettaglio_consumo": "si",
                    },
                    {
                        "nome_cliente": "Cliente",
                        "data_inizio": "01/07/2025",
                        "data_fine": "31/07/2025",
                        "dettaglio_voce": "Voce B",
                        "importo": "50.00",
                        "imponibile_mese": "110.00",
                        "manca_dettaglio": "no",
                        "manca_dettaglio_consumo": "si",
                    },
                ]
            }
        )

        rows = parse_gpt_response(raw, "test.pdf")

        self.assertEqual(rows[0]["imponibile_mese"], "110.00")
        self.assertEqual(rows[1]["imponibile_mese"], "110.00")


if __name__ == "__main__":
    unittest.main()
