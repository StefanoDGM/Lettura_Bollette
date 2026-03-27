import tempfile
import unittest
from pathlib import Path

import pandas as pd

from src.pipeline.aggregate_bills import (
    INPUT_PATH,
    OUT_XLSX,
    aggregate_bolletta_data,
    resolve_input_path,
    resolve_output_path,
)


class TestAggregateBills(unittest.TestCase):
    def test_resolve_default_input_path(self):
        input_path, searched_paths = resolve_input_path(None)
        self.assertEqual(input_path.name, INPUT_PATH.name)
        self.assertIn(input_path, searched_paths)

    def test_resolve_default_output_path(self):
        output_path = resolve_output_path(None)
        self.assertEqual(output_path, OUT_XLSX)
        self.assertEqual(output_path.parent, INPUT_PATH.parent)

    def test_aggregate_uses_consumo_totale_when_detail_missing_single_doc(self):
        rows = [
            {
                "data_fine": "31/01/2024",
                "consumo_totale": "123.4",
                "consumo_dettaglio_riga": "",
                "importo": "10.00",
                "imponibile_mese": "10.00",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "_source_file": "gennaio 2024.pdf",
                "dettaglio_voce": "Quota energia",
            }
        ]

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_dir_path = Path(tmp_dir)
            input_path = tmp_dir_path / "input.csv"
            output_path = tmp_dir_path / "output.xlsx"
            pd.DataFrame(rows).to_csv(input_path, index=False)

            agg = aggregate_bolletta_data(input_path, output_path)

        self.assertEqual(len(agg), 1)
        self.assertAlmostEqual(float(agg.loc[0, "consumo_mese"]), 123.4)
        self.assertEqual(agg.loc[0, "consumo_logica_usata"], "consumo_totale")
        self.assertFalse(bool(agg.loc[0, "mese_ricalcolato"]))

    def test_aggregate_uses_signed_consumo_detail_on_recalculation(self):
        rows = [
            {
                "data_fine": "31/08/2024",
                "consumo_totale": "3700",
                "consumo_dettaglio_riga": "3700",
                "importo": "100.00",
                "imponibile_mese": "100.00",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "no",
                "_source_file": "agosto 2024.pdf",
                "dettaglio_voce": "Consumo energia",
            },
            {
                "data_fine": "31/08/2024",
                "consumo_totale": "4154",
                "consumo_dettaglio_riga": "-3700",
                "importo": "-20.00",
                "imponibile_mese": "80.00",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "no",
                "_source_file": "settembre 2024.pdf",
                "dettaglio_voce": "Storno consumo precedente",
            },
            {
                "data_fine": "31/08/2024",
                "consumo_totale": "4154",
                "consumo_dettaglio_riga": "4154",
                "importo": "20.00",
                "imponibile_mese": "100.00",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "no",
                "_source_file": "settembre 2024.pdf",
                "dettaglio_voce": "Ricalcolo consumo",
            },
        ]

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_dir_path = Path(tmp_dir)
            input_path = tmp_dir_path / "input.csv"
            output_path = tmp_dir_path / "output.xlsx"
            pd.DataFrame(rows).to_csv(input_path, index=False)

            agg = aggregate_bolletta_data(input_path, output_path)

        self.assertEqual(len(agg), 1)
        self.assertAlmostEqual(float(agg.loc[0, "consumo_mese"]), 4154.0)
        self.assertEqual(
            agg.loc[0, "consumo_logica_usata"],
            "somma_consumo_dettaglio_riga_ricalcolo",
        )
        self.assertTrue(bool(agg.loc[0, "mese_ricalcolato"]))
        self.assertEqual(int(agg.loc[0, "source_file_distinti"]), 2)
        self.assertEqual(int(agg.loc[0, "consumi_totali_distinti"]), 2)


if __name__ == "__main__":
    unittest.main()
