import tempfile
import unittest
from pathlib import Path

import pandas as pd

from run_full_pipeline import extract_warning_report, get_pipeline_output_paths


class TestRunFullPipeline(unittest.TestCase):
    def test_get_pipeline_output_paths_uses_custom_output_dir(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            base_dir = Path(tmp_dir)
            paths = get_pipeline_output_paths(base_dir)

        self.assertEqual(paths["csv"].name, "estrazione_tutti_mesi.csv")
        self.assertEqual(paths["xlsx"].name, "estrazione_tutti_mesi.xlsx")
        self.assertEqual(paths["aggregated"].name, "bollette_raggruppate.xlsx")
        self.assertEqual(paths["csv"].parent, base_dir)
        self.assertEqual(paths["xlsx"].parent, base_dir)
        self.assertEqual(paths["aggregated"].parent, base_dir)

    def test_extract_warning_report_filters_months_without_warning(self):
        aggregated_df = pd.DataFrame(
            [
                {"anno": 2024, "mese": "gennaio", "warning_count": 0, "warning_mese": ""},
                {"anno": 2024, "mese": "febbraio", "warning_count": 2, "warning_mese": "Controllo manuale"},
            ]
        )

        warning_df = extract_warning_report(aggregated_df)

        self.assertEqual(len(warning_df), 1)
        self.assertEqual(warning_df.loc[0, "mese"], "febbraio")
        self.assertEqual(warning_df.loc[0, "warning_mese"], "Controllo manuale")

    def test_extract_warning_report_prefers_report_in_dataframe_attrs(self):
        aggregated_df = pd.DataFrame([{"anno": 2024, "mese": "gennaio", "warning_count": 0, "warning_mese": ""}])
        aggregated_df.attrs["warning_report"] = pd.DataFrame(
            [{"anno": 2024, "mese": "febbraio", "warning_count": 1, "warning_mese": "Verifica manuale"}]
        )

        warning_df = extract_warning_report(aggregated_df)

        self.assertEqual(len(warning_df), 1)
        self.assertEqual(warning_df.loc[0, "mese"], "febbraio")
        self.assertEqual(warning_df.loc[0, "warning_mese"], "Verifica manuale")


if __name__ == "__main__":
    unittest.main()
