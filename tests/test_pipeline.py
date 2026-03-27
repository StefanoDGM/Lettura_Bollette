import unittest
from pathlib import Path
from src.extractor.pdf_extractor import MAX_PAGES
from src.pipeline.process_bolletta import (
    find_pdf_files,
    normalize_export_dataframe,
    process_all_pdfs,
    resolve_input_dir,
)
import pandas as pd

class TestPipeline(unittest.TestCase):
    def test_process_empty_dir(self):
        # Test with empty directory
        empty_dir = Path("tests/empty")
        empty_dir.mkdir(exist_ok=True)
        result = process_all_pdfs(empty_dir, "*.pdf", "test.csv", "test.xlsx")
        self.assertIsNone(result)

    def test_resolve_default_input_dir(self):
        input_dir, searched_paths = resolve_input_dir(None)
        self.assertTrue(input_dir.exists())
        self.assertIn(input_dir, searched_paths)

    def test_find_pdf_files_is_case_insensitive(self):
        pdfs = find_pdf_files(Path("tests/data"), "*.pdf")
        self.assertTrue(pdfs)
        self.assertTrue(all(pdf.suffix.lower() == ".pdf" for pdf in pdfs))

    def test_normalize_export_dataframe_uses_comma_for_imports(self):
        df = pd.DataFrame(
            [
                {"importo": "123,45", "imponibile_mese": "456.78", "consumo_totale": "1000,5"},
                {"importo": "67.89", "imponibile_mese": "10,20", "consumo_totale": "2000.0"},
            ]
        )

        normalized = normalize_export_dataframe(df)

        self.assertEqual(normalized.loc[0, "importo"], "123,45")
        self.assertEqual(normalized.loc[1, "importo"], "67,89")
        self.assertEqual(normalized.loc[0, "imponibile_mese"], "456,78")
        self.assertEqual(normalized.loc[1, "imponibile_mese"], "10,2")
        self.assertEqual(normalized.loc[0, "consumo_totale"], "1000,5")
        self.assertEqual(normalized.loc[1, "consumo_totale"], "2000")

    def test_pdf_limit_is_twelve_pages(self):
        self.assertEqual(MAX_PAGES, 12)

if __name__ == "__main__":
    unittest.main()
