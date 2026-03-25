import unittest
from pathlib import Path
from src.pipeline.process_bolletta import find_pdf_files, process_all_pdfs, resolve_input_dir

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
        self.assertTrue(any(pdf.suffix == ".PDF" for pdf in pdfs))
        self.assertTrue(any(pdf.suffix == ".pdf" for pdf in pdfs))

if __name__ == "__main__":
    unittest.main()
