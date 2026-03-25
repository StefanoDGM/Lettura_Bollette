import unittest
from pathlib import Path

from src.pipeline.aggregate_bills import INPUT_PATH, OUT_XLSX, resolve_input_path, resolve_output_path


class TestAggregateBills(unittest.TestCase):
    def test_resolve_default_input_path(self):
        input_path, searched_paths = resolve_input_path(None)
        self.assertTrue(input_path.exists())
        self.assertIn(input_path, searched_paths)

    def test_resolve_default_output_path(self):
        output_path = resolve_output_path(None)
        self.assertEqual(output_path, OUT_XLSX)
        self.assertEqual(output_path.parent, INPUT_PATH.parent)


if __name__ == "__main__":
    unittest.main()
