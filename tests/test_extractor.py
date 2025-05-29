import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from pipeline.extractor import ExcelExtractor


class TestExcelExtractor(unittest.TestCase):
    def setUp(self):
        self.tmp_root = tempfile.mkdtemp()
        self.input_dir = os.path.join(self.tmp_root, "in")
        self.extracted_dir = os.path.join(self.tmp_root, "out")
        os.makedirs(self.input_dir, exist_ok=True)

        df = pd.DataFrame({
            "ID": [1, 2],
            "ADDRESSLINE1": ["a1", "a2"],
            "ADDRESSLINE2": ["b1", "b2"],
            "ADDRESSLINE3": ["c1", "c2"],
            "OTHER": ["x", "y"]
        })
        self.filename = "sample.xlsx"
        self.in_path = Path(self.input_dir) / self.filename
        df.to_excel(self.in_path, index=False)

        self.extractor = ExcelExtractor(self.input_dir, self.extracted_dir)

    def tearDown(self):
        shutil.rmtree(self.tmp_root)

    def test_list_files_finds_only_excel(self):
        (Path(self.input_dir) / "foo.txt").write_text("ignore me")
        files = self.extractor.list_files()
        self.assertIn(self.filename, files)
        self.assertNotIn("foo.txt", files)

    @patch("pipeline.extractor.datetime")
    def test_extract_creates_timestamped_file_and_content(self, mock_dt):
        mock_dt.datetime.utcnow.return_value = mock_dt.datetime(2025, 1, 1, 0, 0, 0)
        mock_dt.datetime.utcnow().strftime.return_value = "20250101T000000"

        out_path = self.extractor.extract(self.filename)
        stem = Path(self.filename).stem
        expected = Path(self.extracted_dir) / f"{stem}_20250101T000000_extracted.xlsx"
        self.assertEqual(Path(out_path), expected)
        self.assertTrue(expected.exists())

        out_df = pd.read_excel(expected, engine="openpyxl")
        self.assertListEqual(
            list(out_df.columns),
            ExcelExtractor.REQUIRED_COLUMNS
        )
        self.assertEqual(out_df.loc[0, "ID"], 1)
        self.assertEqual(out_df.loc[1, "ADDRESSLINE3"], "c2")

    def test_extract_missing_file_raises(self):
        with self.assertRaises(FileNotFoundError):
            self.extractor.extract("doesnotexist.xlsx")

    def test_extract_missing_columns_raises(self):
        bad_df = pd.DataFrame({
            "ID": [1],
            "ADDRESSLINE1": ["a"],
            "ADDRESSLINE3": ["c"]
        })
        bad_file = Path(self.input_dir) / "bad.xlsx"
        bad_df.to_excel(bad_file, index=False)
        with self.assertRaises(KeyError):
            self.extractor.extract("bad.xlsx")


if __name__ == "__main__":
    unittest.main()
