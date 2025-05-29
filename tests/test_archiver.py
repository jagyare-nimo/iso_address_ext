import os
import shutil
import tempfile
import unittest
from pathlib import Path

from pipeline.archiver import Archiver


class TestArchiver(unittest.TestCase):
    def setUp(self):
        self.input_dir = tempfile.mkdtemp()
        self.archive_input_dir = tempfile.mkdtemp()
        self.archive_processed_dir = tempfile.mkdtemp()

        self.original_name = "mydata.xlsx"
        self.timestamp = "20250101T123456"
        self.processed_filename = f"processed_{self.timestamp}.xlsx"

        Path(self.input_dir, self.original_name).write_text("raw")
        Path(self.input_dir, self.processed_filename).write_text("processed")

        self.original_path = os.path.join(self.input_dir, self.original_name)
        self.processed_path = os.path.join(self.input_dir, self.processed_filename)

        self.archiver = Archiver(
            input_dir=self.input_dir,
            archive_input_dir=self.archive_input_dir,
            archive_processed_dir=self.archive_processed_dir
        )

    def tearDown(self):
        shutil.rmtree(self.input_dir)
        shutil.rmtree(self.archive_input_dir)
        shutil.rmtree(self.archive_processed_dir)

    def test_archive_successful(self):
        self.archiver.archive(self.original_name, self.processed_path)

        expected_raw_name = f"mydata_{self.timestamp}.xlsx"
        archived_raw_path = Path(self.archive_input_dir) / expected_raw_name
        self.assertTrue(archived_raw_path.exists(), "Raw file was not archived or renamed correctly")

        archived_processed_path = Path(self.archive_processed_dir) / self.processed_filename
        self.assertTrue(archived_processed_path.exists(), "Processed file was not archived correctly")

    def test_archive_missing_raw_raises(self):
        # Remove raw file
        os.remove(self.original_path)
        with self.assertRaises(FileNotFoundError):
            self.archiver.archive(self.original_name, self.processed_path)

    def test_archive_missing_processed_raises(self):
        # Remove processed file
        os.remove(self.processed_path)
        with self.assertRaises(FileNotFoundError):
            self.archiver.archive(self.original_name, self.processed_path)


if __name__ == '__main__':
    unittest.main()
