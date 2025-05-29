import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

from pipeline.flow import deepparse_flow


class FakeConfig:
    def __init__(self, base_dir):
        self.input_dir = str(Path(base_dir) / "in")
        self.extracted_dir = str(Path(base_dir) / "extracted")
        self.processed_dir = str(Path(base_dir) / "processed")
        self.archive_input_dir = str(Path(base_dir) / "archive" / "in")
        self.archive_processed_dir = str(Path(base_dir) / "archive" / "processed")

        self.database_url = ""
        self.table_name = ""


class TestDeepParseFlow(unittest.TestCase):
    def setUp(self):
        self.tmp_root = tempfile.mkdtemp()
        self.fake_cfg = FakeConfig(self.tmp_root)
        for d in [
            self.fake_cfg.input_dir,
            self.fake_cfg.extracted_dir,
            self.fake_cfg.processed_dir,
            self.fake_cfg.archive_input_dir,
            self.fake_cfg.archive_processed_dir
        ]:
            Path(d).mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.tmp_root)

    @patch("pipeline.flow.Config")
    @patch("pipeline.flow.ExcelExtractor")
    @patch("pipeline.flow.AddressParserService")
    @patch("pipeline.flow.DatabaseRepository")
    @patch("pipeline.flow.Archiver")
    def test_no_files(
            self,
            mock_archiver,
            mock_repo,
            mock_parser,
            mock_extractor,
            mock_config_cls
    ):

        mock_config_cls.return_value = self.fake_cfg

        mock_extractor.return_value.list_files.return_value = []

        deepparse_flow(config_path="ignored")

        mock_extractor.return_value.extract.assert_not_called()
        mock_parser.return_value.parse_file.assert_not_called()
        mock_repo.return_value.save.assert_not_called()
        mock_archiver.return_value.archive.assert_not_called()

    @patch("pipeline.flow.Config")
    @patch("pipeline.flow.ExcelExtractor")
    @patch("pipeline.flow.AddressParserService")
    @patch("pipeline.flow.DatabaseRepository")
    @patch("pipeline.flow.Archiver")
    def test_one_file_success(
            self,
            mock_archiver,
            mock_repo,
            mock_parser,
            mock_extractor,
            mock_config_cls
    ):
        mock_config_cls.return_value = self.fake_cfg

        dummy_file = "foo.xlsx"

        Path(self.fake_cfg.input_dir, dummy_file).write_text("")

        mock_extractor.return_value.list_files.return_value = [dummy_file]

        extracted_path = str(Path(self.fake_cfg.extracted_dir) / "ex_foo.xlsx")
        mock_extractor.return_value.extract.return_value = extracted_path

        dummy_df = MagicMock(name="DataFrame")
        processed_path = str(Path(self.fake_cfg.processed_dir) / "pr_foo.xlsx")
        mock_parser.return_value.parse_file.return_value = (dummy_df, processed_path)

        deepparse_flow(config_path="ignored")

        mock_extractor.return_value.extract.assert_called_once_with(dummy_file)
        mock_parser.return_value.parse_file.assert_called_once_with(
            extracted_path, self.fake_cfg.processed_dir
        )

        mock_repo.return_value.save.assert_called_once_with(dummy_df)
        mock_archiver.return_value.archive.assert_called_once_with(
            dummy_file, processed_path
        )


if __name__ == "__main__":
    unittest.main()
