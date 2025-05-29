import unittest
import tempfile
import shutil
import pandas as pd
from pathlib import Path
from unittest.mock import patch, MagicMock

from pipeline.parser import AddressParserService


class DummyParsed:
    def __init__(self, d):
        self._d = d

    def to_dict(self):
        return self._d


class TestAddressParserService(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        df = pd.DataFrame({
            'ID': ['1', '2', '3'],
            'ADDRESSLINE1': ['a', 'd', 'x'],
            'ADDRESSLINE2': ['b', 'e', 'y'],
            'ADDRESSLINE3': ['c', 'f', 'z']
        })
        self.input_file = Path(self.tmp_dir) / 'ex.xlsx'
        df.to_excel(self.input_file, index=False)
        self.proc_dir = str(Path(self.tmp_dir) / "out")
        Path(self.proc_dir).mkdir()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    @patch("pipeline.parser.AddressParser")
    def test_parse_success(self, mock_parser_class):
        parsed_dicts = [{
            'country': '', 'state': '',
            'house_number': None, 'road': None,
            'city': None, 'postcode': None
        }] * 3
        mock_parser = MagicMock()
        mock_parser.return_value = [DummyParsed(d) for d in parsed_dicts]
        mock_parser_class.return_value = mock_parser

        svc = AddressParserService(workers=1, extracted_by='tester')
        out_df, out_path = svc.parse_file(str(self.input_file), self.proc_dir)

        self.assertEqual(len(out_df), 3)

        self.assertTrue((out_df['extracted_by'] == 'TESTER').all())

        self.assertTrue(Path(out_path).exists())
        self.assertTrue(str(out_path).endswith('.xlsx'))

    def test_parse_missing_file(self):
        svc = AddressParserService()
        with self.assertRaises(FileNotFoundError):
            svc.parse_file('nonexist.xlsx', self.proc_dir)


if __name__ == '__main__':
    unittest.main()
