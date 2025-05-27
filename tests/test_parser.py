import unittest
import tempfile
import shutil
import pandas as pd
from pathlib import Path
from unittest.mock import patch

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
            'ID': ['1', '2'],
            'ADDRESSLINE1': ['a', 'd'],
            'ADDRESSLINE2': ['b', 'e'],
            'ADDRESSLINE3': ['c', 'f']
        })
        self.ex_file = Path(self.tmp_dir) / 'ex.xlsx'
        df.to_excel(self.ex_file, index=False)

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    @patch('pipeline.parser.AddressParser')
    def test_parse(self, mock_parser_class):
        mock_parser = mock_parser_class.return_value
        mock_parser.return_value = [
                                       DummyParsed({
                                           'house_number': '10',
                                           'road': 'Main',
                                           'city': 'Town',
                                           'state': 'ST',
                                           'postcode': '00000',
                                           'country': 'XY'
                                       })
                                   ] * 2

        svc = AddressParserService()
        df, proc = svc.parse_file(str(self.ex_file), str(self.tmp_dir))

        self.assertIn('house_number', df.columns)
        self.assertTrue((df['house_number'] == '10').all())
        self.assertTrue(proc.endswith('.xlsx'))

    def test_parse_missing_file(self):
        svc = AddressParserService()
        with self.assertRaises(Exception):
            svc.parse_file('nonexist.xlsx', str(self.tmp_dir))


if __name__ == '__main__':
    unittest.main()
