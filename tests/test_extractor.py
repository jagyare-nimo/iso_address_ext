import unittest
import pandas as pd
import tempfile
import shutil
from pathlib import Path

from pipeline.extractor import ExcelExtractor


class TestExcelExtractor(unittest.TestCase):

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.input_dir = Path(self.tmp_dir) / 'input'
        self.output_dir = Path(self.tmp_dir) / 'out'
        self.input_dir.mkdir()
        self.output_dir.mkdir()

        df = pd.DataFrame({
            'ID': ['1'],
            'ADDRESSLINE1': ['a'],
            'ADDRESSLINE2': ['b'],
            'ADDRESSLINE3': ['c']
        })
        self.excel_file = self.input_dir / 'test.xlsx'
        df.to_excel(self.excel_file, index=False)

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    def test_list_and_extract(self):
        ex = ExcelExtractor(str(self.input_dir), str(self.output_dir))
        files = ex.list_files()
        self.assertIn('test.xlsx', files)

        out_file = ex.extract('test.xlsx')
        df2 = pd.read_excel(out_file)
        self.assertListEqual(list(df2.columns), ['ID', 'ADDRESSLINE1', 'ADDRESSLINE2', 'ADDRESSLINE3'])

    def test_extract_missing_column(self):
        bad_file = self.input_dir / 'bad.xlsx'
        pd.DataFrame({'ID': [1], 'ADDRESSLINE1': ['x']}).to_excel(bad_file, index=False)
        ex = ExcelExtractor(str(self.input_dir), str(self.output_dir))
        with self.assertRaises(KeyError):
            ex.extract('bad.xlsx')
