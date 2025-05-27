import unittest
import tempfile
import shutil
from pathlib import Path
from pipeline.archiver import Archiver


class TestArchiver(unittest.TestCase):

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.inp = Path(self.tmp_dir) / 'in'
        self.ain = Path(self.tmp_dir) / 'ain'
        self.aproc = Path(self.tmp_dir) / 'aproc'
        self.inp.mkdir()
        self.ain.mkdir()
        self.aproc.mkdir()
        (self.inp / 'f.txt').write_text('x')
        (Path(self.tmp_dir) / 'p.txt').write_text('y')

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    def test_archive(self):
        arch = Archiver(str(self.inp), str(self.ain), str(self.aproc))
        arch.archive('f.txt', str(Path(self.tmp_dir) / 'p.txt'))
        self.assertTrue((self.ain / 'f.txt').exists())
        self.assertTrue((self.aproc / 'p.txt').exists())

    def test_archive_missing(self):
        arch = Archiver('non', 'non2', 'non3')
        with self.assertRaises(Exception):
            arch.archive('no', 'no2')
