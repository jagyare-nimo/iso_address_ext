import unittest
import yaml
import tempfile
import shutil
from pathlib import Path

from pipeline.config import Config


class TestConfig(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.cfg_path = Path(self.tmp_dir) / 'config.yml'

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    def test_config(self):
        cfg_data = {
            'input_dir': 'in',
            'extracted_dir': 'ext',
            'processed_dir': 'proc',
            'archive': {'input_dir': 'ain', 'processed_dir': 'aproc'},
            'datasource': {'url': 'jdbc:h2:file:test', 'driverClassName': 'org.h2.Driver', 'username': 'sa', 'password':''},
            'database': {'table_name': 'iso_address'}
        }
        with open(self.cfg_path, 'w') as f:
            yaml.dump(cfg_data, f)

        cfg = Config(str(self.cfg_path))
        self.assertEqual(cfg.input_dir, 'in')
        self.assertEqual(cfg.extracted_dir, 'ext')
        self.assertTrue(cfg.datasource_url.startswith('jdbc:h2'))
        self.assertEqual(cfg.table_name, 'iso_address')

    def test_config_missing(self):
        with self.assertRaises(FileNotFoundError):
            Config(str(Path(self.tmp_dir) / 'missing.yml'))
