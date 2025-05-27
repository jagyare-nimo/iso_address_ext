import unittest
import tempfile
import shutil
import yaml
from pathlib import Path
from pipeline.flow import deepparse_flow


class TestFlow(unittest.TestCase):

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.res = Path(self.tmp_dir) / 'resources'
        self.res.mkdir()

        cfg = {
            'input_dir': str(self.res / 'in'),
            'extracted_dir': str(self.res / 'ex'),
            'processed_dir': str(self.res / 'pr'),
            'archive': {'input_dir': str(self.res / 'ain'), 'processed_dir': str(self.res / 'apr')},
            'datasource': {'url': 'jdbc:h2:file:test;MODE=PostgreSQL', 'driverClassName': 'org.h2.Driver',
                           'username': '', 'password': ''},
            'database': {'table_name': 'iso_address'}
        }

        (self.res / 'config.yml').write_text(yaml.dump(cfg))
        for d in ['in', 'ex', 'pr', 'ain', 'apr']:
            (self.res / d).mkdir()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    def test_flow_no_files(self):
        deepparse_flow(config_path=str(self.res / 'config.yml'))
