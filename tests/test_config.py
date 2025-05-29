import os
import shutil
import tempfile
import unittest
import yaml
from pathlib import Path

from pipeline.config import Config


class TestConfig(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.config_path = Path(self.tmpdir) / "config.yml"

        sample = {
            "input_dir": "raw",
            "extracted_dir": "ex",
            "processed_dir": "proc",
            "archive": {
                "input_dir": "arc_in",
                "processed_dir": "arc_proc"
            },
            "datasource": {
                "url": "jdbc:postgresql://localhost:5432/db",
                "driverClassName": "org.postgresql.Driver",
                "username": "user",
                "password": "pw"
            },
            "database": {
                "url": "postgresql://user@localhost:5432/db",
                "table_name": "my_table"
            }
        }
        self.config_path.write_text(yaml.safe_dump(sample))
        for d in ("raw", "ex", "proc", "arc_in", "arc_proc"):
            (Path(self.tmpdir) / d).mkdir()

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_loads_all_fields_and_resolves_paths(self):
        cfg = Config(path=str(self.config_path))
        self.assertTrue(cfg.input_dir.endswith(os.path.join("raw")))
        self.assertTrue(cfg.extracted_dir.endswith(os.path.join("ex")))
        self.assertTrue(cfg.processed_dir.endswith(os.path.join("proc")))
        self.assertTrue(cfg.archive_input_dir.endswith(os.path.join("arc_in")))
        self.assertTrue(cfg.archive_processed_dir.endswith(os.path.join("arc_proc")))

        # datasource
        self.assertEqual(cfg.datasource_url, "jdbc:postgresql://localhost:5432/db")
        self.assertEqual(cfg.datasource_driver, "org.postgresql.Driver")
        self.assertEqual(cfg.datasource_username, "user")
        self.assertEqual(cfg.datasource_password, "pw")

        # database
        self.assertEqual(cfg.database_url, "postgresql://user@localhost:5432/db")
        self.assertEqual(cfg.table_name, "my_table")

        rep = repr(cfg)
        self.assertIn("input_dir=", rep)
        self.assertIn("datasource_url='jdbc:postgresql://localhost:5432/db'", rep)
        self.assertIn("table_name='my_table'", rep)

    def test_missing_file_raises(self):
        missing = Path(self.tmpdir) / "does_not_exist.yml"
        with self.assertRaises(FileNotFoundError):
            Config(path=str(missing))


if __name__ == "__main__":
    unittest.main()
