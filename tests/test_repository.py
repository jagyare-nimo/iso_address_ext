import unittest
import pandas as pd
from sqlalchemy import create_engine, text
import tempfile
import os
from pathlib import Path
from pipeline.schema import create_iso_address_table
from pipeline.repository import DatabaseRepository


class DummyConfig:
    """ Minimal config pointing at a file-backed SQLite DB """

    def __init__(self, database_url: str, table_name: str):
        self.database_url = database_url
        self.table_name = table_name


class TestDatabaseRepository(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = Path(self.tmpdir) / "test.db"
        self.db_url = f"sqlite:///{self.db_path}"
        self.table_name = "iso_address"

        pre_engine = create_engine(self.db_url)
        create_iso_address_table(pre_engine)
        pre_engine.dispose()

        self.config = DummyConfig(self.db_url, self.table_name)
        self.repo = DatabaseRepository(self.config)

        self.engine = create_engine(self.db_url)

    def tearDown(self):
        self.engine.dispose()
        try:
            os.remove(self.db_path)
            os.rmdir(self.tmpdir)
        except OSError:
            pass

    def _count_rows(self):
        with self.engine.begin() as conn:
            return conn.execute(text(f"SELECT COUNT(*) FROM {self.table_name}")).scalar()

    def test_save_empty_dataframe(self):
        df = pd.DataFrame(columns=[
            "ID", "full_address", "house_number", "road", "city", "state",
            "postcode", "country", "filename", "processed_timestamp",
            "extracted_by", "status"
        ])
        self.repo.save(df)
        self.assertEqual(self._count_rows(), 0)

    def test_save_inserts_and_roundtrips(self):
        data = [
            {
                "ID": "a1", "full_address": "1 A St", "house_number": "1",
                "road": "A St", "city": "City", "state": "ST", "postcode": "12345",
                "country": "USA", "filename": "f1.xlsx", "processed_timestamp": "t1",
                "extracted_by": "tester", "status": "PERFECT"
            },
            {
                "ID": "b2", "full_address": "2 B Rd", "house_number": "2",
                "road": "B Rd", "city": "Town", "state": "TS", "postcode": "54321",
                "country": "CAN", "filename": "f2.xlsx", "processed_timestamp": "t2",
                "extracted_by": "tester", "status": "PARTIAL"
            }
        ]
        df = pd.DataFrame(data)
        self.repo.save(df, batch_size=1)

        with self.engine.begin() as conn:
            rows = conn.execute(text(
                f"SELECT id, full_address, status FROM {self.table_name}"
            )).all()

        self.assertEqual(len(rows), 2)
        self.assertIn(("a1", "1 A St", "PERFECT"), rows)
        self.assertIn(("b2", "2 B Rd", "PARTIAL"), rows)

    def test_save_upserts_existing(self):
        initial = pd.DataFrame([{
            "ID": "x1", "full_address": "Old", "house_number": "0", "road": "OldRd",
            "city": "OldCity", "state": "OS", "postcode": "00000", "country": "USA",
            "filename": "old.xlsx", "processed_timestamp": "t0",
            "extracted_by": "tester", "status": "PERFECT"
        }])
        self.repo.save(initial, batch_size=1)
        self.assertEqual(self._count_rows(), 1)

        updated = pd.DataFrame([{
            "ID": "x1", "full_address": "New", "house_number": "9", "road": "NewRd",
            "city": "NewCity", "state": "NS", "postcode": "99999", "country": "GB",
            "filename": "new.xlsx", "processed_timestamp": "t1",
            "extracted_by": "tester2", "status": "PARTIAL"
        }])
        self.repo.save(updated, batch_size=1)

        self.assertEqual(self._count_rows(), 1)
        with self.engine.begin() as conn:
            row = conn.execute(text(
                f"SELECT full_address, city, status, extracted_by "
                f"FROM {self.table_name} WHERE id='x1'"
            )).one()
        self.assertEqual(row, ("New", "NewCity", "PARTIAL", "tester2"))


if __name__ == "__main__":
    unittest.main()
