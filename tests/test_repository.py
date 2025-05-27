import unittest
import tempfile
import shutil
import pandas as pd
from pathlib import Path
from unittest.mock import patch

from pipeline.repository import DatabaseRepository


class DummyConn:

    def __init__(self):
        class Cur:
            def __init__(self, outer):
                self.outer = outer

            def execute(self, sql, args=None):
                self.outer.last_execute = (sql, args)

            def executemany(self, sql, data):
                self.outer.last_batch = data

        self._cur = Cur(self)
        self.cursor = self._cur
        self.last_execute = None
        self.last_batch = None

    def commit(self):
        pass


class TestDatabaseRepository(unittest.TestCase):

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.jar_path = str(Path(self.tmp_dir) / 'h2.jar')
        Path(self.jar_path).write_text('dummy jar content')

        self.conn = DummyConn()
        self.patcher = patch('pipeline.repository.jaydebeapi.connect', return_value=self.conn)
        self.mock_connect = self.patcher.start()

        self.repo = DatabaseRepository(
            table_name='iso_address',
            datasource_url='jdbc:h2:file:test;MODE=PostgreSQL',
            driver_class='org.h2.Driver',
            username='',
            password='',
            jar_path=self.jar_path
        )

    def tearDown(self):
        self.patcher.stop()
        shutil.rmtree(self.tmp_dir)

    def test_init_creates_table(self):
        sql, params = self.conn.last_execute
        self.assertIn('CREATE TABLE IF NOT EXISTS iso_address', sql)
        self.assertIsNone(params)

    def test_save_upsert(self):
        df = pd.DataFrame([{
            'ID': 'x',
            'full_address': '123 Main St',
            'house_number': '123',
            'road': 'Main St',
            'city': 'Testville',
            'state': 'TS',
            'postcode': '12345',
            'country': 'XY',
            'filename': 'ex.xlsx',
            'processed_timestamp': '20250101T120000'
        }])

        self.repo.save(df)

        delete_sql, delete_params = self.conn.last_execute
        self.assertTrue(delete_sql.startswith('DELETE FROM iso_address'))
        self.assertEqual(delete_params, ['x'])

        batch = self.conn.last_batch
        self.assertEqual(len(batch), 1)
        row = batch[0]
        self.assertEqual(row[0], 'x')
        self.assertEqual(row[-1], '20250101T120000')


if __name__ == '__main__':
    unittest.main()
