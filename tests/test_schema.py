import unittest
from sqlalchemy import create_engine, inspect
from pipeline.schema import create_iso_address_table


class TestCreateIsoAddressTable(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        create_iso_address_table(self.engine)
        self.inspector = inspect(self.engine)

    def test_table_created(self):
        tables = self.inspector.get_table_names()
        self.assertIn("iso_address", tables)

    def test_columns_exist(self):
        cols = [c["name"] for c in self.inspector.get_columns("iso_address")]
        expected = [
            "record_id",
            "id",
            "full_address",
            "house_number",
            "road",
            "city",
            "state",
            "postcode",
            "country",
            "filename",
            "processed_timestamp",
            "extracted_by",
            "status",
        ]

        self.assertCountEqual(cols, expected)

    def test_column_types_and_nullable(self):
        cols = self.inspector.get_columns("iso_address")
        col_map = {c["name"]: c for c in cols}

        self.assertTrue(col_map["record_id"]["primary_key"])
        self.assertEqual(col_map["record_id"]["type"].__class__.__name__, "INTEGER")

        self.assertFalse(col_map["status"]["nullable"])

        for name in [
            "id",
            "full_address",
            "house_number",
            "road",
            "city",
            "state",
            "postcode",
            "country",
            "filename",
            "processed_timestamp",
            "extracted_by",
        ]:
            self.assertTrue(col_map[name]["nullable"], f"{name} should be nullable")

    def test_record_id_pk_and_autoincrement(self):
        pk = self.inspector.get_pk_constraint("iso_address")["constrained_columns"]
        self.assertEqual(pk, ["record_id"])

        col = next(c for c in self.inspector.get_columns("iso_address") if c["name"] == "record_id")
        self.assertIn(col.get("autoincrement"), (None, True, "auto"))


if __name__ == "__main__":
    unittest.main()
