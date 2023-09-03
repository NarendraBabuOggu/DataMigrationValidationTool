from data_migration_validation_tool.datasource.sqlalchemy_datasource import (
    SQLAlchemyDataSource,
)
from datetime import datetime
from unittest import TestCase
from scripts.sqlite_utils import prepare_sqlite_db
import os


class TestSQLAlchemyDataSource(TestCase):
    """
    TestCase to run unit tests against SQLAlchemyDatasource
    """

    @classmethod
    def setUpClass(cls):
        cls.datasource = SQLAlchemyDataSource(
            "test-sql", "employees", "sqlite:///database.sqlite3"
        )
        prepare_sqlite_db("sqlite:///database.sqlite3")

    def test_load_data(self):
        data = self.datasource.load_data()
        actual_data = data[
            ["id", "name", "email", "mobile", "department", "salary", "join_date"]
        ].to_dict("records")[0]
        expected_data = {
            "id": 1,
            "name": "Narendra",
            "email": "",
            "mobile": "",
            "department": "",
            "salary": 10000,
            "join_date": "2023-09-01",
        }
        self.assertEqual(actual_data, expected_data)

    def test_unique_value_count(self):
        data = self.datasource.get_unique_value_count("id")
        self.assertEqual(data, 1)

    def test_non_null_value_count(self):
        data = self.datasource.get_non_null_value_count("id")
        self.assertEqual(data, 1)

    def test_table_record_count(self):
        data = self.datasource.get_table_count()
        self.assertEqual(data, 1)

    @classmethod
    def tearDownClass(cls):
        cls.datasource.close()
        os.remove("database.sqlite3")
