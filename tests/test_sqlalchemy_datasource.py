from unittest import TestCase
import os
import datetime
from data_migration_validation_tool.datasource import get_datasource
from typing import Dict, Optional, Union


class TestSQLAlchemyDatasource(TestCase):
    """
    TestCase to run unit tests against SQLAlchemyDatasource
    """

    @classmethod
    def setUpClass(cls):
        cls.datasource = get_datasource(
            "sqlalchemy",
            "test-sql",
            "mysql+pymysql://admin:admin@localhost:3306/ecommerce",
        )

    def test_get_schema(self):
        """Test method to check if the get_schema method of datasource working as expected"""
        with self.assertRaises(NotImplementedError) as e:
            self.datasource.get_schema("employees")

    def test_load_data_table(self):
        """
        Test method to check if the load_data method of the datasource returns the expected data.
        """
        data = self.datasource.load_data("employees")
        actual_data: Dict[str, str | int | datetime.date] = data[
            ["id", "name", "email", "mobile", "department", "salary", "join_date"]
        ].to_dict("records")
        print(actual_data)
        expected_data = [
            {
                "id": 1,
                "name": "Narendra",
                "email": "",
                "mobile": "",
                "department": "",
                "salary": 10000.0,
                "join_date": datetime.date(2023, 9, 1),
            }
        ]
        self.assertListEqual(actual_data, expected_data)

    def test_load_data_table_exception(self):
        """
        Test method to check if the load_data method of the datasource returns the expected data.
        """
        with self.assertRaises(Exception):
            data = self.datasource.load_data("customers")

    def test_load_data_no_table_name_and_query(self):
        """
        Test method to check if the load_data method of the datasource returns the expected data.
        """
        with self.assertRaises(ValueError):
            data = self.datasource.load_data()

    def test_load_data_query_exception(self):
        """
        Test method to check if the load_data method of the datasource returns the expected data.
        """
        with self.assertRaises(Exception):
            data = self.datasource.load_data(query="SELECT * FROM customers")

    def test_datasource_repr(self):
        """
        Test method to check if the data source representation is working as expected
        """
        repr_string = str(self.datasource)
        self.assertEqual(
            repr_string,
            "<SQLAlchemyDatasource(name='test-sql', type='SQL', connection_string='mysql+pymysql://admin:admin@localhost:3306/ecommerce')>",
        )

    def test_load_data_query_no_result(self):
        """
        Test method to check if the load_data method of the datasource returns the expected data.
        """
        data = self.datasource.load_data(
            query="SELECT id, name FROM employees WHERE id IS NULL"
        )
        print(data)
        expected_data = None
        self.assertEqual(data, expected_data)

    def test_load_data_query_with_result(self):
        """
        Test method to check if the load_data method of the datasource returns the expected data.
        """
        data = self.datasource.load_data(query="SELECT id, name FROM employees")
        actual_data: Dict[str, str | int | datetime.date] = data[
            ["id", "name"]
        ].to_dict("records")
        print(actual_data)
        expected_data = [{"id": 1, "name": "Narendra"}]
        self.assertListEqual(actual_data, expected_data)

    def test_unique_value_count_table(self):
        """
        Test method to check if the get_unique_value_count method of the datasource returns the expected data.
        """
        data = self.datasource.get_unique_value_count("employees", "id")
        self.assertEqual(data, 1)

    def test_unique_value_count_table_exception(self):
        """
        Test method to check if the get_unique_value_count method of the datasource returns the expected data.
        """
        with self.assertRaises(Exception):
            data = self.datasource.get_unique_value_count("customers", "id")

    def test_unique_value_count_no_table_name_and_query(self):
        """
        Test method to check if the get_unique_value_count method of the datasource returns the expected data.
        """
        with self.assertRaises(ValueError):
            data = self.datasource.get_unique_value_count()

    def test_unique_value_count_query_exception(self):
        """
        Test method to check if the get_unique_value_count method of the datasource returns the expected data.
        """
        with self.assertRaises(Exception):
            data = self.datasource.get_unique_value_count(
                query="customers", column_name="id"
            )

    def test_unique_value_count_no_column_name(self):
        """
        Test method to check if the get_unique_value_count method of the datasource returns the expected data.
        """
        with self.assertRaises(ValueError):
            data = self.datasource.get_unique_value_count(query="customers")

    def test_unique_value_count_query(self):
        """
        Test method to check if the get_unique_value_count method of the datasource returns the expected data.
        """
        data = self.datasource.get_unique_value_count(
            query="SELECT id, name FROM employees", column_name="id"
        )
        self.assertEqual(data, 1)

    def test_non_null_value_count_table(self):
        """
        Test method to check if the get_non_null_value_count method of the datasource returns the expected data.
        """
        data = self.datasource.get_non_null_value_count("employees", "id")
        self.assertEqual(data, 1)

    def test_non_null_value_count_table_exception(self):
        """
        Test method to check if the get_non_null_value_count method of the datasource returns the expected data.
        """
        with self.assertRaises(Exception):
            self.datasource.get_non_null_value_count("customer", "id")

    def test_non_null_value_count_query(self):
        """
        Test method to check if the get_non_null_value_count method of the datasource returns the expected data.
        """
        data = self.datasource.get_non_null_value_count(
            query="SELECT id, name FROM employees", column_name="id"
        )
        self.assertEqual(data, 1)

    def test_non_null_value_count_no_table_name_and_query(self):
        """
        Test method to check if the get_non_null_value_count method of the datasource returns the expected data.
        """
        with self.assertRaises(ValueError):
            data = self.datasource.get_non_null_value_count()

    def test_non_null_value_count_no_column_name(self):
        """
        Test method to check if the get_non_null_value_count method of the datasource returns the expected data.
        """
        with self.assertRaises(ValueError):
            data = self.datasource.get_non_null_value_count(
                query="SELECT id, name FROM employees",
            )

    def test_table_record_count_table(self):
        """
        Test method to check if the get_table_count method of the datasource returns the expected data.
        """
        data = self.datasource.get_table_count("employees")
        self.assertEqual(data, 1)

    def test_table_record_count_no_table_name_and_query(self):
        """
        Test method to check if the get_table_count method of the datasource returns the expected data.
        """
        with self.assertRaises(ValueError):
            self.datasource.get_table_count()

    def test_table_record_count_exception(self):
        """
        Test method to check if the get_table_count method of the datasource returns the expected data.
        """
        with self.assertRaises(Exception):
            self.datasource.get_table_count("customers")

    def test_table_record_count_query(self):
        """
        Test method to check if the get_table_count method of the datasource returns the expected data.
        """
        data = self.datasource.get_table_count(query="SELECT * FROM employees")
        self.assertEqual(data, 1)

    @classmethod
    def tearDownClass(cls):
        """Teardown function runs at the end of all the tests to close the datasource connection"""
        cls.datasource.close()
