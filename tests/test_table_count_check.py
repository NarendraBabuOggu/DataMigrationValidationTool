import unittest
from data_migration_validation_tool.datasource import get_datasource
from data_migration_validation_tool.datacheck import get_datacheck
from data_migration_validation_tool.exceptions import (
    InvalidCheckConfigException,
    InvalidQueryConfigException,
)


class TestTableCountCheck(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.datacheck = get_datacheck("table_count_check")
        cls.datasource = get_datasource(
            "bigquery",
            "my_datasource",
            "bigquery://data-migration-validation-tool/ecommerce",
        )

    def test_table_count_with_table_name_positive_case(self):
        actual_result = self.datacheck.run(
            query_config={"datasource": self.datasource, "table_name": "customers"},
            check_config={"count": 100},
        )
        expected_result = {
            "table_count_check": {
                "status": "Success",
                "actual_table_count": 100,
                "expected_table_count": 100,
            }
        }

        self.assertDictEqual(actual_result, expected_result)

    def test_table_count_with_table_name_negative_case(self):
        actual_result = self.datacheck.run(
            query_config={"datasource": self.datasource, "table_name": "customers"},
            check_config={"count": 10},
        )
        expected_result = {
            "table_count_check": {
                "status": "Failed",
                "actual_table_count": 100,
                "expected_table_count": 10,
            }
        }

        self.assertDictEqual(actual_result, expected_result)

    def test_table_count_with_query_positive_case(self):
        actual_result = self.datacheck.run(
            query_config={
                "datasource": self.datasource,
                "query": "SELECT id, first_name FROM customers WHERE id < 10",
            },
            check_config={"count": 9},
        )
        expected_result = {
            "table_count_check": {
                "status": "Success",
                "actual_table_count": 9,
                "expected_table_count": 9,
            }
        }

        self.assertDictEqual(actual_result, expected_result)

    def test_table_count_with_query_negative_case(self):
        actual_result = self.datacheck.run(
            query_config={
                "datasource": self.datasource,
                "query": "SELECT id, first_name FROM customers WHERE id IS NULL",
            },
            check_config={"count": 10},
        )
        expected_result = {
            "table_count_check": {
                "status": "Failed",
                "actual_table_count": 0,
                "expected_table_count": 10,
            }
        }

        self.assertDictEqual(actual_result, expected_result)

    def test_table_count_with_query_invalid_check_config(self):
        with self.assertRaises(InvalidCheckConfigException) as e:
            self.datacheck.run(
                query_config={
                    "datasource": self.datasource,
                    "query": "SELECT id, first_name FROM customers WHERE id IS NULL",
                },
                check_config={},
            )

    def test_table_count_with_query_invalid_query_config(self):
        with self.assertRaises(InvalidQueryConfigException) as e:
            self.datacheck.run(
                query_config={
                    "query": "SELECT id, first_name FROM customers WHERE id IS NULL",
                },
                check_config={},
            )

        with self.assertRaises(InvalidQueryConfigException) as e:
            self.datacheck.run(
                query_config={"datasource": self.datasource},
                check_config={},
            )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.datasource.close()


if __name__ == "__main__":
    unittest.main()
