import unittest
from data_migration_validation_tool.datacheck import (
    EmptyTableCheck,
)
from data_migration_validation_tool.datasource import get_datasource


class TestEmptyTableCheck(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.datacheck = EmptyTableCheck()
        cls.datasource = get_datasource(
            "bigquery",
            "my_datasource",
            "bigquery://data-migration-validation-tool/ecommerce",
        )

    def test_is_empty_table_with_table_name(self):
        actual_result = self.datacheck.run(
            query_config={"datasource": self.datasource, "table_name": "customers"},
            check_config={},
        )
        expected_result = {
            "empty_table_check": {"status": "Success", "record_count": 100}
        }

        self.assertDictEqual(actual_result, expected_result)

    def test_is_empty_table_with_query_positive_case(self):
        actual_result = self.datacheck.run(
            query_config={
                "datasource": self.datasource,
                "query": "SELECT id, first_name FROM customers WHERE id < 10",
            },
            check_config={},
        )
        expected_result = {
            "empty_table_check": {"status": "Success", "record_count": 9}
        }

        self.assertDictEqual(actual_result, expected_result)

    def test_is_empty_table_with_query_negative_case(self):
        actual_result = self.datacheck.run(
            query_config={
                "datasource": self.datasource,
                "query": "SELECT id, first_name FROM customers WHERE id IS NULL",
            },
            check_config={},
        )
        expected_result = {"empty_table_check": {"status": "Failed", "record_count": 0}}

        self.assertDictEqual(actual_result, expected_result)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.datasource.close()


if __name__ == "__main__":
    unittest.main()
