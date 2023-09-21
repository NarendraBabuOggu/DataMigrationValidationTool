import unittest
from data_migration_validation_tool.datasource.base_datasource import BaseDatasource
from data_migration_validation_tool.datasource import (
    register_datasource,
    get_datasource,
    DATASOURCE,
)


class TestDataSource(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        @register_datasource("Test")
        def test_base_datasource(
            datasource_name: str, datasource_type: str
        ) -> BaseDatasource:
            return BaseDatasource(datasource_name, datasource_type)

        cls.datasource = get_datasource("Test", "test", "")

    def test_register_datasource(self):
        self.assertIn("Test", DATASOURCE)
        self.assertEqual(str(self.datasource), str(DATASOURCE["Test"]("test", "")))

    def test_load_data(self):
        with self.assertRaises(NotImplementedError):
            self.datasource.load_data()

    def test_get_schema(self):
        with self.assertRaises(NotImplementedError):
            self.datasource.get_schema("")

    def test_non_null_value_count(self):
        with self.assertRaises(NotImplementedError):
            self.datasource.get_non_null_value_count()

    def test_table_count(self):
        with self.assertRaises(NotImplementedError):
            self.datasource.get_table_count()

    def test_unique_value_count(self):
        with self.assertRaises(NotImplementedError):
            self.datasource.get_unique_value_count()

    def test_get_datasource(self):
        with self.assertRaises(ValueError):
            get_datasource("CSV", "", "")


if __name__ == "__main__":
    unittest.main()
