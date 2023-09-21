from unittest import TestCase
from data_migration_validation_tool.datasource import get_datasource, BigQueryDatasource
import pandas as pd


class TestBigQueryDataSource(TestCase):
    """
    TestCase to run unit tests against BigQueryDataSource
    """

    @classmethod
    def setUpClass(cls):
        cls.datasource: BigQueryDatasource = get_datasource(
            "bigquery",
            "test-bigquery",
            "bigquery://data-migration-validation-tool/ecommerce",
        )

    def test_load_data_query(self):
        """Test to validate the load_data method of BigQuery Datasource"""
        data: pd.DataFrame = self.datasource.load_data(
            query="SELECT * FROM ecommerce.customers WHERE id = 4"
        )
        actual_data = data.to_dict("records")[0]
        expected_data = {"id": 4, "first_name": "Jimmy", "last_name": "C."}
        self.assertEqual(actual_data, expected_data)

    def test_load_data_table(self):
        """Test to validate the load_data method of BigQuery Datasource"""
        data: pd.DataFrame = self.datasource.load_data("customers")
        self.assertTupleEqual(data.shape, (100, 3))

    def test_load_data_table_with_dataset(self):
        """Test to validate the load_data method of BigQuery Datasource"""
        data: pd.DataFrame = self.datasource.load_data("ecommerce.customers")
        self.assertTupleEqual(data.shape, (100, 3))

    def test_load_data_table_with_project_and_dataset(self):
        """Test to validate the load_data method of BigQuery Datasource"""
        data: pd.DataFrame = self.datasource.load_data(
            "data-migration-validation-tool.ecommerce.customers"
        )
        self.assertTupleEqual(data.shape, (100, 3))

    def test_generate_bigquery_table_name_with_table_and_dataset(self):
        table_name = self.datasource.generate_bigquery_table_name(
            table_id="table_name", dataset_id="dataset"
        )
        self.assertEqual(
            table_name, "`data-migration-validation-tool.dataset.table_name`"
        )

    def test_generate_bigquery_table_name_with_table(self):
        table_name = self.datasource.generate_bigquery_table_name(table_id="table_name")
        self.assertEqual(
            table_name, "`data-migration-validation-tool.ecommerce.table_name`"
        )

    def test_generate_bigquery_table_name_with_all(self):
        table_name = self.datasource.generate_bigquery_table_name(
            table_id="table_name", dataset_id="dataset", project_id="project"
        )
        self.assertEqual(table_name, "`project.dataset.table_name`")

    def test_generate_bigquery_table_name_exception(self):
        with self.assertRaises(ValueError):
            self.datasource.generate_bigquery_table_name(table_id="")

    def test_unique_value_count(self):
        data = self.datasource.get_unique_value_count("customers", "id")
        self.assertEqual(data, 100)

    def test_non_null_value_count(self):
        data = self.datasource.get_non_null_value_count("customers", "first_name")
        self.assertEqual(data, 100)

    def test_table_record_count_table(self):
        data = self.datasource.get_table_count("customers")
        self.assertEqual(data, 100)

    def test_table_record_count_query(self):
        data = self.datasource.get_table_count(
            query="SELECT id, CONCAT(first_name, ' ', last_name) AS name FROM ecommerce.customers WHERE id <= 10"
        )
        self.assertEqual(data, 10)

    def test_table_record_count_empty_table_query(self):
        with self.assertRaises(ValueError):
            data = self.datasource.get_table_count()

    def test_get_schema(self):
        """Test to validate the get_schema method of BigQuery Datasource"""
        actual_schema = self.datasource.get_schema("customers")
        expected_schema = {"id": "INT64", "first_name": "STRING", "last_name": "STRING"}
        self.assertDictEqual(actual_schema, expected_schema)

    def test_get_schema_exception(self):
        """Test to validate the get_schema method of BigQuery Datasource"""
        with self.assertRaises(TypeError):
            self.datasource.get_schema()

    def test_split_bigquery_table_name_excetion(self):
        """Test to validate the get_schema method of BigQuery Datasource"""
        with self.assertRaises(ValueError):
            self.datasource.split_bigquery_table_name(table_name="")

    def test_split_bigquery_table_name_with_dataset(self):
        """Test to validate the get_schema method of BigQuery Datasource"""
        project_id, dataset_id, table_id = self.datasource.split_bigquery_table_name(
            table_name="`ecommerce.table_name`"
        )
        self.assertTupleEqual(
            (project_id, dataset_id, table_id),
            ("data-migration-validation-tool", "ecommerce", "table_name"),
        )

    def test_split_bigquery_table_name_with_project_and_dataset(self):
        """Test to validate the get_schema method of BigQuery Datasource"""
        project_id, dataset_id, table_id = self.datasource.split_bigquery_table_name(
            table_name="project.dataset.table_name"
        )
        self.assertTupleEqual(
            (project_id, dataset_id, table_id),
            ("project", "dataset", "table_name"),
        )

    def test_datasource_repr(self):
        self.assertEqual(
            repr(self.datasource),
            "<BigQueryDatasource(name='test-bigquery', type='SQL', project_id='data-migration-validation-tool', connection_string='bigquery://data-migration-validation-tool/ecommerce')>",
        )

    def test_get_schema_empty_table(self):
        """Test to validate the get_schema method of BigQuery Datasource"""
        with self.assertRaises(ValueError):
            actual_schema = self.datasource.get_schema("")

    @classmethod
    def tearDownClass(cls):
        cls.datasource.close()
