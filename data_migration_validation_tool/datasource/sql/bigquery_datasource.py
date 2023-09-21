"""
This module defines the BigQueryDatasource class, which represents a BigQuery data source.

The BigQueryDatasource class inherits from the BaseDatasource class and provides methods for loading data, getting the schema, getting the number of records, getting the number of non-null records, and getting the number of unique values.

The BigQueryDatasource class uses a SQLAlchemy engine to connect to the data source.

The `BigQueryDatasource` class provides the following methods:

* `load_data()`: Loads the data from the data source.
* `get_schema()`: Gets the schema for the given table.
* `get_table_count()`: Gets the number of records in the table.
* `get_non_null_value_count()`: Gets the number of non-null values from the given column.
* `get_unique_value_count()`: Gets the number of unique values in the given column of the table.

example:

``` py title="example.py"
>>> from data_migration_validation_tool.datasource.bigquery_datasource import BigQueryDatasource
>>>
>>> datasource = BigQueryDatasource(
...     datasource_name="my_database",
...     connection_string="bigquery://data-migration-validation-tool/ecommerce",
... )
>>> schema = datasource.get_schema("customers")
>>> schema
{'id': 'INT64', 'first_name': 'STRING', 'last_name': 'STRING'}
>>> data = datasource.load_data("customers")
>>> data.shape
(100, 3)
>>>
>>> table_count = datasource.get_table_count("customers")
>>>
>>> non_null_value_count = datasource.get_non_null_value_count("customers", "first_name")
>>>
>>> unique_value_count = datasource.get_unique_value_count("customers", "id")

```
"""

from data_migration_validation_tool.datasource.sql.sqlalchemy_datasource import (
    SQLAlchemyDatasource,
)
import pandas as pd
from typing import Dict, Tuple, Optional
from typeguard import typechecked


class BigQueryDatasource(SQLAlchemyDatasource):
    """
    A class to represent BigQuery data sources.

    Args:
        datasource_name (str): The name of the data source.
        connection_string (str): The connection string for the data source.

    Attributes:
        project_id (str): The name of the project used by BigQuery Data Source
        dataset_id (str): The name of the dataset used by BigQuery Data Source
        name (str): The name of the data source
        type (str): The type of data source
        connection_string (str): The connection string for the data source.
        engine (sa.Engine): The SQLAlchemy engine for the data source.


    Methods:
        load_data(self, table_name): Loads the data from the data source
        get_schema(self, table_name): Gets the schema for the given table
        get_table_count(self, table_name): Gets the number of records in the table
        get_non_null_record_count(self, table_name, column_name): Gets the number of non-null records from the given column
        get_unique_value_count(self, table_name, column_name): Gets the number of unique values in the given column of the table.

    example:
    ``` py title="example.py"
    >>> datasource = BigQueryDatasource("test_datasource", "bigquery://data-migration-validation-tool/ecommerce")
    >>> datasource.name
    'test_datasource'
    >>> datasource.type
    'SQL'
    >>> datasource.connection_string
    'bigquery://data-migration-validation-tool/ecommerce'
    >>> data = datasource.load_data("customers")
    >>> data.shape
    (100, 3)

    ```
    """

    def __init__(
        self,
        datasource_name: str,
        connection_string: str,
    ):
        """
        Initializes a BigQueryDatasource object.

        Args:
            datasource_name (str): The name of the data source
            connection_string (str): The connection string for the data source.

        Usage Example:

        ``` py title="example.py"
        >>> datasource = BigQueryDatasource("test_datasource", "bigquery://data-migration-validation-tool/ecommerce")
        >>> datasource.name
        'test_datasource'
        >>> datasource.type
        'SQL'
        >>> datasource.connection_string
        'bigquery://data-migration-validation-tool/ecommerce'
        >>> datasource.project_id
        'data-migration-validation-tool'
        >>> datasource.dataset_id
        'ecommerce'

        ```
        """

        super().__init__(datasource_name, connection_string)
        connections_split_parts = self.connection_string.replace(
            "bigquery://", ""
        ).split("/", 1)
        self.project_id = connections_split_parts[0]
        self.dataset_id = (
            connections_split_parts[1] if len(connections_split_parts) == 2 else None
        )

    @typechecked
    def get_schema(self, table_name: str) -> Dict[str, str]:
        """
        Gets the schema for the given table.

        Args:
            table_name (str) : The name of the table to get the schema for. If None, all tables are loaded.

        Returns:
            A Dictionary containing the column name as the Key and data type as the Value

        Raises:
            ValueError: If table_name is invalid
        Example:

        ```
        >>> datasource = BigQueryDatasource("my_datasource", "bigquery://data-migration-validation-tool/ecommerce")
        >>> datasource.get_schema("customers")
        {'id': 'INT64', 'first_name': 'STRING', 'last_name': 'STRING'}

        ```
        """
        try:
            project_id, dataset_id, table_id = self.split_bigquery_table_name(
                table_name
            )
        except ValueError:
            raise ValueError(f"Invalid table_name: {table_name}")

        with self.engine.connect() as conn:
            df = pd.read_sql(
                f"""
SELECT COLUMN_NAME, DATA_TYPE 
FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
WHERE TABLE_NAME = '{table_id}'
            """,
                conn,
            )

        return dict(df.to_dict("split")["data"]) if not df.empty else []

    @typechecked
    def split_bigquery_table_name(self, table_name: str) -> Tuple[str, str, str]:
        """
        Splits a BigQuery table name into project_id, dataset_id and table_id.

        Args:
            table_name (str): the table name to split.

        Returns:
            Tuple[str, str, str]: a tuple of project_id, dataset_id and table_id.

        Raises:
            ValueError: if the table name is not valid.

        Examples:
            ``` py title="example.py"
            >>> datasource = BigQueryDatasource("my_datasource", "bigquery://data-migration-validation-tool/ecommerce")
            >>> datasource.split_bigquery_table_name("`project.dataset.table`")
            ('project', 'dataset', 'table')
            >>> datasource.split_bigquery_table_name("`dataset.table`")
            ('data-migration-validation-tool', 'dataset', 'table')
            >>> datasource.split_bigquery_table_name("table")
            ('data-migration-validation-tool', 'ecommerce', 'table')
            >>> datasource.split_bigquery_table_name("")
            Traceback (most recent call last):
              ...
            ValueError: invalid table name

            ```
        """
        if table_name.strip() == "":
            raise ValueError("invalid table name")

        table_name_parts = table_name.strip("`").split(".", 2)
        if len(table_name_parts) == 1:
            return tuple((self.project_id, self.dataset_id, table_name_parts[0]))
        elif len(table_name_parts) == 2:
            return tuple((self.project_id, table_name_parts[0], table_name_parts[1]))
        elif len(table_name_parts) == 3:
            return tuple(
                (table_name_parts[0], table_name_parts[1], table_name_parts[2])
            )
        else:
            raise ValueError("invalid table name")

    @typechecked
    def generate_bigquery_table_name(
        self,
        table_id: str,
        dataset_id: Optional[str] = None,
        project_id: Optional[str] = None,
    ) -> str:
        """
        Generates a BigQuery table name from optional arguments of table_id, dataset_id and project_id.

        Args:
            table_id (str): the table id to use
            dataset_id (str, optional): the dataset id to use. Defaults to None.
            project_id (str, optional): the project id to use. Defaults to None.

        Returns:
            str: the generated table name.

        Raises:
            ValueError: If table_id is empty

        example:
        ``` py title="example.py"
        >>> datasource = BigQueryDatasource("my_datasource", "bigquery://data-migration-validation-tool/ecommerce")
        >>> datasource.generate_bigquery_table_name("table")
        '`data-migration-validation-tool.ecommerce.table`'
        >>> datasource.generate_bigquery_table_name("table", "dataset")
        '`data-migration-validation-tool.dataset.table`'
        >>> datasource.generate_bigquery_table_name("table", "dataset", "project")
        '`project.dataset.table`'
        >>> datasource.generate_bigquery_table_name("")
        Traceback (most recent call last):
          ...
        ValueError: table_id should not be empty

        ```
        """
        table_name = ""
        if table_id.strip(" ") == "":
            raise ValueError("table_id should not be empty")

        if not project_id and dataset_id:
            table_name = self.project_id + "." + dataset_id + "." + table_id
        elif (not project_id) and (not dataset_id):
            table_name = self.project_id + "." + self.dataset_id + "." + table_id
        elif project_id and dataset_id:
            table_name = project_id + "." + dataset_id + "." + table_id

        return "`" + table_name + "`"

    def __repr__(self) -> str:
        """
        A function to represent the BaseDatasource class as a string
        Returns
            A string representing the Base Datasource

        example:
        ``` py title="example.py"
        >>> datasource = BigQueryDatasource("test_datasource", "bigquery://data-migration-validation-tool/ecommerce")
        >>> datasource
        <BigQueryDatasource(name='test_datasource', type='SQL', project_id='data-migration-validation-tool', connection_string='bigquery://data-migration-validation-tool/ecommerce')>

        ```
        """
        return f"<BigQueryDatasource(name='{self.name}', type='{self.type}', project_id='{self.project_id}', connection_string='{self.connection_string}')>"
