"""
This module defines a base class for data sources.

The BaseDatasource class provides a common interface for loading data, getting the schema, \
and getting the number of records in a table.

The subclasses of BaseDatasource must implement the following methods:

* load_data()
* get_schema()
* get_table_count()
* get_non_null_value_count()
* get_unique_value_count()
"""

from typing import Optional, Dict
import pandas as pd


class BaseDatasource:

    """
    Base class for data sources.

    Args:
        datasource_name (str): The name of the data source.
        datasource_type (str): The type of data source.

    Attributes:
        name (str): The name of the data source
        type (str): The type of data source

    example:
    >>> datasource = BaseDatasource("test_datasource", "csv")
    >>> datasource.name
    'test_datasource'
    >>> datasource.type
    'csv'
    """

    def __init__(self, datasource_name: str, datasource_type: str) -> None:
        """
        Initializes a BaseDatasource object.

        Args:
            datasource_name (str): The name of the data source.
            datasource_type (str): The type of data source.
        """
        self.name = datasource_name
        self.type = datasource_type

    def load_data(self, table_name: Optional[str] = None) -> pd.DataFrame:
        """
        Load the data from the data source.

        Args:
            table_name (Optional[str]): The name of the table to load.

        Returns:
            The data from the table.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.
        """
        raise NotImplementedError("This method must be implemented by subclasses.")

    def get_schema(self, table_name: str) -> Dict[str, str]:
        """
        Get the schema for the given table.

        Args:
            table_name (str): The name of the table to get the schema for.

        Returns:
            The schema for the table.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.
        """
        raise NotImplementedError("This method must be implemented by subclasses.")

    def get_table_count(
        self, table_name: Optional[str] = None, query: Optional[str] = None
    ) -> int:
        """
        Get the number of records in the table.

        Args:
            table_name (Optional[str]): The name of the table to get the count for.
            query (Optional[str]): A query to get the count.

        Returns:
            The number of records in the table.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.
        """
        raise NotImplementedError("This method must be implemented by subclasses.")

    def get_non_null_value_count(
        self,
        table_name: Optional[str] = None,
        column_name: Optional[str] = None,
        query: Optional[str] = None,
    ) -> int:
        """
        Get the number of non-null values from the given column.

        Args:
            table_name (Optional[str]): The name of the table to get the count for.
            column_name (Optional[str]): The name of the column to get the count for.
            query (Optional[str]): A query to get the count.

        Returns:
            The number of non-null values in the column.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.
        """
        raise NotImplementedError("This method must be implemented by subclasses.")

    def get_unique_value_count(
        self,
        table_name: Optional[str] = None,
        column_name: Optional[str] = None,
        query: Optional[str] = None,
    ) -> int:
        """
        Get the number of unique values in the given column of the table.

        Args:
            table_name (Optional[str]): The name of the table to get the count for.
            column_name (Optional[str]): The name of the column to get the count for.
            query (Optional[str]): A query to get the count.

        Returns:
            The number of unique values in the column.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.
        """
        raise NotImplementedError("This method must be implemented by subclasses.")

    def __repr__(self) -> str:
        """
        A function to represent the BaseDatasource class as a string
        Returns
            A string representing the Base Datasource

        example:
        ``` py title="example.py"
        >>> datasource = BaseDatasource("my_datasource", "SQL")
        >>> datasource.name
        'my_datasource'
        >>> datasource
        <BaseDatasource(name='my_datasource', type='SQL')>
        >>> datasource = BaseDatasource("my_datasource2", "CSV")
        >>> datasource.name
        'my_datasource2'
        >>> datasource
        <BaseDatasource(name='my_datasource2', type='CSV')>

        ```
        """
        return f"<BaseDatasource(name='{self.name}', type='{self.type}')>"
