"""
This module defines the SQLAlchemyDatasource class, which represents a SQLAlchemy data source.

The SQLAlchemyDatasource class inherits from the BaseDatasource class and provides methods for loading data, getting the schema, getting the number of records, getting the number of non-null records, and getting the number of unique values.

The SQLAlchemyDatasource class uses a SQLAlchemy engine to connect to the data source.

The `SQLAlchemyDatasource` class provides the following methods:

* `load_data()`: Loads the data from the data source.
* `get_schema()`: Gets the schema for the given table.
* `get_table_count()`: Gets the number of records in the table.
* `get_non_null_value_count()`: Gets the number of non-null values from the given column.
* `get_unique_value_count()`: Gets the number of unique values in the given column of the table.

example:

``` py title="example.py"
>>> from data_migration_validation_tool.datasource.sqlalchemy_datasource import SQLAlchemyDatasource
>>>
>>> datasource = SQLAlchemyDatasource(
...     datasource_name="my_database",
...     connection_string="mysql+pymysql://admin:admin@localhost:3306/ecommerce",
... )
>>>
>>> data = datasource.load_data("employees")
>>>
>>> table_count = datasource.get_table_count("employees")
>>>
>>> non_null_value_count = datasource.get_non_null_value_count("employees", "name")
>>>
>>> unique_value_count = datasource.get_unique_value_count("employees", "name")

```
"""

from sqlalchemy import create_engine
from data_migration_validation_tool.datasource.base_datasource import BaseDatasource
import pandas as pd
from typing import Dict, Optional, Union, List
from typeguard import typechecked
from datetime import datetime


class SQLAlchemyDatasource(BaseDatasource):
    """
    A class to represent SQLAlchemy data sources.

    Args:
        datasource_name (str): The name of the data source.
        connection_string (str): The connection string for the data source.

    Attributes:
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
    >>> datasource = SQLAlchemyDatasource("test_datasource", "mysql+pymysql://admin:admin@localhost:3306/ecommerce")
    >>> datasource.name
    'test_datasource'
    >>> datasource.type
    'SQL'
    >>> datasource.connection_string
    'mysql+pymysql://admin:admin@localhost:3306/ecommerce'
    >>> data = datasource.load_data("employees")
    >>> data.shape
    (1, 9)

    ```
    """

    @typechecked
    def __init__(self, datasource_name: str, connection_string: str) -> None:
        """
        Initializes a SQLAlchemyDatasource object.

        Args:
            datasource_name (str): The name of the data source
            connection_string (str): The connection string for the data source.

        Usage Example:

        ``` py title="example.py"
        >>> datasource = SQLAlchemyDatasource("my_datasource", "mysql+pymysql://admin:admin@localhost:3306/ecommerce")
        >>> datasource.name
        'my_datasource'
        >>> datasource.type
        'SQL'
        >>> datasource.connection_string
        'mysql+pymysql://admin:admin@localhost:3306/ecommerce'

        ```
        """
        super().__init__(datasource_name, "SQL")
        self.connection_string = connection_string
        self.engine = create_engine(connection_string)

    @typechecked
    def load_data(
        self, table_name: Optional[str] = None, query: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        Loads the data from the data source.

        Args:
            table_name: The name of the table to load. If None, all tables are loaded.
            query: A SQL query to execute.

        Returns:
            The data as a Pandas DataFrame.

        Usage Example:
        ``` py title="example.py"
        >>> datasource = SQLAlchemyDatasource("my_datasource", "mysql+pymysql://admin:admin@localhost:3306/ecommerce")
        >>> data = datasource.load_data("employees")
        >>> data.shape
        (1, 9)

        ```
        """
        if ((not table_name) or table_name.strip() == "") and (
            (not query) or query.strip() == ""
        ):
            raise ValueError(
                "Either table_name or query is required. Both should not be None or Empty"
            )
        table_name = table_name if table_name else f"({query}) src"
        with self.engine.connect() as conn:
            try:
                df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
            except Exception as e:
                print(e)
                raise e
        return df if not df.empty else None

    def get_schema(self, table_name: str) -> Dict[str, str]:
        """
        Gets the schema for the given table.

        Args:
            table_name (str) : The name of the table to get the schema for.
        Returns:
            A Dictionary containing the column name as the Key and data type as the Value

        Example:

        ```
        >>> datasource = SQLAlchemyDatasource("my_datasource", "mysql+pymysql://admin:admin@localhost:3306/ecommerce")
        >>> try:
        ...     datasource.get_schema("employees")
        ... except NotImplementedError:
        ...     print("get_schema is not implemented for SQLAlchmeyDatasource")
        get_schema is not implemented for SQLAlchmeyDatasource

        ```
        """
        raise NotImplementedError("This method must be implemented by subclasses.")

    @typechecked
    def get_table_count(
        self, table_name: Optional[str] = None, query: Optional[str] = None
    ) -> int:
        """
        Gets the number of records in the table.

        Args:
            table_name: The name of the table to get the count for. If None, all tables are counted.
            query: A SQL query to execute.

        Returns:
            The number of records.

        Raises:
            ValueError: If both table_name and query are empty or None.

        example:

        ``` py title="example.py"
        >>> datasource = SQLAlchemyDatasource("my_datasource", "mysql+pymysql://admin:admin@localhost:3306/ecommerce")
        >>> datasource.get_table_count("employees")
        1

        ```
        """
        if ((not table_name) or table_name.strip() == "") and (
            (not query) or query.strip() == ""
        ):
            raise ValueError(
                "Either table_name or query is required. Both should not be None or Empty"
            )

        table_name = table_name if table_name else f"({query}) src"
        with self.engine.connect() as conn:
            try:
                df = pd.read_sql(f"SELECT COUNT(*) AS cnt FROM {table_name}", conn)
            except Exception as e:
                print(e)
                raise e
        return df["cnt"].tolist()[0] if not df.empty else 0

    @typechecked
    def get_non_null_value_count(
        self,
        table_name: Optional[str] = None,
        column_name: Optional[str] = None,
        query: Optional[str] = None,
    ) -> int:
        """
        Gets the number of non-null values from the given column.

        Args:
            table_name: The name of the table to get the count for. If None, all tables are counted.
            column_name: The name of the column to get the count for.
            query: A SQL query to execute.

        Returns:
            The number of non-null values.

        Raises:
            ValueError: if column_name is empty or None
                        if both table_name and query are empty or None

        example:
        ``` py title="example.py"
        >>> datasource = SQLAlchemyDatasource("my_datasource", "mysql+pymysql://admin:admin@localhost:3306/ecommerce")
        >>> datasource.get_non_null_value_count("employees", "name")
        1

        ```
        """
        if ((not table_name) or table_name.strip() == "") and (
            (not query) or query.strip() == ""
        ):
            raise ValueError(
                "Either table_name or query is required. Both should not be None or Empty"
            )
        if not column_name or column_name.strip() == "":
            raise ValueError("column_name should not be None or empty")

        table_name = table_name if table_name else f"({query}) src"
        with self.engine.connect() as conn:
            try:
                df = pd.read_sql(
                    f"SELECT COUNT(*) AS cnt FROM {table_name} WHERE {column_name} IS NOT NULL",
                    conn,
                )
            except Exception as e:
                print(e)
                raise e
        return df["cnt"].tolist()[0] if not df.empty else 0

    @typechecked
    def get_unique_values(
        self,
        table_name: Optional[str] = None,
        column_name: Optional[str] = None,
        query: Optional[str] = None,
    ) -> Optional[List[Union[str, int, float, datetime.date]]]:
        """
        Gets the number of unique values in the given column of the table.

        Args:
            table_name: The name of the table to get the count for. If None, all tables are counted.
            column_name: The name of the column to get the count for.
            query: A SQL query to execute.

        Returns:
            List of unique values.

        Raises:
            ValueError: if column_name is empty or None
                        if both table_name and query are empty or None

        example:
        ``` py title="example.py"
        >>> datasource = SQLAlchemyDatasource("my_datasource", "mysql+pymysql://admin:admin@localhost:3306/ecommerce")
        >>> datasource.get_unique_values("employees", "id")
        [1]

        ```
        """
        if ((not table_name) or table_name.strip() == "") and (
            (not query) or query.strip() == ""
        ):
            raise ValueError(
                "Either table_name or query is required. Both should not be None or Empty"
            )
        if not column_name or column_name.strip() == "":
            raise ValueError("column_name should not be None or empty")

        table_name = table_name if table_name else f"({query}) src"
        with self.engine.connect() as conn:
            try:
                df = pd.read_sql(
                    f"SELECT DISTINCT {column_name} FROM {table_name}", conn
                )
            except Exception as e:
                print(e)
                raise e
        return df[column_name].tolist() if not df.empty else []

    @typechecked
    def get_unique_value_count(
        self,
        table_name: Optional[str] = None,
        column_name: Optional[str] = None,
        query: Optional[str] = None,
    ) -> Optional[int]:
        """
        Gets the number of unique values in the given column of the table.

        Args:
            table_name: The name of the table to get the count for. If None, all tables are counted.
            column_name: The name of the column to get the count for.
            query: A SQL query to execute.

        Returns:
            The number of unique values.

        Raises:
            ValueError: if column_name is empty or None
                        if both table_name and query are empty or None

        example:
        ``` py title="example.py"
        >>> datasource = SQLAlchemyDatasource("my_datasource", "mysql+pymysql://admin:admin@localhost:3306/ecommerce")
        >>> datasource.get_unique_value_count("employees", "id")
        1

        ```
        """
        try:
            unique_values = self.get_unique_values(table_name, column_name, query)
            return len(unique_values)
        except Exception as e:
            raise e

    def close(self) -> None:
        """To Close the SQLAlchemy Engine that is created"""
        self.engine.dispose()

    def __repr__(self) -> str:
        """
        A function to represent the BaseDatasource class as a string
        Returns
            A string representing the Base Datasource

        example:
        ``` py title="example.py"
        >>> datasource = SQLAlchemyDatasource("my_datasource", "mysql+pymysql://admin:admin@localhost:3306/ecommerce")
        >>> datasource.name
        'my_datasource'
        >>> datasource
        <SQLAlchemyDatasource(name='my_datasource', type='SQL', connection_string='mysql+pymysql://admin:admin@localhost:3306/ecommerce')>
        >>> datasource = SQLAlchemyDatasource("my_datasource2", "sqlite:///:memory:")
        >>> datasource.name
        'my_datasource2'
        >>> datasource
        <SQLAlchemyDatasource(name='my_datasource2', type='SQL', connection_string='sqlite:///:memory:')>

        ```
        """
        return f"<SQLAlchemyDatasource(name='{self.name}', type='{self.type}', connection_string='{self.connection_string}')>"
