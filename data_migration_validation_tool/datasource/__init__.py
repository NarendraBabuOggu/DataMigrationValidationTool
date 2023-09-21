"""
This module defines a base class for data sources and subclasses for different types of data sources, such as SQLAlchemy and BigQuery.
It also provides functions to register new data sources and to get a data source instance based on the type, name and connection string.
"""
from data_migration_validation_tool.datasource.base_datasource import BaseDatasource
from data_migration_validation_tool.datasource.sql.sqlalchemy_datasource import (
    SQLAlchemyDatasource,
)
from data_migration_validation_tool.datasource.sql.bigquery_datasource import (
    BigQueryDatasource,
)
from typing import Union, Callable, Type, TypeVar, Dict
from typeguard import typechecked

DATASOURCE = TypeVar("DATASOURCE", bound="BaseDatasource")
DATA_SOURCES: Dict[str, Type[BaseDatasource]] = {
    "sqlalchemy": SQLAlchemyDatasource,
    "bigquery": BigQueryDatasource,
}


def register_datasource(datasource_type: str) -> Callable:
    """
    This function returns a decorator that registers a new data source class with the given type in the DATASOURCE dictionary.
    The decorator expects a function that takes a datasource name and a connection string as arguments and returns an instance of the data source class.

    Args:
        datasource_type (str): The type of the data source to register.

    Returns:
        Callable: A decorator function that registers the data source class.

    Examples:
        ``` py title="example.py"
        >>> @register_datasource("sqlite")
        ... def get_sql_datasource(datasource_name: str, file_path: str) -> SQLAlchemyDatasource:
        ...     return SQLAlchemyDatasource(datasource_name, file_path)
        ...
        >>> get_datasource("sqlite", "test", "sqlite:///:memory:")
        <SQLAlchemyDatasource(name='test', type='SQL', connection_string='sqlite:///:memory:')>

        ```
    """

    def decorator(fn: Callable) -> Callable:
        DATA_SOURCES[datasource_type] = fn
        return fn

    return decorator


@register_datasource("sqlalchemy")
def get_sqlalchemy_datasource(
    datasource_name: str,
    connection_string: str,
) -> SQLAlchemyDatasource:
    """
    Registers a SQLAlchemy data source with the given name and connection string and returns the SQLAlchemyDatasource Instance.

    Args:
        datasource_name: The name of the data source.
        connection_string: The connection string to the database.

    Returns:
       A `SQLAlchemyDatasource` object.

    example:
    ``` py title="example.py"
    >>> from data_migration_validation_tool.datasource import get_sqlalchemy_datasource
    >>> datasource = get_sqlalchemy_datasource("my_datasource", "mysql+pymysql://admin:admin@localhost:3306/ecommerce")
    >>> datasource.name
    'my_datasource'
    >>> datasource.type
    'SQL'
    >>> datasource.connection_string
    'mysql+pymysql://admin:admin@localhost:3306/ecommerce'

    ```
    """
    return SQLAlchemyDatasource(datasource_name, connection_string)


@register_datasource("bigquery")
def get_bigquery_datasource(
    datasource_name: str,
    connection_string: str,
) -> BigQueryDatasource:
    """
    Registers a BigQuery data source with the given name and connection string.

    Args:
        datasource_name: The name of the data source.
        connection_string: The connection string to the database.

    Returns:
        A `BigQueryDataSource` object.

    example:
    ``` py title="example.py"
    >>> from data_migration_validation_tool.datasource import get_bigquery_datasource
    >>> datasource = get_sqlalchemy_datasource("my_datasource", "bigquery://project_id/ecommerce")
    >>> datasource.name
    'my_datasource'
    >>> datasource.type
    'SQL'
    >>> datasource.connection_string
    'bigquery://project_id/ecommerce'

    ```
    """
    return BigQueryDatasource(datasource_name, connection_string)


@typechecked
def get_datasource(
    datasource_type: str, datasource_name: str, connection_string: str
) -> DATASOURCE:
    """
    This function returns an instance of a data source class based on the given type, name and connection string.
    It looks up the DATASOURCE dictionary for the corresponding function that creates the data source instance.
    If the type is not registered, it returns a ValueError.

    Args:
        datasource_type (str): The type of the data source to get.
        datasource_name (str): The name of the data source.
        connection_string (str): The connection string for the data source.

    Returns:
        Union[BaseDatasource, ValueError]: An instance of a data source class or a ValueError if the type is not registered.

    Examples:
        >>> get_datasource("sqlalchemy", "test", "sqlite:///:memory:")
        <SQLAlchemyDatasource(name='test', type='SQL', connection_string='sqlite:///:memory:')>
        >>> try:
        ...     get_datasource("invalid", "test", "test")
        ... except ValueError as e:
        ...     print(f"ValueError: {e}")
        ValueError: Data Source of type invalid is Not Available
    """
    if datasource_type in DATA_SOURCES:
        return DATA_SOURCES.get(datasource_type)(datasource_name, connection_string)
    else:
        raise ValueError(f"Data Source of type {datasource_type} is Not Available")
