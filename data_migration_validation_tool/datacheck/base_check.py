"""
This module contains the BaseCheck class, which is the base class for all data check subclasses.

A data check is a validation process that compares the data from two data sources based on a query configuration and a check configuration.

The module also defines the exceptions that can be raised by the data check classes, such as InvalidQueryConfigException and InvalidCheckConfigException.
"""
from typing import Dict, Union, Callable, Tuple, Optional, Type
from data_migration_validation_tool.exceptions import (
    InvalidQueryConfigException,
    InvalidCheckConfigException,
)
from typeguard import typechecked
from data_migration_validation_tool.datasource import DATASOURCE
from datetime import datetime


class BaseCheck:
    """A class to represent a base class for data check.

    This class defines the common attributes and methods for all data check subclasses.
    A data check is a validation process that compares the data from two data sources
    based on a query configuration and a check configuration.

    Attributes:
        name: A string that represents the name of the data check.
        type: A string that represents the type of the data check, such as 'count', 'sum', etc.
        description: A string that provides a brief description of the data check.

    Methods:
        run: Executes the data check and returns the result as a tuple of (status, message).
        validate_query_config: Validates the query configuration for the data sources.
        extract_table_name: Extracts the table name from the query configuration.
    """

    @typechecked
    def __init__(self, check_name: str, check_type: str, description: str = "") -> None:
        """Initializes a BaseCheck object with the given attributes.

        Args:
            check_name: A string that represents the name of the data check.
            check_type: A string that represents the type of the data check, such as 'count', 'sum', etc.
            description: A string that provides a brief description of the data check. Defaults to "".

        Raises:
            ValueError: If name or type is empty or None.
        """
        if not check_name or not check_type:
            raise ValueError("Name and type cannot be empty or None.")
        self.name = check_name
        self.type = check_type
        self.description = description

    @typechecked
    def run(
        self,
        query_config: Dict[
            str, Union[str, Type[DATASOURCE], int, float, datetime.date]
        ],
        check_config: Dict[
            str, Union[str, Type[DATASOURCE], int, float, datetime.date]
        ],
    ) -> Optional[Dict[str, Dict]]:
        """Executes the data check and returns the result as a tuple of (status, message).

        This method must be implemented by subclasses to perform the specific data validation logic.

        Args:
            query_config: A dictionary that contains the query configuration for the data sources.
                The keys are 'source1' and 'source2', and the values are dictionaries with the following keys:
                    - 'datasource': A BaseDatasource object that represents the data source.
                    - 'query': A string that represents the SQL query to execute on the data source.
                    - 'table_name': A string that represents the Table Name to use from the data source.
                    - 'params': A dictionary that contains the query parameters as key-value pairs. Optional.
            check_config: A dictionary that contains the check configuration for the data validation.
                The keys and values depend on the type of the data check.

        Returns:
            A dictionary with the check name, status and arguments specific to the check, where status is a boolean that indicates whether the data check Success or Failed,

        Raises:
            InvalidQueryConfigException: If query_config is invalid or missing required keys or values.
            InvalidCheckConfigException: If check_config is invalid or missing required keys or values.
            NotImplementedError: If this method is not implemented by subclasses.
        """
        raise NotImplementedError("This method must be implemented by subclasses.")

    @staticmethod
    @typechecked
    def validate_query_config(
        query_config: Dict[
            str, Union[str, Type[DATASOURCE], int, float, datetime.date]
        ],
        required_params: Tuple[Tuple[str, ...], ...] = (
            ("datasource",),
            ("table_name", "query"),
        ),
    ) -> None:
        """Validates the query configuration for the data sources.

        This method checks if the query configuration has any one of the required parameters for each data source.

        Args:
            query_config: A dictionary that contains the query configuration for the data sources.
                The keys are 'source1' and 'source2', and the values are dictionaries with the following keys:
                    - 'datasource': A BaseDatasource object that represents the data source.
                    - 'query': A string that represents the SQL query to execute on the data source.
                    - 'params': A dictionary that contains the query parameters as key-value pairs. Optional.
            required_params: A tuple of tuples that contains the required parameters for each                 data source. For example, (('datasource',), ('table_name', 'query')) means that each data source
                must have either a 'datasource' or a 'table_name' and 'query'. Defaults to (('datasource',), ('table_name', 'query')).

        Raises:
            InvalidQueryConfigException: If query_config is invalid or missing required keys or values.
        """
        for required_param in required_params:
            if any([param in query_config for param in required_param]):
                pass
            else:
                raise InvalidQueryConfigException(
                    f"Query Config should have any one of the Params {required_param}."
                )

    @staticmethod
    @typechecked
    def validate_check_config(
        check_config: Dict[
            str, Union[str, Type[DATASOURCE], int, float, datetime.date]
        ],
        required_params: Tuple[Tuple[str, ...], ...],
    ) -> None:
        """Validates the query configuration for the data sources.

        This method checks if the datacheck configuration has any one of the required parameters for each data source.

        Args:
            check_config: A dictionary that contains the datacheck configuration for the data checks.
                A dictionary that contains the datacheck parameters as key-value pairs.
            required_params: A tuple of tuples that contains the required parameters for each

        Raises:
            InvalidCheckConfigException: If query_config is invalid or missing required keys or values.

        """
        for required_param in required_params:
            if any([param in check_config for param in required_param]):
                pass
            else:
                raise InvalidCheckConfigException(
                    f"Check Config should have any one of the Params {required_param}."
                )

    @staticmethod
    @typechecked
    def extract_table_name(
        query_config: Dict[str, Union[str, Type[DATASOURCE], int, float, datetime.date]]
    ) -> Optional[str]:
        """Extracts the table name from the query configuration.

        This method returns the table name from the query configuration if it exists, or the query itself
        wrapped in parentheses otherwise.

        Args:
            query_config: A dictionary that contains the query configuration for a data source.
                The keys are 'datasource', 'query' and 'params', and the values are a BaseDatasource object,
                a string and a dictionary respectively.

        Returns:
            A string that represents the table name or the query.

        Raises:
            InvalidQueryConfigException: If query_config is invalid or missing required keys or values.
        """
        table_name = ""
        if "table_name" in query_config:
            table_name = query_config["table_name"]
        elif "query" in query_config:
            table_name = f"({query_config['query']})"
        else:
            raise InvalidQueryConfigException(
                "Query Config should have table_name or query."
            )
        return table_name

    def __str__(self) -> str:
        """Returns a string representation of the BaseCheck object.

        Returns:
            A string that contains the name, type and description of the data check.
        """
        return f"""
Data Check: 
{self.name} ({self.type})
{self.description}"""

    def __repr__(self) -> str:
        """Returns a string representation of the BaseCheck object.

        Returns:
            A string that contains the class name and the attributes of the data check.
        """
        return f"{self.__class__.__name__}(name={self.name}, type={self.type}, description={self.description})"
