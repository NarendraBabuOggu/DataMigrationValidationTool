"""
This module defines a decorator function that registers data check functions in a global dictionary.

A data check function is a callable that creates and returns a data check object, which is an instance of a subclass of BaseCheck.

A data check object performs some validation logic on the data from two data sources
based on a query configuration and a check configuration.

The module also exposes the global dictionary of registered data check functions as DATA_CHECKS, and a function that returns a data check object from the dictionary given its name.
"""

from typing import Callable, Dict, Type, TypeVar
from data_migration_validation_tool.datacheck.base_check import BaseCheck
from data_migration_validation_tool.exceptions import InvalidDataCheckException
from data_migration_validation_tool.datacheck.data_quality_check.value_check import (
    EmptyTableCheck,
    TableCountCheck,
)
from data_migration_validation_tool.datacheck.data_quality_check.column_check import (
    UniqueColumnValueCheck,
    CheckColumnValueBelongToSet,
    CheckColumnValueToBeInRange,
)
from typeguard import typechecked

DATACHECK = TypeVar("DATACHECK", bound=BaseCheck)

# A global dictionary that maps data check names to data check functions
DATA_CHECKS: Dict[str, Type[BaseCheck]] = {
    "empty_table_check": EmptyTableCheck,
    "table_count_check": TableCountCheck,
    "unique_column_value_check": UniqueColumnValueCheck,
    "check_column_values_belong_to_set": CheckColumnValueBelongToSet,
    "check_column_values_to_be_in_range": CheckColumnValueToBeInRange,
}


@typechecked
def register_datacheck(check_name: str) -> Callable:
    """A decorator function that registers a data check function in the DATA_CHECKS dictionary.

    Args:
        check_name: A string that represents the name of the data check function.

    Returns:
        A decorator function that takes a data check function as an argument and returns it after registering it in the DATA_CHECKS dictionary.

    ```
    """

    def decorator(fn: Callable) -> Callable:
        # Register the data check function in the DATA_CHECKS dictionary with the given name
        DATA_CHECKS[check_name] = fn
        # Return the data check function
        return fn

    # Return the decorator function
    return decorator


@typechecked
def get_datacheck(check_name: str) -> DATACHECK:
    """A function that returns a data check object from the DATA_CHECKS dictionary.

    Args:
        check_name: A string that represents the name of the data check object.

    Returns:
        A data check object that corresponds to the given name.

    Raises:
        InvalidDataCheckException: If the given name is not found in the DATA_CHECKS dictionary.

    Examples:
        ``` py title="example.py"
        >>> get_datacheck("empty_table_check")
        EmptyTableCheck(name=empty_table_check, type=value_check, description=A Check to validate whether the Table has any data or not)
        >>> try:
        ...     get_datacheck("invalid")
        ... except InvalidDataCheckException as e:
        ...     print(e)
        DataCheck with Name invalid is not available

        ```
    """
    if check_name in DATA_CHECKS:
        return DATA_CHECKS[check_name]()
    else:
        raise InvalidDataCheckException(
            f"DataCheck with Name {check_name} is not available"
        )


@register_datacheck("empty_table_check")
@typechecked
def get_empty_table_check() -> EmptyTableCheck:
    """Function to create and return the EmptyTableCheck object.

    Returns:
        An EmptyTableCheck object that performs a validation logic to check if any of the two data sources is empty.

    Examples:

        ``` py title="example.py"
        >>> get_empty_table_check()
        EmptyTableCheck(name=empty_table_check, type=value_check, description=A Check to validate whether the Table has any data or not)

        ```
    """
    return EmptyTableCheck()


@register_datacheck("table_count_check")
@typechecked
def get_table_count_check() -> TableCountCheck:
    """Function to create and return the TableCountCheck object.

    Returns:
        An TableCountCheck object that performs a validation logic to check if the record counto fth egiven table is matching with the given value.

    Examples:

        ``` py title="example.py"
        >>> get_table_count_check()
        TableCountCheck(name=table_count_check, type=value_check, description=A Check to compare the number of Rows in a given table)

        ```
    """
    return TableCountCheck()


@register_datacheck("unique_column_value_check")
@typechecked
def get_unique_column_value_check() -> UniqueColumnValueCheck:
    """Function to create and return the UniqueColumnValueCheck object.

    Returns:
        An UniqueColumnValueCheck object that performs a validation logic to check whether all the values in the given column are unique.

    Examples:

        ``` py title="example.py"
        >>> get_unique_column_value_check()
        UniqueColumnValueCheck(name=unique_column_value_check, type=column_check, description=A Check to validate whether all the values in the given column are unique)

        ```
    """
    return UniqueColumnValueCheck()


@register_datacheck("check_column_value_belong_to_set")
@typechecked
def get_check_column_value_belong_to_set() -> CheckColumnValueBelongToSet:
    """Function to create and return the CheckColumnValueBelongToSet object.

    Returns:
        An CheckColumnValueBelongToSet object that performs a validation logic to check whether the values in the given column belongs to a set of values.

    Examples:

        ``` py title="example.py"
        >>> get_check_column_value_belong_to_set()
        CheckColumnValueBelongToSet(name=check_column_value_belong_to_set, type=column_check, description=A Check to validate whether the values in the given column belongs to a set of values)

        ```
    """
    return CheckColumnValueBelongToSet()


@register_datacheck("check_column_value_to_be_in_range")
@typechecked
def get_check_column_value_to_be_in_range() -> CheckColumnValueToBeInRange:
    """Function to create and return the CheckColumnValueBelongToSet object.

    Returns:
        An CheckColumnValueToBeInRange object that performs a validation logic to check whether the values in the given column are in the given range

    Examples:

        ``` py title="example.py"
        >>> get_check_column_value_to_be_in_range()
        CheckColumnValueToBeInRange(name=check_column_value_to_be_in_range, type=column_check, description=A Check to validate whether the values in the given column are in the given range)

        ```
    """
    return CheckColumnValueToBeInRange()
