"""
This module contains the EmptyTableCheck class, which is a subclass of the BaseCheck class.

The EmptyTableCheck class performs a data check to validate whether a given table has any data or not.

Methods:
    run: Executes the data check and returns the result as a dictionary with the check name, status and arguments specific to the check, where status is a boolean that indicates whether the data check Success or Failed,

example:

``` py title="example.py"
>>> from data_migration_validation_tool.datacheck.data_quality_check.value_check.empty_table_check import EmptyTableCheck
>>> from data_migration_validation_tool.datasource import get_datasource
>>> check = EmptyTableCheck()
>>> check.name
'empty_table_check'
>>> check.type
'value_check'
>>> check.description
'A Check to validate whether the Table has any data or not'
>>> check
EmptyTableCheck(name=empty_table_check, type=value_check, description=A Check to validate whether the Table has any data or not)
>>> datasource = get_datasource("bigquery", "my_datasource", "bigquery://data-migration-validation-tool/ecommerce")
>>> query_config = {"datasource": datasource, "table_name": "customers"}
>>> result = check.run(query_config, {})
>>> result["empty_table_check"]["status"]
'Success'
>>> query_config = {"datasource": datasource, "query": "SELECT * FROM ecommerce.customers WHERE FALSE"}
>>> result = check.run(query_config, {})
>>> result["empty_table_check"]["status"]
'Failed'

```
"""
from data_migration_validation_tool.datacheck.base_check import BaseCheck
from typing import Dict, Union, Optional
from data_migration_validation_tool.datasource.base_datasource import BaseDatasource
from datetime import datetime


class EmptyTableCheck(BaseCheck):
    """A class to represent a data check to validate if a table is empty.

    This class inherits from the BaseCheck class and implements the run() method to validate
    whether the table has any data or not.

    Attributes:
        name: A string that represents the name of the data check.
        type: A string that represents the type of the data check, such as 'value_check'.
        description: A string that provides a brief description of the data check.

    Methods:
        run: Executes the data check and returns the result as a dictionary with the check name, status and arguments specific to the check, where status is a boolean that indicates whether the data check Success or Failed.

    Raises:
        InvalidQueryConfigException: If query_config is invalid or missing required keys or values.

    """

    def __init__(self):
        """
        example:
        ``` py
        >>> check = EmptyTableCheck()
        >>> check.name
        'empty_table_check'
        >>> check.type
        'value_check'
        >>> check.description
        'A Check to validate whether the Table has any data or not'

        ```
        """
        super().__init__(
            "empty_table_check",
            "value_check",
            "A Check to validate whether the Table has any data or not",
        )

    def run(
        self,
        query_config: Dict[str, Union[str, BaseDatasource, int, float, datetime.date]],
        check_config: Dict[str, Union[str, BaseDatasource, int, float, datetime.date]],
    ) -> Optional[Dict[str, Dict]]:
        """Executes the data check and returns the result as a dictionary with the check name, status and arguments specific to the check, where status is a boolean that indicates whether the data check Success or Failed.

        Args:
            query_config: A dictionary that contains the query configuration for the data sources. The keys are 'datasource' and ('table_name' or 'query'), and the values are dictionaries with the following keys:
                - 'datasource': A BaseDatasource object that represents the data source.
                - 'table_name': A string that represents the Table Name to use from the data source.
                - 'query': A string that represents the query to use from the data source.
            check_config: Not used in this check.

        Returns:
            A dictionary with the check name, status and arguments specific to the check, where status is a boolean that indicates whether the data check Success or Failed.

        example:
        ``` py
        >>> from data_migration_validation_tool.datacheck.data_quality_check.value_check.empty_table_check import EmptyTableCheck
        >>> from data_migration_validation_tool.datasource import get_datasource
        >>> from data_migration_validation_tool.exceptions import InvalidQueryConfigException
        >>> check = EmptyTableCheck()
        >>> datasource = get_datasource("bigquery", "my_datasource", "bigquery://data-migration-validation-tool/ecommerce")
        >>> query_config = {"datasource": datasource, "table_name": "customers"}
        >>> result = check.run(query_config, {})
        >>> result["empty_table_check"]["status"]
        'Success'
        >>> query_config = {"datasource": datasource, "query": "SELECT * FROM ecommerce.customers WHERE FALSE"}
        >>> result = check.run(query_config, {})
        >>> result["empty_table_check"]["status"]
        'Failed'
        >>> try:
        ...     result = check.run({}, {})
        ... except InvalidQueryConfigException as e:
        ...     print(e)
        Query Config should have any one of the Params ('datasource',).
        >>> try:
        ...     query_config = {"datasource": datasource}
        ...     result = check.run(query_config, {})
        ... except InvalidQueryConfigException as e:
        ...     print(e)
        Query Config should have any one of the Params ('table_name', 'query').

        ```
        """
        try:
            BaseCheck.validate_query_config(query_config)
            table_name = self.extract_table_name(query_config)
            datasource: BaseDatasource = query_config["datasource"]
            table_count = datasource.get_table_count(table_name)
        except Exception as e:
            raise e

        return {
            self.name: {
                "status": "Failed" if table_count == 0 else "Success",
                "record_count": table_count,
            }
        }
