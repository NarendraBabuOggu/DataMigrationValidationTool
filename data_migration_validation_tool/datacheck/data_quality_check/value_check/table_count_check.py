"""This module contains the TableCountCheck class, which is a subclass of the BaseCheck class.

The TableCountCheck class is used to validate whether the number of rows in a given table matches a given expected value.
The check is performed by querying the table count from the data source and comparing it to the expected value.

If the table count does not match the expected value, then the check will fail. Otherwise, the check will pass.

This module also contains a doctest for the TableCountCheck class.

Methods:
        run: Executes the data check and returns the result as a dictionary with the check name, status and arguments specific to the check, where status is a boolean that indicates whether the data check Success or Failed.

example:
``` py title=example.py
>>> from data_migration_validation_tool.datacheck.data_quality_check.value_check.table_count_check import TableCountCheck
>>> from data_migration_validation_tool.datasource import get_datasource
>>> check = TableCountCheck()
>>> check.name
'table_count_check'
>>> check.type
'value_check'
>>> check.description
'A Check to compare the number of Rows in a given table'
>>> check
TableCountCheck(name=table_count_check, type=value_check, description=A Check to compare the number of Rows in a given table)
>>> datasource = get_datasource("bigquery", "my_datasource", "bigquery://data-migration-validation-tool/ecommerce")
>>> query_config = {"datasource": datasource, "table_name": "customers"}
>>> check_config = {"count": 100}
>>> check_result = check.run(query_config, check_config)
>>> check_result
{'table_count_check': {'status': 'Success', 'actual_table_count': 100, 'expected_table_count': 100}}
>>> query_config = {"datasource": datasource, "query": "SELECT * FROM ecommerce.customers"}
>>> check_config = {"count": 20}
>>> check_result = check.run(query_config, check_config)
>>> check_result
{'table_count_check': {'status': 'Failed', 'actual_table_count': 100, 'expected_table_count': 20}}

"""
from data_migration_validation_tool.datacheck.base_check import BaseCheck
from typing import Dict, Union, Optional
from data_migration_validation_tool.datasource.base_datasource import BaseDatasource
from datetime import datetime


class TableCountCheck(BaseCheck):
    """A class to represent a data check for table count.

    This class inherits from the BaseCheck class and implements the run() method to validate
    whether the number of rows in a given table matches a given expected value.

    Attributes:
        name: A string that represents the name of the data check.
        type: A string that represents the type of the data check, such as 'value_check'.
        description: A string that provides a brief description of the data check.

    Methods:
        run: Executes the data check and returns the result as a dictionary with the check name, status and arguments specific to the check, where status is a boolean that indicates whether the data check Success or Failed.

    Raises:
        InvalidQueryConfigException: If query_config is invalid or missing required keys or values.
        InvalidCheckConfigException: If check_config is invalid or missing required keys or values.
    """

    def __init__(self):
        super().__init__(
            "table_count_check",
            "value_check",
            "A Check to compare the number of Rows in a given table",
        )

    def run(
        self,
        query_config: Dict[str, Union[str, BaseDatasource, int, float, datetime.date]],
        check_config: Dict[str, Union[str, BaseDatasource, int, float, datetime.date]],
    ) -> Optional[Dict[str, Dict]]:
        """Executes the data check and returns the result as a dictionary with the check name, status and arguments specific to the check, where status is a boolean that indicates whether the data check Success or Failed.

        Args:
            query_config: A dictionary that contains the query configuration for the data sources. The keys are 'datasource' and 'table_name', and the values are dictionaries with the following keys:
                - 'datasource': A BaseDatasource object that represents the data source.
                - 'table_name': A string that represents the Table Name to use from the data source.
                - 'params': A dictionary that contains the query parameters as key-value pairs. Optional.
            check_config: A dictionary that contains the check configuration for the data sources. The keys are 'count', and the values are integers.

        Returns:
            A dictionary with the check name, status and arguments specific to the check, where status is a boolean that indicates whether the data check Success or Failed.
        Raises:
            InvalidQueryConfigException: If query_config is invalid or missing required keys or values.
            InvalidCheckConfigException: If check_config is invalid or missing required keys or values.

        example:
        ``` py
        >>> from data_migration_validation_tool.datasource import get_datasource
        >>> table_count_check = TableCountCheck()
        >>> datasource = get_datasource("bigquery", "my_datasource", "bigquery://data-migration-validation-tool/ecommerce")
        >>> query_config = {"datasource": datasource, "table_name": "customers"}
        >>> check_config = {"count": 100}
        >>> check_result = table_count_check.run(query_config, check_config)
        >>> check_result
        {'table_count_check': {'status': 'Success', 'actual_table_count': 100, 'expected_table_count': 100}}
        >>> query_config = {"datasource": datasource, "query": "SELECT * FROM ecommerce.customers"}
        >>> check_config = {"count": 20}
        >>> check_result = table_count_check.run(query_config, check_config)
        >>> check_result
        {'table_count_check': {'status': 'Failed', 'actual_table_count': 100, 'expected_table_count': 20}}
        >>> try:
        ...     table_count_check.run({"wrong_key": "wrong_value"}, {})
        ... except Exception as e:
        ...     print(e)
        Query Config should have any one of the Params ('datasource',).
        >>> try:
        ...     table_count_check.run(query_config, {})
        ... except Exception as e:
        ...     print(e)
        Check Config should have any one of the Params ('count',).

        ```
        """
        try:
            BaseCheck.validate_query_config(query_config)
            BaseCheck.validate_check_config(check_config, (("count",),))
            table_name = self.extract_table_name(query_config)
            datasource: BaseDatasource = query_config["datasource"]
            table_count = datasource.get_table_count(table_name)
        except Exception as e:
            raise e

        return {
            self.name: {
                "status": "Failed"
                if table_count != check_config["count"]
                else "Success",
                "actual_table_count": table_count,
                "expected_table_count": check_config["count"],
            }
        }
