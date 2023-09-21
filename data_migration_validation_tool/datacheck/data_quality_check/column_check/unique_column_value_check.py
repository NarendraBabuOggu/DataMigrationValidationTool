"""This module contains the UniqueColumnValueCheck class, which is a subclass of the BaseCheck class.

The UniqueColumnValueCheck class is used to validate whether the values in the given column are unique.
The check is performed by querying the unique value count for the given column from the data source and comparing it to the total number of records in the table.

If the unique value count is greater than or equal to the threshold, then the check will fail. Otherwise, the check will pass.

This module also contains a doctest for the UniqueColumnValueCheck class.

example:
``` py title="example.py"
>>> from data_migration_validation_tool.datasource import get_datasource
>>> check = UniqueColumnValueCheck()
>>> check.name
'unique_column_value_check'
>>> check.description
'A Check to validate whether all the values in the given column are unique'
>>> datasource = get_datasource("bigquery", "my_datasource", "bigquery://data-migration-validation-tool/ecommerce")
>>> query_config = {"datasource": datasource, "table_name": "customers", "column_name": "id"}
>>> check_config = {"threshold": 0.99}
>>> check_result = check.run(query_config, check_config)
>>> check_result
{'unique_column_value_check': {'status': 'Success', 'expected_unique_value_count': 100, 'actual_unique_value_count': 100, 'matching_percentage': 100.0, 'threshold': 0.99}}
>>> query_config = {"datasource": datasource, "query": "SELECT last_name FROM ecommerce.customers", "column_name": "last_name"}
>>> check_config = {}
>>> check_result = check.run(query_config, check_config)
>>> check_result
{'unique_column_value_check': {'status': 'Failed', 'expected_unique_value_count': 19, 'actual_unique_value_count': 100, 'matching_percentage': 19.0, 'threshold': 1}}

```
"""
from data_migration_validation_tool.datacheck.base_check import BaseCheck
from typing import Dict, Union, Type, Optional
from data_migration_validation_tool.datasource import DATASOURCE
from datetime import datetime


class UniqueColumnValueCheck(BaseCheck):
    """A class to represent a data check for unique column value.

    This class inherits from the BaseCheck class and implements the run() method to validate
    whether the values in the given column are unique.

    Attributes:
        name: A string that represents the name of the data check.
        type: A string that represents the type of the data check, such as 'column_check'.
        description: A string that provides a brief description of the data check.

    Methods:
        run: Executes the data check and returns the result as a dictionary with the check name, status and arguments specific to the check, where status is a boolean that indicates whether the data check Success or Failed.

    Raises:
        InvalidQueryConfigException: If query_config is invalid or missing required keys or values.
        InvalidCheckConfigException: If check_config is invalid or missing required keys or values.
    """

    def __init__(self):
        super().__init__(
            "unique_column_value_check",
            "column_check",
            "A Check to validate whether all the values in the given column are unique",
        )

    def run(
        self,
        query_config: Dict[
            str, Union[str, Type[DATASOURCE], int, float, datetime.date]
        ],
        check_config: Dict[
            str, Union[str, Type[DATASOURCE], int, float, datetime.date]
        ],
    ) -> Optional[Dict[str, Dict]]:
        """Executes the data check and returns the result as a dictionary with the check name, status and arguments specific to the check, where status is a boolean that indicates whether the data check Success or Failed.

        Args:
            query_config: A dictionary that contains the query configuration for the data sources. The keys are 'datasource', 'table_name', 'column_name', and the values are dictionaries with the following keys:
                - 'datasource': A BaseDatasource object that represents the data source.
                - 'table_name': A string that represents the Table Name to use from the data source.
                - 'column_name': A string that represents the Column Name to use from the table.
                - 'params': A dictionary that contains the query parameters as key-value pairs. Optional.
            check_config: A dictionary that contains the check configuration for the data sources. The keys are 'threshold', and the values are integers.

        Returns:
            A dictionary with the check name, status and arguments specific to the check, where status is a boolean that indicates whether the data check Success or Failed.

        Raises:
            InvalidQueryConfigException: If query_config is invalid or missing required keys or values.
            InvalidCheckConfigException: If check_config is invalid or missing required keys or values.

        example:
        ``` py
        >>> from data_migration_validation_tool.datasource import get_datasource
        >>> check = UniqueColumnValueCheck()
        >>> datasource = get_datasource("bigquery", "my_datasource", "bigquery://data-migration-validation-tool/ecommerce")
        >>> query_config = {"datasource": datasource, "table_name": "customers", "column_name": "id"}
        >>> check_config = {"threshold": 0.99}
        >>> check_result = check.run(query_config, check_config)
        >>> check_result
        {'unique_column_value_check': {'status': 'Success', 'expected_unique_value_count': 100, 'actual_unique_value_count': 100, 'matching_percentage': 100.0, 'threshold': 0.99}}
        >>> query_config = {"datasource": datasource, "query": "SELECT last_name FROM ecommerce.customers", "column_name": "last_name"}
        >>> check_config = {}
        >>> check_result = check.run(query_config, check_config)
        >>> check_result
        {'unique_column_value_check': {'status': 'Failed', 'expected_unique_value_count': 19, 'actual_unique_value_count': 100, 'matching_percentage': 19.0, 'threshold': 1}}
        >>> try:
        ...     check.run({"wrong_key": "wrong_value"}, {})
        ... except Exception as e:
        ...     print(e)
        Query Config should have any one of the Params ('datasource',).
        >>> try:
        ...     check.run({"datasource": datasource, "table_name": "customers"}, {})
        ... except Exception as e:
        ...     print(e)
        Query Config should have any one of the Params ('column_name',).

        ```
        """
        try:
            BaseCheck.validate_query_config(
                query_config,
                required_params=(
                    ("datasource",),
                    ("table_name", "query"),
                    ("column_name",),
                ),
            )
            table_name = self.extract_table_name(query_config)
            datasource: Type[DATASOURCE] = query_config["datasource"]
            column_name = query_config["column_name"]
            unique_value_count = datasource.get_unique_value_count(
                table_name, column_name
            )
            table_count = datasource.get_table_count(table_name)
            unique_value_percentage = unique_value_count / table_count
            threshold = check_config.get("threshold", 1)
        except Exception as e:
            raise e

        return {
            self.name: {
                "status": "Failed"
                if unique_value_percentage < threshold
                else "Success",
                "expected_unique_value_count": unique_value_count,
                "actual_unique_value_count": table_count,
                "matching_percentage": unique_value_percentage * 100,
                "threshold": threshold,
            }
        }
