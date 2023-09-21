"""This module contains the CheckColumnValueBelongToSet class, which is a subclass of the BaseCheck class.

The CheckColumnValueBelongToSet class is used to validate whether the values in the given column belong to a set of values.
The check is performed by querying the unique values for the given column from the data source and comparing it to the expected set of values.

If the percentage of matching values is less than the threshold, then the check will fail. Otherwise, the check will pass.

This module also contains a doctest for the CheckColumnValueBelongToSet class.
example:
``` py title="example.py"
>>> from data_migration_validation_tool.datasource import get_datasource
>>> check = CheckColumnValueBelongToSet()
>>> check.name
'check_column_value_belong_to_set'
>>> check.description
'A Check to validate whether the values in the given column belongs to a set of values'
>>> datasource = get_datasource("bigquery", "my_datasource", "bigquery://data-migration-validation-tool/ecommerce")
>>> query_config = {"datasource": datasource, "query": "SELECT category FROM UNNEST(['flower', 'flower', 'fruit', 'fruit', 'fruit', 'flower']) category", "column_name": "category"}
>>> check_config = {"column_value_set": ["fruit", "flower"], "threshold": 0.99}
>>> check_result = check.run(query_config, check_config)
>>> check_result
{'check_column_value_belong_to_set': {'status': 'Success', 'actual_unique_values': ['flower', 'fruit'], 'expected_unique_values': ['flower', 'fruit'], 'matching_percentage': 100.0, 'threshold': 0.99}}
>>> query_config = {"datasource": datasource, "query": "SELECT category FROM UNNEST(['flower', 'flower', 'fruit', 'fruit', 'fruit', 'flower', 'tree']) category", "column_name": "category"}
>>> check_config = {"column_value_set": ["fruit", "flower"]}
>>> check_result = check.run(query_config, check_config)
>>> check_result
{'check_column_value_belong_to_set': {'status': 'Failed', 'actual_unique_values': ['flower', 'fruit', 'tree'], 'expected_unique_values': ['flower', 'fruit'], 'matching_percentage': 66.67, 'threshold': 1}}
>>> try:
...     check.run({"wrong_key": "wrong_value"}, {})
... except Exception as e:
...     print(e)
Query Config should have any one of the Params ('datasource',).

```
"""

from data_migration_validation_tool.datacheck.base_check import BaseCheck
from typing import Dict, Union, Type, Optional
from data_migration_validation_tool.datasource import DATASOURCE
from datetime import datetime


class CheckColumnValueBelongToSet(BaseCheck):
    """A class to represent a data check for column value belongs to set.

    This class inherits from the BaseCheck class and implements the run() method to validate
    whether the values in the given column belong to a set of values.

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
            "check_column_value_belong_to_set",
            "column_check",
            "A Check to validate whether the values in the given column belongs to a set of values",
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
            check_config: A dictionary that contains the check configuration for the data sources. The keys are 'column_value_set', and the values are dictionaries with the following keys:
                - 'column_value_set': A set of values that the column values should belong to.

        Returns:
            A dictionary with the check name, status and arguments specific to the check, where status is a boolean that indicates whether the data check Success or Failed.

        Raises:
            InvalidQueryConfigException: If query_config is invalid or missing required keys or values.
            InvalidCheckConfigException: If check_config is invalid or missing required keys or values.

        example:
        ``` py
        >>> from data_migration_validation_tool.datasource import get_datasource
        >>> check = CheckColumnValueBelongToSet()
        >>> datasource = get_datasource("bigquery", "my_datasource", "bigquery://data-migration-validation-tool/ecommerce")
        >>> query_config = {"datasource": datasource, "query": "SELECT category FROM UNNEST(['flower', 'flower', 'fruit', 'fruit', 'fruit', 'flower']) category", "column_name": "category"}
        >>> check_config = {"column_value_set": ["fruit", "flower"], "threshold": 0.99}
        >>> check_result = check.run(query_config, check_config)
        >>> check_result
        {'check_column_value_belong_to_set': {'status': 'Success', 'actual_unique_values': ['flower', 'fruit'], 'expected_unique_values': ['flower', 'fruit'], 'matching_percentage': 100.0, 'threshold': 0.99}}
        >>> query_config = {"datasource": datasource, "query": "SELECT category FROM UNNEST(['flower', 'flower', 'fruit', 'fruit', 'fruit', 'flower', 'tree']) category", "column_name": "category"}
        >>> check_config = {"column_value_set": ["fruit", "flower"]}
        >>> check_result = check.run(query_config, check_config)
        >>> check_result
        {'check_column_value_belong_to_set': {'status': 'Failed', 'actual_unique_values': ['flower', 'fruit', 'tree'], 'expected_unique_values': ['flower', 'fruit'], 'matching_percentage': 66.67, 'threshold': 1}}
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
        BaseCheck.validate_query_config(
            query_config,
            required_params=(
                ("datasource",),
                ("table_name", "query"),
                ("column_name",),
            ),
        )
        BaseCheck.validate_check_config(
            check_config,
            required_params=(("column_value_set",),),
        )
        table_name = self.extract_table_name(query_config)
        datasource: Type[DATASOURCE] = query_config["datasource"]
        column_name = query_config["column_name"]

        actual_column_values = datasource.get_unique_values(table_name, column_name)
        excpected_column_values = set(check_config["column_value_set"])
        threshold = check_config.get("threshold", 1)

        matching_column_count = 0
        for column_value in actual_column_values:
            if column_value in excpected_column_values:
                matching_column_count += 1
        matching_percentage = matching_column_count / len(actual_column_values)

        return {
            self.name: {
                "status": "Failed" if matching_percentage < threshold else "Success",
                "actual_unique_values": list(sorted(actual_column_values)),
                "expected_unique_values": list(sorted(excpected_column_values)),
                "matching_percentage": round(matching_percentage * 100, 2),
                "threshold": threshold,
            }
        }
