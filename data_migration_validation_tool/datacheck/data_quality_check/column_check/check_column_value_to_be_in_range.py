"""This module contains the CheckColumnValueToBeInRange class, which is a subclass of the BaseCheck class.

The CheckColumnValueToBeInRange class is used to validate whether the values in the given column belong to a range of values.
The check is performed by querying the minimum and maximum values for the given column from the data source and comparing it to the expected range of values.

If the percentage of matching values is less than the threshold, then the check will fail. Otherwise, the check will pass.

This module also contains a doctest for the CheckColumnValueToBeInRange class.

example:
``` py title="example.py"
>>> from data_migration_validation_tool.datasource import get_datasource
>>> check = CheckColumnValueToBeInRange()
>>> check.name
'check_column_value_to_be_in_range'
>>> check.description
'A Check to validate whether the values in the given column are in the given range'
>>> datasource = get_datasource("bigquery", "my_datasource", "bigquery://data-migration-validation-tool/ecommerce")
>>> query_config = {"datasource": datasource, "query": "SELECT id FROM UNNEST(GENERATE_ARRAY(10, 1000, 1)) id", "column_name": "id"}
>>> check_config = {"column_value_range": { "min": 10, "max": 1000}, "threshold": 1}
>>> check_result = check.run(query_config, check_config)
>>> check_result
{'check_column_value_to_be_in_range': {'status': 'Success', 'actual_column_range': {'min': 10, 'max': 1000}, 'expected_column_range': {'min': 10, 'max': 1000}, 'matching_percentage': 100.0, 'threshold': 1}}

```
"""

from data_migration_validation_tool.datacheck.base_check import BaseCheck
from typing import Dict, Union, Type
from data_migration_validation_tool.datasource import DATASOURCE
from datetime import datetime


class CheckColumnValueToBeInRange(BaseCheck):
    """A class to represent a data check for column value belongs to set.

    This class inherits from the BaseCheck class and implements the run() method to validate
    whether the values in the given column belong to a range of values.

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
            "check_column_value_to_be_in_range",
            "column_check",
            "A Check to validate whether the values in the given column are in the given range",
        )

    def run(
        self,
        query_config: Dict[
            str, Union[str, Type[DATASOURCE], int, float, datetime.date]
        ],
        check_config: Dict[
            str, Union[str, Type[DATASOURCE], int, float, datetime.date]
        ],
    ) -> Dict[str, Dict]:
        """Executes the data check and returns the result as a dictionary with the check name, status and arguments specific to the check, where status is a boolean that indicates whether the data check Success or Failed.

        Args:
            query_config: A dictionary that contains the query configuration for the data sources. The keys are 'datasource', 'table_name', 'column_name', and the values are dictionaries with the following keys:
                - 'datasource': A BaseDatasource object that represents the data source.
                - 'table_name': A string that represents the Table Name to use from the data source.
                - 'column_name': A string that represents the Column Name to use from the table.
                - 'params': A dictionary that contains the query parameters as key-value pairs. Optional.
            check_config: A dictionary that contains the check configuration for the data sources. The keys are 'column_value_range', and the values are dictionaries with the following keys:
                - 'column_value_range': A dictionary containing the min and max values that the column values should belong to.

        Returns:
            A dictionary with the check name, status and arguments specific to the check, where status is a boolean that indicates whether the data check Success or Failed.

        Raises:
            InvalidQueryConfigException: If query_config is invalid or missing required keys or values.
            InvalidCheckConfigException: If check_config is invalid or missing required keys or values.

        example:
        ``` py
        >>> from data_migration_validation_tool.datasource import get_datasource
        >>> check = CheckColumnValueToBeInRange()
        >>> datasource = get_datasource("bigquery", "my_datasource", "bigquery://data-migration-validation-tool/ecommerce")
        >>> query_config = {"datasource": datasource, "query": "SELECT id FROM UNNEST(GENERATE_ARRAY(10, 1000, 1)) id", "column_name": "id"}
        >>> check_config = {"column_value_range": { "min": 10, "max": 1000}, "threshold": 1}
        >>> check_result = check.run(query_config, check_config)
        >>> check_result
        {'check_column_value_to_be_in_range': {'status': 'Success', 'actual_column_range': {'min': 10, 'max': 1000}, 'expected_column_range': {'min': 10, 'max': 1000}, 'matching_percentage': 100.0, 'threshold': 1}}
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
        >>> try:
        ...     check.run(query_config, {})
        ... except Exception as e:
        ...     print(e)
        Check Config should have any one of the Params ('column_value_range',).

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
            required_params=(("column_value_range",),),
        )
        table_name = self.extract_table_name(query_config)
        datasource: Type[DATASOURCE] = query_config["datasource"]
        column_name = query_config["column_name"]

        actual_column_range = datasource.load_data(
            query=f"SELECT MIN({column_name}) AS min, MAX({column_name}) AS max FROM {table_name}"
        ).to_dict("records")[0]
        expected_column_range = check_config["column_value_range"]
        matching_record_count = datasource.load_data(
            query=f"SELECT COUNT(1) AS matching_record_count FROM {table_name} WHERE {column_name} >= {expected_column_range['min']} AND {column_name} <= {expected_column_range['max']} "
        ).to_dict("records")[0]["matching_record_count"]
        table_count = datasource.get_table_count(table_name)
        matching_percentage = matching_record_count / table_count
        threshold = check_config.get("threshold", 1)

        return {
            self.name: {
                "status": "Failed" if matching_percentage < threshold else "Success",
                "actual_column_range": actual_column_range,
                "expected_column_range": expected_column_range,
                "matching_percentage": round(matching_percentage * 100, 2),
                "threshold": threshold,
            }
        }
