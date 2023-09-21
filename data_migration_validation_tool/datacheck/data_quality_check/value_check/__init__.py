"""
This Module contains the Data Quality Checks that will be performed on queries which returns a single value.

Available Data Checks:
    - EmptyTableCheck
    - TableCountCheck
"""

from data_migration_validation_tool.datacheck.data_quality_check.value_check.empty_table_check import (
    EmptyTableCheck,
)
from data_migration_validation_tool.datacheck.data_quality_check.value_check.table_count_check import (
    TableCountCheck,
)
