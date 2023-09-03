from data_migration_validation_tool.datasource.sqlalchemy_datasource import (
    SQLAlchemyDataSource,
)
import pandas as pd
from typing import List, Dict


class BigQueryDataSource(SQLAlchemyDataSource):
    """ """

    def __init__(
        self,
        datasource_name: str,
        connection_string: str,
        full_table_name: str = None,
        table_id: str = None,
        dataset_id: str = None,
        project_id: str = None,
    ):
        if not full_table_name:
            full_table_name = "`" + project_id + "." + dataset_id + "." + table_id + "`"
        super().__init__(datasource_name, full_table_name, connection_string)
        self.project_id = project_id
        self.table_id = table_id
        self.dataset_id = dataset_id

    def get_schema(
        self,
    ) -> List[Dict[str, str]]:
        """Gets the schema for the given table."""
        with self.engine.connect() as conn:
            df = pd.read_sql(
                f"""
SELECT COLUMN_NAME, DATATYPE 
FROM `{self.project_id}.{self.dataset_id}.INFORMATION_SCHEMA.COLUMNS` "
WHERE TABLE_NAME = '{self.table_id}'
            """
            )
        return df.to_dict("records") if not df.empty else []
