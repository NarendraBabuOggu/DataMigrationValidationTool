from sqlalchemy import create_engine
from data_migration_validation_tool.datasource.base_datasource import BaseDatasource
import pandas as pd


class SQLAlchemyDataSource(BaseDatasource):
    """
    A class to represent SQLAlchemy data sources.

    Attributes:
        datasource_name (str): The name of the data source
        datasource_type (str): The type of data source
        connection_string (str): The connection string for the data source.

    Methods:
        load_data(self): Loads the data from the data source
        get_schema(self): Gets the schema for the given table
        get_table_count(self): Gets the number of records in the table
        get_non_null_record_count(self, column_name): Gets the number of non-null records from the given column
        get_unique_value_count(self, column_name): Gets the number of unique values in the given column of the table.
    """

    def __init__(self, datasource_name: str, table_name: str, connection_string: str):
        super().__init__(datasource_name, "SQL")
        self.table_name = table_name
        self.engine = create_engine(connection_string)

    def load_data(self) -> pd.DataFrame:
        """Loads the data from the data source."""
        with self.engine.connect() as conn:
            df = pd.read_sql(f"SELECT * FROM {self.table_name}", conn)
        return df

    def get_schema(self):
        """Gets the schema for the given table."""
        raise NotImplementedError("This method must be implemented by subclasses.")

    def get_table_count(self) -> int:
        """Gets the number of records in the table."""
        with self.engine.connect() as conn:
            df = pd.read_sql(f"SELECT COUNT(*) FROM {self.table_name}", conn)
        return df.shape[0] if not df.empty else 0

    def get_non_null_value_count(self, column_name) -> int:
        """Gets the number of non-null values from the given column."""
        with self.engine.connect() as conn:
            df = pd.read_sql(
                f"SELECT COUNT(*) FROM {self.table_name} WHERE {column_name} IS NOT NULL",
                conn,
            )
        return df.shape[0] if not df.empty else 0

    def get_unique_value_count(self, column_name) -> int:
        """Gets the number of unique values in the given column of the table."""
        with self.engine.connect() as conn:
            df = pd.read_sql(
                f"SELECT DISTINCT {column_name} FROM {self.table_name}", conn
            )
        return df.shape[0] if not df.empty else 0

    def close(self):
        self.engine.dispose()
