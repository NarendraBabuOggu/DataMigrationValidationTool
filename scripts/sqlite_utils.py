from sqlalchemy import create_engine
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Date,
    DateTime,
    FLOAT,
    text,
    inspect,
)
from datetime import datetime


def prepare_sqlite_db(conn_string: str = "sqlite:///database.sqlite3"):
    engine = create_engine(conn_string, echo=True)
    conn = engine.connect()

    if inspect(engine).has_table("employees"):
        conn.execute("DROP TABLE employees")

    metadata_obj = MetaData(engine)

    employees_table = Table(
        "employees",
        metadata_obj,
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("email", String),
        Column("mobile", String),
        Column("department", String),
        Column("salary", FLOAT),
        Column("join_date", Date),
        Column("created_datetime", DateTime, default=datetime.now),
        Column("updated_datetime", DateTime, default=datetime.now),
    )
    metadata_obj.create_all(engine)

    stmnt = employees_table.insert().values(
        name="Narendra",
        email="",
        mobile="",
        department="",
        salary="10000",
        join_date=datetime(2023, 9, 1),
    )
    conn.execute(stmnt)
