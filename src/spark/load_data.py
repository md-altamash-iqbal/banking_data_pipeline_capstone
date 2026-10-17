
import sqlite3
import pandas as pd

from pathlib import Path
from pyspark.sql import SparkSession


BASE_DIR = Path(__file__).resolve().parents[2]
DATABASE_PATH = BASE_DIR / "database" / "banking.db"

ALLOWED_TABLES = {
    "transactions",
    "accounts",
    "customers",
    "branches",
}


def load_sqlite_table(
    table_name: str,
    spark: SparkSession,
    database_path: Path = DATABASE_PATH,
):
    if table_name not in ALLOWED_TABLES:
        raise ValueError(f"Invalid table name: {table_name}")

    database_path = Path(database_path)

    if not database_path.is_file():
        raise FileNotFoundError(
            f"Database not found: {database_path}"
        )

    connection = sqlite3.connect(str(database_path))

    try:
        pandas_df = pd.read_sql_query(
            f"SELECT * FROM {table_name}",
            connection,
        )
    finally:
        connection.close()

    return spark.createDataFrame(pandas_df)


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("Week6-Banking-Data-Load")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        transactions_df = load_sqlite_table(
            "transactions",
            spark,
        )
        transactions_df.show()
    finally:
        spark.stop()
