import sqlite3
from pathlib import Path


import pandas as pd
from pyspark.sql import SparkSession


# --------------------------------------------------
# 1. Create SparkSession
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("Week6-Banking-Data-Load")
    .master("local[*]")
    .getOrCreate()
)


# --------------------------------------------------
# 2. Project paths
# --------------------------------------------------

BASE_DIR = Path(__file__).resolve().parents[2]

DATABASE_PATH = BASE_DIR / "database" / "banking.db"


# --------------------------------------------------
# 3. Check database exists
# --------------------------------------------------

if not DATABASE_PATH.exists():
    raise FileNotFoundError(
        f"Banking database not found: {DATABASE_PATH}"
    )

print(f"Using database: {DATABASE_PATH}")


# --------------------------------------------------
# 4. Load SQLite tables into Spark DataFrames
# --------------------------------------------------

def load_sqlite_table(table_name):

    connection = sqlite3.connect(str(DATABASE_PATH))

    try:
        pandas_df = pd.read_sql_query(
            f"SELECT * FROM {table_name}",
            connection
        )

        return spark.createDataFrame(pandas_df)

    finally:
        connection.close()


transactions_df = load_sqlite_table("transactions")
accounts_df = load_sqlite_table("accounts")
customers_df = load_sqlite_table("customers")
branches_df = load_sqlite_table("branches")


# --------------------------------------------------
# 5. Function to inspect a DataFrame
# --------------------------------------------------

def inspect_dataframe(name, df):

    print("\n" + "=" * 60)
    print(name)
    print("=" * 60)

    print("\nSample rows:")
    df.show(5, truncate=False)

    print("\nSchema:")
    df.printSchema()

    print("\nRow count:")
    print(df.count())


# --------------------------------------------------
# 6. Inspect all DataFrames
# --------------------------------------------------

inspect_dataframe("TRANSACTIONS", transactions_df)
inspect_dataframe("ACCOUNTS", accounts_df)
inspect_dataframe("CUSTOMERS", customers_df)
inspect_dataframe("BRANCHES", branches_df)


# --------------------------------------------------
# 7. Stop Spark
# --------------------------------------------------

spark.stop()