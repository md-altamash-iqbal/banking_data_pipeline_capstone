import sqlite3
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, try_to_date, when

# 1. Create SparkSession

spark = (
    SparkSession.builder
    .appName("Week6-Banking-Transformations")
    .master("local[*]")
    .getOrCreate()
)

# 2. Project path

BASE_DIR = Path(__file__).resolve().parents[2]

DATABASE_PATH = BASE_DIR / "database" / "banking.db"

# 3. Load transaction from banking.db

connection = sqlite3.connect(str(DATABASE_PATH))

try:
    transactions_pandas = pd.read_sql_query(
        "SELECT * FROM transactions",
        connection
    )
finally:
    connection.close()


transactions_df = spark.createDataFrame(transactions_pandas)

# Normalize transaction

transactions_df = transactions_df.withColumn(
    "transaction_date",
    coalesce(
        try_to_date(col("transaction_date"), "yyyy-MM-dd"),
        try_to_date(col("transaction_date"), "M/d/yyyy")
    )
)

# 4. CREDIT-only transactions

credit_df = transactions_df.filter(
    transactions_df.transaction_type == "CREDIT"
)

print("\n" + "=" * 60)
print("CREDIT-ONLY TRANSACTIONS")
print("=" * 60)

credit_df.show(truncate=False)

print("CREDIT transaction count:", credit_df.count())

# 5. High-value transactions
# Rule :- Transaction with amount >= 500 are considered high-value.
  
HIGH_VALUE_THRESHOLD = 500.0

high_value_df = transactions_df.filter(
    transactions_df.amount >= HIGH_VALUE_THRESHOLD
)

print("\n" + "=" * 60)
print("HIGH-VALUE TRANSACTIONS")
print("=" * 60)

print(
    f"High-value threshold: {HIGH_VALUE_THRESHOLD}"
)

high_value_df.show(truncate=False)

print(
    "High-value transaction count:",
    high_value_df.count()
)

# 6. Selected reporting columns

report_df = transactions_df.select(
    "transaction_id",
    "account_id",
    "transaction_date",
    "transaction_type",
    "amount"
)

print("\n" + "=" * 60)
print("SELECTED REPORTING COLUMNS")
print("=" * 60)

report_df.show(truncate=False)

# 7. Derived trasaction category

classified_df = transactions_df.withColumn(
    "transaction_category",
    when(
        transactions_df.amount >= 500,
        "HIGH_VALUE"
    )
    .when(
        transactions_df.amount >= 100,
        "MEDIUM_VALUE"
    )
    .otherwise("LOW_VALUE")
)

print("\n" + "=" * 60)
print("DERIVED TRANSACTION CATEGORY")
print("=" * 60)

classified_df.select(
    "transaction_id",
    "transaction_type",
    "amount",
    "transaction_category"
).show(truncate=False)

spark.stop()