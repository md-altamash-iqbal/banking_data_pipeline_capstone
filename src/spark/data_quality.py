import sqlite3
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count


# 1. Create SparkSession

spark = (
    SparkSession.builder
    .appName("Week6-Banking-Data-Quality")
    .master("local[*]")
    .getOrCreate()
)


# 2. Project paths

BASE_DIR = Path(__file__).resolve().parents[2]
DATABASE_PATH = BASE_DIR / "database" / "banking.db"


# 3. Load SQLite table into Spark DataFrame

def load_table(table_name):
    connection = sqlite3.connect(str(DATABASE_PATH))

    try:
        pandas_df = pd.read_sql_query(
            f"SELECT * FROM {table_name}",
            connection
        )

        return spark.createDataFrame(pandas_df)

    finally:
        connection.close()


# 4. Load required tables

transactions_df = load_table("transactions")
accounts_df = load_table("accounts")
customers_df = load_table("customers")
branches_df = load_table("branches")


# 5. Helper function for PASS / FAIL results

def print_result(check_name, bad_count):
    status = "PASS" if bad_count == 0 else "FAIL"

    print(
        f"{status:<6} | "
        f"{check_name:<50} | "
        f"Bad records: {bad_count}"
    )



# 6. Data Quality Checks

print("\n" + "=" * 90)
print("DATA QUALITY CHECKS")
print("=" * 90)

print("\n" + "-" * 90)
print("NULL VALUE CHECKS")
print("-" * 90)



# Check 1: Null transaction IDs

null_transaction_id_count = (
    transactions_df
    .filter(col("transaction_id").isNull())
    .count()
)

print_result(
    "Null transaction IDs",
    null_transaction_id_count
)


# Check 2: Null account IDs

null_account_id_count = (
    transactions_df
    .filter(col("account_id").isNull())
    .count()
)

print_result(
    "Null account IDs",
    null_account_id_count
)


# Check 3: Null amounts

null_amount_count = (
    transactions_df
    .filter(col("amount").isNull())
    .count()
)

print_result(
    "Null transaction amounts",
    null_amount_count
)


# 7. Duplicate Transaction ID Check

print("\n" + "-" * 90)
print("DUPLICATE CHECK")
print("-" * 90)

duplicate_transaction_ids = (
    transactions_df
    .groupBy("transaction_id")
    .count()
    .filter(col("count") > 1)
)

duplicate_transaction_id_count = duplicate_transaction_ids.count()

print_result(
    "Duplicate transaction IDs",
    duplicate_transaction_id_count
)


# 8. Transaction → Account Integrity Check

print("\n" + "-" * 90)
print("TRANSACTION → ACCOUNT INTEGRITY")
print("-" * 90)

transaction_account_check = (
    transactions_df
    .join(
        accounts_df,
        transactions_df.account_id == accounts_df.account_id,
        "left"
    )
    .filter(accounts_df.account_id.isNull())
)

invalid_transaction_account_count = (
    transaction_account_check.count()
)

print_result(
    "Transactions referencing nonexistent accounts",
    invalid_transaction_account_count
)


# 9. Account → Customer Integrity Check

print("\n" + "-" * 90)
print("ACCOUNT → CUSTOMER INTEGRITY")
print("-" * 90)

account_customer_check = (
    accounts_df
    .join(
        customers_df,
        accounts_df.customer_id == customers_df.customer_id,
        "left"
    )
    .filter(customers_df.customer_id.isNull())
)

invalid_account_customer_count = (
    account_customer_check.count()
)

print_result(
    "Accounts referencing nonexistent customers",
    invalid_account_customer_count
)


# 10. Account → Branch Integrity Check

print("\n" + "-" * 90)
print("ACCOUNT → BRANCH INTEGRITY")
print("-" * 90)

account_branch_check = (
    accounts_df
    .join(
        branches_df,
        accounts_df.branch_id == branches_df.branch_id,
        "left"
    )
    .filter(branches_df.branch_id.isNull())
)

invalid_account_branch_count = (
    account_branch_check.count()
)

print_result(
    "Accounts referencing nonexistent branches",
    invalid_account_branch_count
)


# 11. Final Summary

all_checks = [
    null_transaction_id_count,
    null_account_id_count,
    null_amount_count,
    duplicate_transaction_id_count,
    invalid_transaction_account_count,
    invalid_account_customer_count,
    invalid_account_branch_count,
]

total_bad_records = sum(all_checks)

print("\n" + "=" * 90)
print("DATA QUALITY SUMMARY")
print("=" * 90)

if total_bad_records == 0:
    print("OVERALL RESULT: PASS")
    print("All data quality checks passed.")
else:
    print("OVERALL RESULT: FAIL")
    print(f"Total bad records found across checks: {total_bad_records}")



# 12. Stop Spark

spark.stop()