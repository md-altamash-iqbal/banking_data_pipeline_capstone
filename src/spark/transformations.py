
import sqlite3
from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    coalesce,
    try_to_date,
    when,
)


# --------------------------------------------------
# 1. CREATE SPARK SESSION
# --------------------------------------------------

def create_spark_session() -> SparkSession:
    """Create and return a SparkSession."""
    return (
        SparkSession.builder
        .appName("Week6-Banking-Transformations")
        .master("local[*]")
        .getOrCreate()
    )


# --------------------------------------------------
# 2. LOAD TRANSACTIONS FROM SQLITE
# --------------------------------------------------

def load_transactions(
    spark: SparkSession,
    database_path: Path,
) -> DataFrame:
    """Load transactions from banking.db into a Spark DataFrame."""

    connection = sqlite3.connect(str(database_path))

    try:
        transactions_pandas = pd.read_sql_query(
            "SELECT * FROM transactions",
            connection,
        )
    finally:
        connection.close()

    return spark.createDataFrame(transactions_pandas)


# --------------------------------------------------
# 3. NORMALIZE TRANSACTION DATES
# --------------------------------------------------

def normalize_transaction_dates(
    transactions_df: DataFrame,
) -> DataFrame:
    """Convert supported date formats into a date column."""

    return transactions_df.withColumn(
        "transaction_date",
        coalesce(
            try_to_date(col("transaction_date"), "yyyy-MM-dd"),
            try_to_date(col("transaction_date"), "M/d/yyyy"),
        ),
    )


# --------------------------------------------------
# 4. FILTER CREDIT TRANSACTIONS
# --------------------------------------------------

def filter_credit_transactions(
    transactions_df: DataFrame,
) -> DataFrame:
    """Return only CREDIT transactions."""

    return transactions_df.filter(
        col("transaction_type") == "CREDIT"
    )


# --------------------------------------------------
# 5. FILTER HIGH-VALUE TRANSACTIONS
# --------------------------------------------------

def filter_high_value_transactions(
    transactions_df: DataFrame,
    threshold: float = 500.0,
) -> DataFrame:
    """Return transactions whose amount meets the threshold."""

    return transactions_df.filter(
        col("amount") >= threshold
    )


# --------------------------------------------------
# 6. SELECT REPORTING COLUMNS
# --------------------------------------------------

def select_reporting_columns(
    transactions_df: DataFrame,
) -> DataFrame:
    """Select the columns required for reporting."""

    return transactions_df.select(
        "transaction_id",
        "account_id",
        "transaction_date",
        "transaction_type",
        "amount",
    )


# --------------------------------------------------
# 7. CLASSIFY TRANSACTIONS
# --------------------------------------------------

def classify_transactions(
    transactions_df: DataFrame,
) -> DataFrame:
    """Add a category based on the transaction amount."""

    return transactions_df.withColumn(
        "transaction_category",
        when(col("amount") >= 500, "HIGH_VALUE")
        .when(col("amount") >= 100, "MEDIUM_VALUE")
        .otherwise("LOW_VALUE"),
    )


# --------------------------------------------------
# 8. RUN ALL TRANSFORMATIONS
# --------------------------------------------------

def run_transformations(
    transactions_df: DataFrame,
) -> dict[str, DataFrame]:
    """Execute transformations and return their DataFrames."""

    normalized_df = normalize_transaction_dates(
        transactions_df
    )

    return {
        "credit": filter_credit_transactions(normalized_df),
        "high_value": filter_high_value_transactions(
            normalized_df
        ),
        "report": select_reporting_columns(normalized_df),
        "classified": classify_transactions(normalized_df),
    }


# --------------------------------------------------
# 9. MAIN EXECUTION
# --------------------------------------------------

def main() -> None:
    """Load data, run transformations, and display results."""

    base_dir = Path(__file__).resolve().parents[2]
    database_path = base_dir / "database" / "banking.db"

    spark = create_spark_session()

    try:
        transactions_df = load_transactions(
            spark,
            database_path,
        )

        results = run_transformations(transactions_df)

        print("\nCREDIT-ONLY TRANSACTIONS")
        results["credit"].show(truncate=False)
        print("CREDIT count:", results["credit"].count())

        print("\nHIGH-VALUE TRANSACTIONS")
        results["high_value"].show(truncate=False)
        print(
            "High-value count:",
            results["high_value"].count(),
        )

        print("\nSELECTED REPORTING COLUMNS")
        results["report"].show(truncate=False)

        print("\nDERIVED TRANSACTION CATEGORY")
        results["classified"].select(
            "transaction_id",
            "transaction_type",
            "amount",
            "transaction_category",
        ).show(truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
