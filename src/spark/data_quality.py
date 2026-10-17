
import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.spark.load_data import load_sqlite_table


BASE_DIR = Path(__file__).resolve().parents[2]
DATABASE_PATH = BASE_DIR / "database" / "banking.db"

ALLOWED_TABLES = {
    "transactions",
    "accounts",
    "customers",
    "branches",
}


def load_table(
    table_name: str,
    spark: SparkSession,
    database_path: Path = DATABASE_PATH,
) -> DataFrame:
    """Load an approved SQLite table as a Spark DataFrame."""
    if table_name not in ALLOWED_TABLES:
        raise ValueError(f"Invalid table name: {table_name}")

    return load_sqlite_table(
        table_name=table_name,
        spark=spark,
        database_path=database_path,
    )


def _count_rows(dataframe: DataFrame) -> int:
    """Return the number of rows matching a DataFrame condition."""
    return dataframe.count()


def validate_banking_data(
    transactions_df: DataFrame,
    accounts_df: DataFrame,
    customers_df: DataFrame,
    branches_df: DataFrame,
) -> dict:
    """
    Run the seven mandatory banking data-quality checks.

    Returns structured check results when all checks pass.
    Raises ValueError when any mandatory check fails.
    """

    required_columns = {
        "transactions": (
            transactions_df,
            {"transaction_id", "account_id", "amount"},
        ),
        "accounts": (
            accounts_df,
            {"account_id", "customer_id", "branch_id"},
        ),
        "customers": (
            customers_df,
            {"customer_id"},
        ),
        "branches": (
            branches_df,
            {"branch_id"},
        ),
    }

    # Fail clearly if a required column is missing.
    for table_name, (dataframe, expected_columns) in required_columns.items():
        missing_columns = expected_columns - set(dataframe.columns)

        if missing_columns:
            raise ValueError(
                f"{table_name} is missing required columns: "
                f"{sorted(missing_columns)}"
            )

    # Check 1: Null transaction IDs.
    null_transaction_id_count = _count_rows(
        transactions_df.filter(F.col("transaction_id").isNull())
    )

    # Check 2: Null account IDs in transactions.
    null_account_id_count = _count_rows(
        transactions_df.filter(F.col("account_id").isNull())
    )

    # Check 3: Null transaction amounts.
    null_amount_count = _count_rows(
        transactions_df.filter(F.col("amount").isNull())
    )

    # Check 4: Duplicate transaction IDs.
    duplicate_transaction_id_count = _count_rows(
        transactions_df
        .groupBy("transaction_id")
        .count()
        .filter(F.col("count") > 1)
    )

    # Check 5: Transactions referencing nonexistent accounts.
    invalid_transaction_account_count = _count_rows(
        transactions_df.alias("t")
        .join(
            accounts_df.select("account_id").alias("a"),
            F.col("t.account_id") == F.col("a.account_id"),
            "left_anti",
        )
    )

    # Check 6: Accounts referencing nonexistent customers.
    invalid_account_customer_count = _count_rows(
        accounts_df.alias("a")
        .join(
            customers_df.select("customer_id").alias("c"),
            F.col("a.customer_id") == F.col("c.customer_id"),
            "left_anti",
        )
    )

    # Check 7: Accounts referencing nonexistent branches.
    invalid_account_branch_count = _count_rows(
        accounts_df.alias("a")
        .join(
            branches_df.select("branch_id").alias("b"),
            F.col("a.branch_id") == F.col("b.branch_id"),
            "left_anti",
        )
    )

    checks = {
        "null_transaction_ids": null_transaction_id_count,
        "null_account_ids": null_account_id_count,
        "null_amounts": null_amount_count,
        "duplicate_transaction_ids": duplicate_transaction_id_count,
        "transactions_with_missing_accounts": (
            invalid_transaction_account_count
        ),
        "accounts_with_missing_customers": (
            invalid_account_customer_count
        ),
        "accounts_with_missing_branches": (
            invalid_account_branch_count
        ),
    }

    print("\n" + "=" * 85)
    print("BANKING DATA QUALITY REPORT")
    print("=" * 85)

    for check_name, bad_count in checks.items():
        status = "PASS" if bad_count == 0 else "FAIL"
        print(
            f"{status:<6} | {check_name:<45} | "
            f"Bad records/groups: {bad_count}"
        )

    failed_checks = {
        name: count
        for name, count in checks.items()
        if count > 0
    }

    if failed_checks:
        raise ValueError(
            "Banking data-quality validation failed: "
            f"{failed_checks}"
        )

    print("\nOVERALL RESULT: PASS")

    return {
        "status": "PASS",
        "checks": checks,
        "total_failed_checks": 0,
    }


def main() -> None:
    """Run data-quality validation as a standalone script."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--database",
        type=Path,
        default=DATABASE_PATH,
        help="Path to the SQLite banking database.",
    )
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("Week7-Banking-Data-Quality")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        transactions_df = load_table(
            "transactions", spark, args.database
        )
        accounts_df = load_table(
            "accounts", spark, args.database
        )
        customers_df = load_table(
            "customers", spark, args.database
        )
        branches_df = load_table(
            "branches", spark, args.database
        )

        result = validate_banking_data(
            transactions_df,
            accounts_df,
            customers_df,
            branches_df,
        )

        print(f"\nValidation status: {result['status']}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
