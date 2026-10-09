"""
Week 6 - Banking Analytics with PySpark

Source:
    database/banking.db
    data/generated/synthetic_transactions.csv

Output:
    output/spark/parquet/branch_analysis/
"""

from pathlib import Path
import sqlite3

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


# --------------------------------------------------
# 1. PROJECT PATHS
# --------------------------------------------------

# Assumes this file is located in src/spark/
PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATABASE_PATH = PROJECT_ROOT / "database" / "banking.db"

GENERATED_DATA_PATH = (
    PROJECT_ROOT / "data" / "generated" / "synthetic_transactions.csv"
)

PARQUET_PATH = (
    PROJECT_ROOT / "output" / "spark" / "parquet" / "branch_analysis"
)


# --------------------------------------------------
# 2. REQUIRED SCHEMAS
# --------------------------------------------------

REQUIRED_SQLITE_TABLES = {
    "transactions": {
        "transaction_id",
        "account_id",
        "transaction_type",
        "amount",
    },
    "accounts": {
        "account_id",
        "customer_id",
        "branch_id",
        "account_type",
    },
    "customers": {
        "customer_id",
        "customer_name",
    },
    "branches": {
        "branch_id",
        "branch_name",
    },
}

REQUIRED_SYNTHETIC_COLUMNS = {
    "transaction_id",
    "account_id",
    "transaction_type",
    "amount",
    "transaction_date",
    "currency",
}


# --------------------------------------------------
# 3. HELPER FUNCTIONS
# --------------------------------------------------

def heading(title: str) -> None:
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


def require_file(path: Path, description: str) -> None:
    if not path.is_file():
        raise FileNotFoundError(
            f"{description} not found: {path}"
        )


def require_columns(
    dataframe: DataFrame,
    required_columns: set[str],
    name: str,
) -> None:
    missing = required_columns.difference(dataframe.columns)

    if missing:
        raise ValueError(
            f"{name} is missing required columns: "
            f"{', '.join(sorted(missing))}"
        )


def load_sqlite_table(
    spark: SparkSession,
    table_name: str,
) -> DataFrame:
    """Load an approved SQLite table into a Spark DataFrame."""

    if table_name not in REQUIRED_SQLITE_TABLES:
        raise ValueError(f"Unsupported table: {table_name}")

    with sqlite3.connect(DATABASE_PATH) as connection:
        pandas_df = pd.read_sql_query(
            f'SELECT * FROM "{table_name}"',
            connection,
        )

    if pandas_df.empty:
        raise ValueError(
            f"SQLite table '{table_name}' contains no rows."
        )

    dataframe = spark.createDataFrame(pandas_df)

    require_columns(
        dataframe,
        REQUIRED_SQLITE_TABLES[table_name],
        table_name,
    )

    return dataframe


def transaction_summary(
    dataframe: DataFrame,
    *group_columns: str,
) -> DataFrame:
    """Count transactions and calculate amount statistics."""

    return (
        dataframe
        .groupBy(*group_columns)
        .agg(
            F.count("transaction_id").alias("transaction_count"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("average_amount"),
        )
        .orderBy(*group_columns)
    )


# --------------------------------------------------
# 4. MAIN PROGRAM
# --------------------------------------------------

def main() -> None:

    require_file(DATABASE_PATH, "SQLite database")
    require_file(GENERATED_DATA_PATH, "Synthetic transaction CSV")

    PARQUET_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    spark = (
        SparkSession.builder
        .appName("Week6-Banking-Analytics")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:

        # ------------------------------------------
        # PART 1 - LOAD SQLITE TABLES
        # ------------------------------------------

        transactions = load_sqlite_table(
            spark, "transactions"
        )
        accounts = load_sqlite_table(
            spark, "accounts"
        )
        customers = load_sqlite_table(
            spark, "customers"
        )
        branches = load_sqlite_table(
            spark, "branches"
        )

        # Convert transaction amount to numeric.
        transactions = transactions.withColumn(
            "amount",
            F.col("amount").cast("double"),
        )

        if transactions.filter(
            F.col("amount").isNull()
            | (F.col("amount") <= 0)
        ).limit(1).count():
            raise ValueError(
                "SQLite transactions contain an invalid amount."
            )

        heading("PART 1 - DATASET COUNTS")

        for name, dataframe in {
            "Transactions": transactions,
            "Accounts": accounts,
            "Customers": customers,
            "Branches": branches,
        }.items():
            print(f"{name}: {dataframe.count()}")

        # ------------------------------------------
        # PART 2 - TRANSACTION TYPE ANALYSIS
        # ------------------------------------------

        heading("PART 2 - TRANSACTION TYPE ANALYSIS")

        transaction_summary(
            transactions,
            "transaction_type",
        ).show(truncate=False)

        # ------------------------------------------
        # PART 3 - ACCOUNT-WISE ANALYSIS
        # ------------------------------------------

        heading("PART 3 - ACCOUNT-WISE ANALYSIS")

        transaction_summary(
            transactions,
            "account_id",
        ).show(truncate=False)

        # ------------------------------------------
        # PART 4 - JOIN TRANSACTIONS AND ACCOUNTS
        # ------------------------------------------

        transaction_account = (
            transactions.alias("t")
            .join(
                accounts.alias("a"),
                F.col("t.account_id") == F.col("a.account_id"),
                "inner",
            )
            .select(
                F.col("t.transaction_id"),
                F.col("t.account_id"),
                F.col("t.transaction_type"),
                F.col("t.amount"),
                F.col("a.customer_id"),
                F.col("a.branch_id"),
                F.col("a.account_type"),
            )
        )

        heading("PART 4 - TRANSACTION + ACCOUNT JOIN")

        transaction_account.show(truncate=False)

        # ------------------------------------------
        # PART 5 - CUSTOMER-WISE ACTIVITY
        # ------------------------------------------

        customer_activity = (
            transaction_account.alias("ta")
            .join(
                customers.alias("c"),
                F.col("ta.customer_id")
                == F.col("c.customer_id"),
                "inner",
            )
            .select(
                "ta.transaction_id",
                "ta.account_id",
                "ta.customer_id",
                "c.customer_name",
                "ta.amount",
                "ta.transaction_type",
            )
        )

        heading("PART 5 - CUSTOMER-WISE ACTIVITY")

        transaction_summary(
            customer_activity,
            "customer_id",
            "customer_name",
        ).show(truncate=False)

        # ------------------------------------------
        # PART 6 - BRANCH-WISE ANALYSIS
        # ------------------------------------------

        transaction_branch = (
            transaction_account.alias("ta")
            .join(
                branches.alias("b"),
                F.col("ta.branch_id")
                == F.col("b.branch_id"),
                "inner",
            )
            .select(
                "ta.transaction_id",
                "ta.amount",
                "ta.branch_id",
                "b.branch_name",
            )
        )

        heading("PART 6 - BRANCH-WISE ANALYSIS")

        branch_analysis = transaction_summary(
            transaction_branch,
            "branch_id",
            "branch_name",
        )

        branch_analysis.show(truncate=False)

        heading("PART 6 - ACCOUNT-TYPE ACTIVITY")

        transaction_summary(
            transaction_account,
            "account_type",
        ).show(truncate=False)

        heading("PART 6 - MULTIPLE AGGREGATIONS")

        transactions.agg(
            F.count("transaction_id").alias(
                "total_transactions"
            ),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("average_amount"),
        ).show(truncate=False)

        # ------------------------------------------
        # PART 7 - ADDITIONAL DATA QUALITY CHECKS
        # ------------------------------------------

        heading("PART 7 - SQLITE DATA QUALITY")

        print(
            "Transactions with invalid amounts:",
            transactions.filter(
                F.col("amount").isNull()
                | (F.col("amount") <= 0)
            ).count(),
        )

        print(
            "Distinct account IDs:",
            accounts.select("account_id").distinct().count(),
        )

        print(
            "Total account rows:",
            accounts.count(),
        )

        # ------------------------------------------
        # PART 8 - LARGE DATA EXERCISE
        # ------------------------------------------

        heading("PART 8 - LOAD SYNTHETIC DATA")

        # Validate original columns BEFORE adding or casting columns.
        synthetic_raw = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(str(GENERATED_DATA_PATH))
        )

        require_columns(
            synthetic_raw,
            REQUIRED_SYNTHETIC_COLUMNS,
            "Synthetic transaction CSV",
        )

        synthetic = synthetic_raw.withColumn(
            "amount",
            F.col("amount").cast("double"),
        )

        # Validate required fields.
        missing_condition = F.lit(False)

        for column in REQUIRED_SYNTHETIC_COLUMNS:
            missing_condition = (
                missing_condition
                | F.col(column).isNull()
                | (
                    F.trim(F.col(column).cast("string"))
                    == ""
                )
            )

        if synthetic.filter(
            missing_condition
        ).limit(1).count():
            raise ValueError(
                "Synthetic CSV contains null or blank required fields."
            )

        if synthetic.filter(
            F.col("amount").isNull()
            | (F.col("amount") <= 0)
        ).limit(1).count():
            raise ValueError(
                "Synthetic CSV contains an invalid amount."
            )

        invalid_types = synthetic.filter(
            ~F.col("transaction_type").isin("CREDIT", "DEBIT")
        )

        if invalid_types.limit(1).count():
            raise ValueError(
                "Synthetic CSV contains an unsupported transaction type."
            )

        synthetic = synthetic.cache()

        synthetic_count = synthetic.count()

        print(
            f"Synthetic transaction count: {synthetic_count}"
        )

        synthetic.printSchema()
        synthetic.show(5, truncate=False)

        # Confirm unique transaction IDs.
        distinct_ids = (
            synthetic
            .select("transaction_id")
            .distinct()
            .count()
        )

        print(f"Distinct transaction IDs: {distinct_ids}")

        if distinct_ids != synthetic_count:
            raise ValueError(
                "Synthetic data contains duplicate transaction IDs."
            )

        # High-value filter.
        high_value = synthetic.filter(
            F.col("amount") >= F.lit(1000.0)
        )

        high_value_count = high_value.count()

        heading("PART 8 - HIGH-VALUE FILTER")

        print(
            f"Transactions with amount >= 1000: "
            f"{high_value_count}"
        )

        high_value.show(5, truncate=False)

        # Aggregation by transaction type.
        heading("PART 8 - TRANSACTION TYPE AGGREGATION")

        synthetic_type_analysis = transaction_summary(
            synthetic,
            "transaction_type",
        )

        synthetic_type_analysis.show(truncate=False)

        # Validate account keys before joining.
        account_count = accounts.count()
        distinct_account_count = (
            accounts.select("account_id").distinct().count()
        )

        if account_count != distinct_account_count:
            raise ValueError(
                "Accounts table contains duplicate account IDs."
            )

        # Find synthetic transactions with no matching account.
        unmatched_accounts = (
            synthetic
            .select("account_id")
            .distinct()
            .join(
                accounts.select("account_id").distinct(),
                on="account_id",
                how="left_anti",
            )
        )

        unmatched_account_count = unmatched_accounts.count()

        # Join synthetic transactions with accounts.
        synthetic_account = (
            synthetic.alias("s")
            .join(
                accounts.alias("a"),
                F.col("s.account_id")
                == F.col("a.account_id"),
                "inner",
            )
            .select(
                F.col("s.transaction_id"),
                F.col("s.account_id"),
                F.col("s.transaction_type"),
                F.col("s.amount"),
                F.col("s.transaction_date"),
                F.col("s.currency"),
                F.col("a.customer_id"),
                F.col("a.branch_id"),
                F.col("a.account_type"),
            )
            .cache()
        )

        joined_count = synthetic_account.count()

        heading("PART 8 - JOIN WITH ACCOUNTS")

        print(f"Joined transaction count: {joined_count}")
        print(f"Unmatched account IDs: {unmatched_account_count}")

        synthetic_account.show(5, truncate=False)

        # Validate branch keys.
        branch_count = branches.count()
        distinct_branch_count = (
            branches.select("branch_id").distinct().count()
        )

        if branch_count != distinct_branch_count:
            raise ValueError(
                "Branches table contains duplicate branch IDs."
            )

        unmatched_branches = (
            synthetic_account
            .select("branch_id")
            .distinct()
            .join(
                branches.select("branch_id").distinct(),
                on="branch_id",
                how="left_anti",
            )
        )

        unmatched_branch_count = unmatched_branches.count()

        # Join accounts to branches.
        synthetic_branch = (
            synthetic_account.alias("sa")
            .join(
                branches.alias("b"),
                F.col("sa.branch_id")
                == F.col("b.branch_id"),
                "inner",
            )
            .select(
                "sa.transaction_id",
                "sa.account_id",
                "sa.customer_id",
                "sa.branch_id",
                "b.branch_name",
                "sa.account_type",
                "sa.transaction_type",
                "sa.amount",
                "sa.currency",
                "sa.transaction_date",
            )
            .cache()
        )

        branch_joined_count = synthetic_branch.count()

        heading("PART 8 - BRANCH ANALYSIS")

        print(
            f"Branch-joined transaction count: "
            f"{branch_joined_count}"
        )
        print(f"Unmatched branch IDs: {unmatched_branch_count}")

        synthetic_branch_analysis = transaction_summary(
            synthetic_branch,
            "branch_id",
            "branch_name",
        )

        synthetic_branch_analysis.show(truncate=False)

        # Assignment validation checks.
        heading("PART 8 - VALIDATION RESULTS")

        checks = {
            "Expected 100,000 synthetic transactions":
                synthetic_count == 100_000,
            "High-value filter returned records":
                high_value_count > 0,
            "Transaction aggregation returned results":
                synthetic_type_analysis.limit(1).count() > 0,
            "Every synthetic transaction matched an account":
                unmatched_account_count == 0
                and joined_count == synthetic_count,
            "Every synthetic transaction matched a branch":
                unmatched_branch_count == 0
                and branch_joined_count == synthetic_count,
        }

        for description, passed in checks.items():
            print(
                f"{'PASS' if passed else 'FAIL'} | {description}"
            )

        # ------------------------------------------
        # PART 9 - SPARK EXECUTION CONCEPTS
        # ------------------------------------------

        heading("PART 9 - SPARK EXECUTION CONCEPTS")

        # A. Partitions
        partition_count = synthetic.rdd.getNumPartitions()

        print(
            "Synthetic DataFrame partitions:",
            partition_count,
        )

        # B. Transformation: filter
        credit_transactions = synthetic.filter(
            F.col("transaction_type") == "CREDIT"
        )

        print(
            "Created a filtered CREDIT DataFrame."
        )
        print(
            "Filter is a transformation; it is evaluated lazily."
        )

        # C. Action: count
        credit_count = credit_transactions.count()

        print(f"CREDIT transaction count: {credit_count}")
        print("count() is an action.")

        # D. Shuffle demonstration
        print(
            "groupBy('branch_id').count() may require a shuffle."
        )

        branch_grouped = (
            synthetic_account
            .groupBy("branch_id")
            .count()
        )

        # Inspect the physical execution plan.
        branch_grouped.explain("formatted")

        # show() triggers execution of this grouped result.
        branch_grouped.show(truncate=False)

        # ------------------------------------------
        # WRITE ANALYTICS RESULTS TO PARQUET
        # ------------------------------------------

        heading("PARQUET WRITE")

        expected_row_count = branch_analysis.count()

        (
            branch_analysis.write
            .mode("overwrite")
            .parquet(str(PARQUET_PATH))
        )

        print(f"Parquet output: {PARQUET_PATH}")

        # Read the output back to validate it.
        heading("PARQUET READ AND VALIDATION")

        parquet_data = spark.read.parquet(
            str(PARQUET_PATH)
        )

        parquet_row_count = parquet_data.count()

        print(f"Expected rows: {expected_row_count}")
        print(f"Rows read from Parquet: {parquet_row_count}")

        if parquet_row_count != expected_row_count:
            raise RuntimeError(
                "Parquet row count does not match "
                "the expected row count."
            )

        print("PASS | Parquet row count matches.")

        # Release cached DataFrames.
        synthetic.unpersist()
        synthetic_account.unpersist()
        synthetic_branch.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()