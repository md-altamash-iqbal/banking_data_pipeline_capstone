
from pyspark.sql import SparkSession, functions as F
from pathlib import Path
import sqlite3


# --------------------------------------------------
# PROJECT PATHS
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATABASE_PATH = PROJECT_ROOT / "database" / "banking.db"
GENERATED_DATA_PATH = (
    PROJECT_ROOT / "data" / "generated" / "synthetic_transactions.csv"
)


def heading(title):
    print("\n" + "=" * 65)
    print(title)
    print("=" * 65)


def main():
    # --------------------------------------------------
    # CREATE SPARK SESSION
    # --------------------------------------------------

    spark = (
        SparkSession.builder
        .appName("Week6-Spark-SQL-Comparison")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        heading("SPARK SQL COMPARISON")

        # --------------------------------------------------
        # LOAD SYNTHETIC TRANSACTIONS
        # --------------------------------------------------

        if not GENERATED_DATA_PATH.exists():
            raise FileNotFoundError(
                f"Synthetic data not found: {GENERATED_DATA_PATH}\n"
                "Run generate_transactions.py first."
            )

        synthetic = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(str(GENERATED_DATA_PATH))
        )

        print("Synthetic transaction count:", synthetic.count())

        # --------------------------------------------------
        # REGISTER TEMPORARY VIEW
        # --------------------------------------------------

        synthetic.createOrReplaceTempView("synthetic_transactions")

        print("Temporary view registered: synthetic_transactions")

        # --------------------------------------------------
        # PART 10A - PYSPARK DATAFRAME API
        # --------------------------------------------------

        heading("PYSPARK DATAFRAME API")

        dataframe_result = (
            synthetic
            .groupBy("transaction_type")
            .agg(
                F.count("*").alias("transaction_count"),
                F.sum("amount").alias("total_amount"),
                F.avg("amount").alias("average_amount"),
            )
            .orderBy("transaction_type")
        )

        dataframe_result.show(truncate=False)

        # --------------------------------------------------
        # PART 10B - SPARK SQL
        # --------------------------------------------------

        heading("SPARK SQL")

        sql_result = spark.sql("""
            SELECT
                transaction_type,
                COUNT(*) AS transaction_count,
                SUM(amount) AS total_amount,
                AVG(amount) AS average_amount
            FROM synthetic_transactions
            GROUP BY transaction_type
            ORDER BY transaction_type
        """)

        sql_result.show(truncate=False)

        # --------------------------------------------------
        # PART 10C - VALIDATE BOTH RESULTS
        # --------------------------------------------------

        heading("VALIDATION")

        dataframe_rows = dataframe_result.collect()
        sql_rows = sql_result.collect()

        if dataframe_rows == sql_rows:
            print("PASS | PySpark DataFrame and Spark SQL results match.")
        else:
            raise AssertionError(
                "FAIL | PySpark DataFrame and Spark SQL results differ."
            )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
