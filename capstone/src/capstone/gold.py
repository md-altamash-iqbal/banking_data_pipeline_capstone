
"""Build Gold summaries using PySpark and verify them against SQLite."""

import csv
import os
import sqlite3
import tempfile
from decimal import Decimal
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATABASE_PATH = PROJECT_ROOT / "database" / "capstone.db"
GOLD_DIR = PROJECT_ROOT / "output" / "gold"
GOLD_CSV = GOLD_DIR / "gold_summary.csv"

GROUP_COLUMNS = [
    "branch_id",
    "transaction_date",
    "transaction_type",
]


def validate_silver_snapshot(connection):
    """Block Gold publication unless Silver and Exceptions reconcile."""
    audit = connection.execute(
        """
        SELECT status, source_rows, accepted_rows, rejected_rows
        FROM silver_run_audit
        ORDER BY started_at DESC
        LIMIT 1
        """
    ).fetchone()

    if audit is None:
        raise RuntimeError("No Silver validation run exists.")

    status, source_rows, accepted_rows, rejected_rows = audit

    if status != "SUCCESS":
        raise RuntimeError(
            f"Latest Silver run is {status}; Gold publication blocked."
        )

    if source_rows != accepted_rows + rejected_rows:
        raise RuntimeError("Silver audit reconciliation failed.")

    actual_silver = connection.execute(
        "SELECT COUNT(*) FROM silver_records"
    ).fetchone()[0]

    actual_exceptions = connection.execute(
        "SELECT COUNT(*) FROM exceptions_records"
    ).fetchone()[0]

    if actual_silver != accepted_rows:
        raise RuntimeError(
            f"Silver count mismatch: audit={accepted_rows}, "
            f"table={actual_silver}"
        )

    if actual_exceptions != rejected_rows:
        raise RuntimeError(
            f"Exceptions count mismatch: audit={rejected_rows}, "
            f"table={actual_exceptions}"
        )


def normalize_result(rows):
    """Normalize SQL and Spark results for comparison."""
    normalized = []

    for row in rows:
        branch, business_date, transaction_type, count, amount = row

        normalized.append(
            (
                branch,
                business_date,
                transaction_type,
                int(count),
                Decimal(str(amount)).quantize(Decimal("0.01")),
            )
        )

    return sorted(normalized)


def build_gold():
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    # Read Silver and calculate an independent SQL result.
    with sqlite3.connect(DATABASE_PATH) as connection:
        validate_silver_snapshot(connection)

        silver_rows = connection.execute(
            """
            SELECT
                branch_id,
                transaction_date,
                transaction_type,
                amount
            FROM silver_records
            ORDER BY branch_id, transaction_date, transaction_type
            """
        ).fetchall()

        sql_rows = connection.execute(
            """
            SELECT
                branch_id,
                transaction_date,
                transaction_type,
                COUNT(*) AS transaction_count,
                ROUND(SUM(amount), 2) AS total_amount
            FROM silver_records
            GROUP BY branch_id, transaction_date, transaction_type
            ORDER BY branch_id, transaction_date, transaction_type
            """
        ).fetchall()

    if not silver_rows:
        raise RuntimeError(
            "Silver contains no accepted records; Gold publication blocked."
        )

    spark = (
        SparkSession.builder
        .appName("Week8-Banking-Gold")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        schema = StructType(
            [
                StructField("branch_id", StringType(), False),
                StructField("transaction_date", StringType(), False),
                StructField("transaction_type", StringType(), False),
                StructField("amount", DoubleType(), False),
            ]
        )

        spark_df = spark.createDataFrame(silver_rows, schema=schema)

        # PySpark Gold aggregation.
        spark_gold = (
            spark_df
            .groupBy(*GROUP_COLUMNS)
            .agg(
                F.count("*").alias("transaction_count"),
                F.round(F.sum("amount"), 2).alias("total_amount"),
            )
            .orderBy(*GROUP_COLUMNS)
        )

        spark_rows = [
            (
                row["branch_id"],
                row["transaction_date"],
                row["transaction_type"],
                row["transaction_count"],
                row["total_amount"],
            )
            for row in spark_gold.collect()
        ]

        # Never publish if the independent SQL result differs.
        if normalize_result(spark_rows) != normalize_result(sql_rows):
            raise RuntimeError(
                "PySpark and SQLite Gold results do not match. "
                "Existing Gold output was not replaced."
            )

        # Stage the CSV using Python, not Spark's Hadoop CSV writer.
        # This avoids the Windows winutils.exe/HADOOP_HOME dependency.
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            suffix=".csv",
            prefix="gold_summary_",
            dir=GOLD_DIR,
            delete=False,
        ) as output_file:
            temporary_csv = Path(output_file.name)

            try:
                writer = csv.writer(output_file)
                writer.writerow(
                    [
                        "branch_id",
                        "transaction_date",
                        "transaction_type",
                        "transaction_count",
                        "total_amount",
                    ]
                )
                writer.writerows(spark_rows)

            except Exception:
                output_file.close()
                temporary_csv.unlink(missing_ok=True)
                raise

        # Publish SQLite Gold in one transaction, after all checks pass.
        try:
            with sqlite3.connect(DATABASE_PATH) as connection:
                validate_silver_snapshot(connection)

                with connection:
                    connection.execute(
                        """
                        CREATE TABLE IF NOT EXISTS gold_summary (
                            branch_id TEXT NOT NULL,
                            transaction_date TEXT NOT NULL,
                            transaction_type TEXT NOT NULL,
                            transaction_count INTEGER NOT NULL,
                            total_amount REAL NOT NULL,
                            PRIMARY KEY (
                                branch_id,
                                transaction_date,
                                transaction_type
                            )
                        )
                        """
                    )

                    connection.execute("DELETE FROM gold_summary")

                    connection.executemany(
                        """
                        INSERT INTO gold_summary (
                            branch_id,
                            transaction_date,
                            transaction_type,
                            transaction_count,
                            total_amount
                        )
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        spark_rows,
                    )

            # Replace the published CSV only after the new file is ready.
            os.replace(temporary_csv, GOLD_CSV)

        finally:
            # If publication fails before replacement, clean up the staged file.
            if temporary_csv.exists():
                temporary_csv.unlink()

        print(f"Accepted Silver rows: {len(silver_rows)}")
        print(f"Gold summary groups: {len(spark_rows)}")
        print("PySpark vs SQLite: MATCH")
        print(f"Gold SQLite table: {DATABASE_PATH}")
        print(f"Gold CSV export: {GOLD_CSV}")
        print("Gold publication: SUCCESS")

    finally:
        spark.stop()


if __name__ == "__main__":
    build_gold()
