
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from dagster import (
    DefaultScheduleStatus,
    Definitions,
    RetryPolicy,
    job,
    op,
    schedule,
)
from pyspark.sql import SparkSession

from src.spark.analytics import transaction_summary
from src.spark.data_quality import validate_banking_data
from src.spark.load_data import load_sqlite_table
from src.spark.transformations import run_transformations


PROJECT_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_DATABASE = PROJECT_ROOT / "database" / "banking.db"

REPORT_PATH = (
    PROJECT_ROOT / "output" / "week7" / "banking_report.json"
)


def get_database_path() -> Path:
    """Resolve the source SQLite database path."""
    return Path(
        os.environ.get(
            "BANKING_DB_PATH",
            str(DEFAULT_DATABASE),
        )
    ).resolve()


def create_spark_session(app_name: str) -> SparkSession:
    """Create a local Spark session."""
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .getOrCreate()
    )


# --------------------------------------------------
# STEP 1: EXTRACT
# --------------------------------------------------

@op(retry_policy=RetryPolicy(max_retries=2, delay=2))
def extract_banking_data(context) -> str:
    """Check source database and required tables."""
    database_path = get_database_path()

    context.log.info(
        f"Starting SQLite extraction: {database_path}"
    )

    if not database_path.is_file():
        raise FileNotFoundError(
            f"Banking database not found: {database_path}"
        )

    connection = sqlite3.connect(
        f"{database_path.as_uri()}?mode=ro",
        uri=True,
    )

    try:
        required_tables = (
            "transactions",
            "accounts",
            "customers",
            "branches",
        )

        for table_name in required_tables:
            exists = connection.execute(
                """
                SELECT COUNT(*)
                FROM sqlite_master
                WHERE type = 'table' AND name = ?
                """,
                (table_name,),
            ).fetchone()[0]

            if exists != 1:
                raise ValueError(
                    f"Required table missing: {table_name}"
                )

            row_count = connection.execute(
                f'SELECT COUNT(*) FROM "{table_name}"'
            ).fetchone()[0]

            context.log.info(
                f"Source table checked: {table_name}, "
                f"rows={row_count}"
            )

    finally:
        connection.close()

    context.log.info("SQLite extraction checks completed")

    return str(database_path)


# --------------------------------------------------
# STEP 2: VALIDATE
# --------------------------------------------------

@op
def validate_banking_data_op(
    context,
    database_path: str,
) -> str:
    """Run the existing banking data quality checks."""
    context.log.info("Starting banking data validation")

    spark = create_spark_session(
        "Week7-Banking-Validation"
    )

    try:
        tables = {
            table_name: load_sqlite_table(
                table_name,
                spark,
                Path(database_path),
            )
            for table_name in (
                "transactions",
                "accounts",
                "customers",
                "branches",
            )
        }

        result = validate_banking_data(
            tables["transactions"],
            tables["accounts"],
            tables["customers"],
            tables["branches"],
        )

        context.log.info(
            f"Validation completed: {result['status']}"
        )

        return database_path

    finally:
        spark.stop()


# --------------------------------------------------
# STEP 3: TRANSFORM
# --------------------------------------------------

@op
def transform_banking_data(
    context,
    database_path: str,
) -> dict:
    """Transform transactions and prepare report summary."""
    context.log.info("Starting PySpark transformations")

    spark = create_spark_session(
        "Week7-Banking-Transformations"
    )

    try:
        transactions_df = load_sqlite_table(
            "transactions",
            spark,
            Path(database_path),
        )

        results = run_transformations(transactions_df)

        credit_count = results["credit"].count()
        high_value_count = results["high_value"].count()
        reporting_count = results["report"].count()

        context.log.info(
            f"CREDIT transactions: {credit_count}"
        )
        context.log.info(
            f"High-value transactions: {high_value_count}"
        )
        context.log.info(
            f"Reporting rows: {reporting_count}"
        )

        summary_df = transaction_summary(
            transactions_df,
            "transaction_type",
        )

        summary = [
            {
                "transaction_type": row["transaction_type"],
                "transaction_count": row["transaction_count"],
                "total_amount": row["total_amount"],
                "average_amount": row["average_amount"],
            }
            for row in summary_df.collect()
        ]

        return {
            "database_path": str(database_path),
            "summary": summary,
        }

    finally:
        spark.stop()


# --------------------------------------------------
# STEP 4: PUBLISH
# --------------------------------------------------

@op
def publish_banking_report(
    context,
    transform_result: dict,
) -> str:
    """Write the report atomically to a JSON file."""
    context.log.info("Publishing banking report")

    report_path = Path(
        os.environ.get(
            "BANKING_REPORT_PATH",
            str(REPORT_PATH),
        )
    ).resolve()

    report = {
        "generated_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "source_database": transform_result["database_path"],
        "summary": transform_result["summary"],
    }

    report_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp_path = report_path.with_name(
        f".{report_path.name}.tmp"
    )

    try:
        temp_path.write_text(
            json.dumps(report, indent=2),
            encoding="utf-8",
        )
        os.replace(temp_path, report_path)

    finally:
        temp_path.unlink(missing_ok=True)

    context.log.info(
        f"Report published successfully: {report_path}"
    )

    return str(report_path)


# --------------------------------------------------
# DAGSTER JOB
# --------------------------------------------------

@job
def banking_pipeline():
    source = extract_banking_data()
    validated = validate_banking_data_op(source)
    transformed = transform_banking_data(validated)
    publish_banking_report(transformed)


# --------------------------------------------------
# DAILY SCHEDULE (DISABLED BY DEFAULT)
# --------------------------------------------------

@schedule(
    job=banking_pipeline,
    cron_schedule="0 6 * * *",
    default_status=DefaultScheduleStatus.STOPPED,
)
def daily_banking_schedule(_context):
    return {}


# --------------------------------------------------
# DAGSTER DEFINITIONS
# --------------------------------------------------

defs = Definitions(
    jobs=[banking_pipeline],
    schedules=[daily_banking_schedule],
)
