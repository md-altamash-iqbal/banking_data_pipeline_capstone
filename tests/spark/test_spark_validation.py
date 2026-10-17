
import pytest

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)


# --------------------------------------------------
# SPARK TEST FIXTURE
# --------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .appName("Week6-Temporary-Validation-Tests")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()


# --------------------------------------------------
# TEMPORARY TRANSACTION DATA
# --------------------------------------------------

@pytest.fixture
def sample_transactions(spark):
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("amount", DoubleType(), True),
    ])

    data = [
        ("T001", "A001", "CREDIT", 100.0),
        ("T002", "A002", "DEBIT", 50.0),
        ("T003", "A001", "CREDIT", 200.0),
        ("T004", "A003", "CREDIT", 150.0),
        ("T005", "A004", "DEBIT", 25.0),
    ]

    return spark.createDataFrame(data, schema)


# --------------------------------------------------
# TEST 1 - EXPECTED INPUT ROW COUNT
# --------------------------------------------------

def test_expected_input_row_count(sample_transactions):
    assert sample_transactions.count() == 5


# --------------------------------------------------
# TEST 2 - KNOWN CREDIT COUNT
# --------------------------------------------------


def test_credit_transaction_count(sample_transactions):
    credit_count = (
        sample_transactions
        .filter(F.col("transaction_type") == "CREDIT")
        .count()
    )

    assert credit_count == 3


# --------------------------------------------------
# TEST 3 - KNOWN AGGREGATION RESULT
# --------------------------------------------------

def test_credit_total_amount(sample_transactions):
    result = (
        sample_transactions
        .filter(F.col("transaction_type") == "CREDIT")
        .agg(F.sum("amount").alias("total_amount"))
        .first()
    )

    assert result["total_amount"] == pytest.approx(450.0)


# --------------------------------------------------
# TEST 4 - DUPLICATE TRANSACTION ID
# --------------------------------------------------

def test_duplicate_transaction_id(spark):
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("amount", DoubleType(), True),
    ])

    temporary_data = [
        ("T001", "A001", "CREDIT", 100.0),
        ("T001", "A001", "CREDIT", 100.0),
        ("T002", "A002", "DEBIT", 50.0),
    ]

    df = spark.createDataFrame(temporary_data, schema)

    duplicate_ids = (
        df.groupBy("transaction_id")
        .count()
        .filter(
            (F.col("transaction_id").isNotNull())
            & (F.col("count") > 1)
        )
        .select("transaction_id")
        .collect()
    )

    assert [row["transaction_id"] for row in duplicate_ids] == ["T001"]


# --------------------------------------------------
# TEST 5 - NULL VALUE CHECK
# --------------------------------------------------

def test_null_amount_check(spark):
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("amount", DoubleType(), True),
    ])

    temporary_data = [
        ("T001", "A001", "CREDIT", 100.0),
        ("T002", "A002", "DEBIT", None),
        ("T003", "A003", "CREDIT", 200.0),
    ]

    df = spark.createDataFrame(temporary_data, schema)

    null_amount_count = (
        df.filter(F.col("amount").isNull()).count()
    )

    assert null_amount_count == 1


# --------------------------------------------------
# TEST 6 - JOIN INTEGRITY
# --------------------------------------------------

def test_join_integrity(spark):
    transaction_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("amount", DoubleType(), True),
    ])

    account_schema = StructType([
        StructField("account_id", StringType(), True),
        StructField("customer_id", StringType(), True),
    ])

    transactions = spark.createDataFrame(
        [
            ("T001", "A001", 100.0),
            ("T002", "A999", 200.0),
        ],
        transaction_schema,
    )

    accounts = spark.createDataFrame(
        [
            ("A001", "C001"),
            ("A002", "C002"),
        ],
        account_schema,
    )

    unmatched_count = (
        transactions
        .join(accounts, on="account_id", how="left_anti")
        .count()
    )

    assert unmatched_count == 1
