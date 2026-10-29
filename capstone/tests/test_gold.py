
import sqlite3
from decimal import Decimal

import pytest

from src.capstone.gold import (
    normalize_result,
    validate_silver_snapshot,
)


@pytest.fixture
def gold_test_db():
    """Create an isolated SQLite database for Gold gate tests."""
    connection = sqlite3.connect(":memory:")

    connection.executescript(
        """
        CREATE TABLE silver_run_audit (
            run_id TEXT PRIMARY KEY,
            started_at TEXT NOT NULL,
            completed_at TEXT NOT NULL,
            status TEXT NOT NULL,
            source_rows INTEGER NOT NULL,
            accepted_rows INTEGER NOT NULL,
            rejected_rows INTEGER NOT NULL,
            error_message TEXT
        );

        CREATE TABLE silver_records (
            transaction_id TEXT PRIMARY KEY,
            amount REAL NOT NULL
        );

        CREATE TABLE exceptions_records (
            exception_id INTEGER PRIMARY KEY,
            rejection_reasons_json TEXT NOT NULL
        );
        """
    )

    yield connection
    connection.close()


def insert_audit(
    connection,
    status="SUCCESS",
    source_rows=3,
    accepted_rows=2,
    rejected_rows=1,
):
    connection.execute(
        """
        INSERT INTO silver_run_audit (
            run_id, started_at, completed_at, status,
            source_rows, accepted_rows, rejected_rows, error_message
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "test-run",
            "2026-09-06T10:00:00+00:00",
            "2026-09-06T10:01:00+00:00",
            status,
            source_rows,
            accepted_rows,
            rejected_rows,
            None,
        ),
    )


def test_valid_silver_snapshot_passes(gold_test_db):
    insert_audit(gold_test_db)

    gold_test_db.executemany(
        "INSERT INTO silver_records VALUES (?, ?)",
        [("TXN001", 100.0), ("TXN002", 200.0)],
    )
    gold_test_db.execute(
        "INSERT INTO exceptions_records VALUES (?, ?)",
        (1, '["UNKNOWN_ACCOUNT_ID"]'),
    )

    validate_silver_snapshot(gold_test_db)


def test_failed_silver_run_blocks_gold(gold_test_db):
    insert_audit(
        gold_test_db,
        status="FAILED",
        source_rows=3,
        accepted_rows=2,
        rejected_rows=1,
    )

    with pytest.raises(RuntimeError, match="Gold publication blocked"):
        validate_silver_snapshot(gold_test_db)


def test_missing_silver_audit_blocks_gold(gold_test_db):
    with pytest.raises(
        RuntimeError, match="No Silver validation run exists"
    ):
        validate_silver_snapshot(gold_test_db)


def test_incorrect_reconciliation_blocks_gold(gold_test_db):
    insert_audit(
        gold_test_db,
        status="SUCCESS",
        source_rows=5,
        accepted_rows=2,
        rejected_rows=1,
    )

    with pytest.raises(
        RuntimeError, match="Silver audit reconciliation failed"
    ):
        validate_silver_snapshot(gold_test_db)


def test_silver_table_count_mismatch_blocks_gold(gold_test_db):
    insert_audit(gold_test_db)

    # Audit says two accepted rows, but only one is present.
    gold_test_db.execute(
        "INSERT INTO silver_records VALUES (?, ?)",
        ("TXN001", 100.0),
    )
    gold_test_db.execute(
        "INSERT INTO exceptions_records VALUES (?, ?)",
        (1, '["UNKNOWN_ACCOUNT_ID"]'),
    )

    with pytest.raises(RuntimeError, match="Silver count mismatch"):
        validate_silver_snapshot(gold_test_db)


def test_exceptions_table_count_mismatch_blocks_gold(gold_test_db):
    insert_audit(gold_test_db)

    gold_test_db.executemany(
        "INSERT INTO silver_records VALUES (?, ?)",
        [("TXN001", 100.0), ("TXN002", 200.0)],
    )

    # Audit says one exception, but the exception table is empty.
    with pytest.raises(RuntimeError, match="Exceptions count mismatch"):
        validate_silver_snapshot(gold_test_db)


def test_normalize_result_handles_numeric_types():
    rows = [
        ("BR002", "2026-09-06", "DEBIT", 2, 60.0),
        ("BR001", "2026-09-06", "CREDIT", 1, 500),
    ]

    result = normalize_result(rows)

    assert result == [
        ("BR001", "2026-09-06", "CREDIT", 1, Decimal("500.00")),
        ("BR002", "2026-09-06", "DEBIT", 2, Decimal("60.00")),
    ]


def test_normalize_result_rounds_amounts_to_two_decimals():
    rows = [
        ("BR001", "2026-09-06", "CREDIT", 1, 125.456)
    ]

    result = normalize_result(rows)

    assert result[0][4] == Decimal("125.46")
