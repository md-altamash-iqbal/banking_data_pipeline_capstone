import sqlite3

from cdp.src.validate_data import validate_and_load_transactions
from cdp.src import build_customer360


def create_database(path):
    connection = sqlite3.connect(path)

    connection.executescript("""
        CREATE TABLE customers (
            customer_id TEXT PRIMARY KEY,
            customer_name TEXT NOT NULL
        );

        CREATE TABLE accounts (
            account_id TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL
        );

        CREATE TABLE transactions (
            transaction_id TEXT PRIMARY KEY,
            account_id TEXT NOT NULL,
            transaction_date TEXT NOT NULL,
            transaction_type TEXT NOT NULL,
            amount REAL NOT NULL,
            currency TEXT NOT NULL,
            branch_id TEXT NOT NULL
        );

        CREATE TABLE exception_records (
            exception_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_table TEXT NOT NULL,
            source_record_id TEXT,
            reason TEXT NOT NULL,
            raw_record_json TEXT,
            detected_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        INSERT INTO customers VALUES ('C001', 'Alex Morgan');
        INSERT INTO accounts VALUES ('A001', 'C001');
    """)

    connection.commit()
    return connection


def test_transaction_ingestion_is_idempotent(tmp_path):
    database_path = tmp_path / "rerun_test.db"
    connection = create_database(database_path)

    records = [{
        "transaction_id": "T001",
        "account_id": "A001",
        "transaction_date": "2026-09-01",
        "transaction_type": "CREDIT",
        "amount": 100.0,
        "currency": "USD",
        "branch_id": "BR001"
    }]

    # First ingestion
    first_result = validate_and_load_transactions(
        connection,
        records
    )

    connection.commit()

    # Second ingestion of the same records
    second_result = validate_and_load_transactions(
        connection,
        records
    )

    connection.commit()

    total = connection.execute(
        "SELECT COUNT(*) FROM transactions"
    ).fetchone()[0]

    assert first_result["loaded"] == 1
    assert second_result["loaded"] == 0
    assert second_result["skipped"] == 1
    assert total == 1

    connection.close()


def test_customer360_rebuild_produces_identical_results(
    tmp_path,
    monkeypatch
):
    database_path = tmp_path / "gold_rerun_test.db"
    connection = create_database(database_path)

    connection.execute("""
        INSERT INTO transactions VALUES (
            'T001', 'A001', '2026-09-01',
            'CREDIT', 100.0, 'USD', 'BR001'
        )
    """)

    connection.commit()
    connection.close()

    monkeypatch.setattr(
        build_customer360,
        "DATABASE_PATH",
        database_path
    )

    # First Gold build
    build_customer360.build_customer360()

    connection = sqlite3.connect(database_path)

    first_result = connection.execute("""
        SELECT *
        FROM customer_360
        ORDER BY customer_id
    """).fetchall()

    connection.close()

    # Second Gold build
    build_customer360.build_customer360()

    connection = sqlite3.connect(database_path)

    second_result = connection.execute("""
        SELECT *
        FROM customer_360
        ORDER BY customer_id
    """).fetchall()

    connection.close()

    assert first_result == second_result
    assert len(second_result) == 1
    assert second_result[0][3] == 1
    assert second_result[0][4] == 100.0