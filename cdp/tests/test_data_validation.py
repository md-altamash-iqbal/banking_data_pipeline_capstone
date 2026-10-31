import sqlite3

from cdp.src.validate_data import validate_and_load_transactions


def create_test_database(path):
    connection = sqlite3.connect(path)

    connection.executescript("""
        CREATE TABLE accounts (
            account_id TEXT PRIMARY KEY
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

        INSERT INTO accounts VALUES ('A001');
    """)

    connection.commit()
    return connection


def sample_transaction(transaction_id="T001", account_id="A001"):
    return {
        "transaction_id": transaction_id,
        "account_id": account_id,
        "transaction_date": "2026-09-01",
        "transaction_type": "CREDIT",
        "amount": 100.0,
        "currency": "USD",
        "branch_id": "BR001"
    }


def test_duplicate_transaction_is_rejected(tmp_path):
    connection = create_test_database(tmp_path / "test.db")

    records = [
        sample_transaction("T001"),
        sample_transaction("T001")
    ]

    result = validate_and_load_transactions(connection, records)

    assert result["loaded"] == 1
    assert result["rejected"] == 1

    transaction_count = connection.execute(
        "SELECT COUNT(*) FROM transactions"
    ).fetchone()[0]

    exception_count = connection.execute(
        "SELECT COUNT(*) FROM exception_records"
    ).fetchone()[0]

    assert transaction_count == 1
    assert exception_count == 1

    connection.close()


def test_unknown_account_is_rejected(tmp_path):
    connection = create_test_database(tmp_path / "test.db")

    records = [
        sample_transaction("T002", "UNKNOWN_ACCOUNT")
    ]

    result = validate_and_load_transactions(connection, records)

    assert result["loaded"] == 0
    assert result["rejected"] == 1

    transaction_count = connection.execute(
        "SELECT COUNT(*) FROM transactions"
    ).fetchone()[0]

    exception = connection.execute(
        """
        SELECT reason
        FROM exception_records
        """
    ).fetchone()

    assert transaction_count == 0
    assert exception[0] == "Unknown account_id: UNKNOWN_ACCOUNT"

    connection.close()


def test_missing_required_field_is_rejected(tmp_path):
    connection = create_test_database(tmp_path / "test.db")

    record = sample_transaction("T003")
    record["transaction_date"] = ""

    result = validate_and_load_transactions(connection, [record])

    assert result["loaded"] == 0
    assert result["rejected"] == 1

    connection.close()


def test_unknown_customer_account_is_rejected(tmp_path, monkeypatch):
    import csv

    from cdp.src import load_reference_data

    # Create an isolated test database.
    database_path = tmp_path / "test_reference.db"
    connection = sqlite3.connect(database_path)
    connection.execute("PRAGMA foreign_keys = ON")

    load_reference_data.initialize_database(connection)

    # Insert one valid customer.
    connection.execute(
        """
        INSERT INTO customers (customer_id, customer_name)
        VALUES (?, ?)
        """,
        ("C1001", "Test Customer")
    )

    # Create an account referencing a nonexistent customer.
    accounts_file = tmp_path / "accounts.csv"

    with accounts_file.open(
        "w",
        newline="",
        encoding="utf-8"
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "account_id",
                "customer_id",
                "branch_id",
                "account_type",
                "account_status"
            ]
        )
        writer.writeheader()
        writer.writerow({
            "account_id": "A9999",
            "customer_id": "C999",
            "branch_id": "BR001",
            "account_type": "CHECKING",
            "account_status": "ACTIVE"
        })

    monkeypatch.setattr(
        load_reference_data,
        "ACCOUNTS_PATH",
        accounts_file
    )

    # Execute account validation.
    loaded_count = load_reference_data.load_accounts(connection)

    assert loaded_count == 0

    # Verify the orphan account was not inserted.
    account = connection.execute(
        "SELECT * FROM accounts WHERE account_id = ?",
        ("A9999",)
    ).fetchone()

    assert account is None

    # Verify the exception was recorded.
    exception = connection.execute(
        """
        SELECT source_table, source_record_id, reason
        FROM exception_records
        WHERE source_record_id = ?
        """,
        ("A9999",)
    ).fetchone()

    assert exception is not None
    assert exception[0] == "accounts_reference"
    assert exception[1] == "A9999"
    assert "Unknown customer_id: C999" in exception[2]

    connection.close()
