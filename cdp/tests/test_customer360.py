import sqlite3

from cdp.src import build_customer360


def create_test_database(path):
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

        INSERT INTO customers VALUES
            ('C001', 'Alex Morgan'),
            ('C002', 'Jordan Lee');

        INSERT INTO accounts VALUES
            ('A001', 'C001'),
            ('A002', 'C001');

        INSERT INTO transactions VALUES
            ('T001', 'A001', '2026-09-01', 'CREDIT', 100, 'USD', 'BR001'),
            ('T002', 'A001', '2026-09-02', 'DEBIT', 50, 'USD', 'BR001'),
            ('T003', 'A002', '2026-09-03', 'CREDIT', 200, 'USD', 'BR001');
    """)

    connection.commit()
    connection.close()


def test_customer_with_multiple_accounts(tmp_path, monkeypatch):
    database_path = tmp_path / "test.db"

    create_test_database(database_path)

    monkeypatch.setattr(
        build_customer360,
        "DATABASE_PATH",
        database_path
    )

    build_customer360.build_customer360()

    connection = sqlite3.connect(database_path)

    result = connection.execute("""
        SELECT account_count, transaction_count,
               total_transaction_amount
        FROM customer_360
        WHERE customer_id = 'C001'
    """).fetchone()

    connection.close()

    assert result == (2, 3, 350.0)


def test_customer_with_no_transactions(tmp_path, monkeypatch):
    database_path = tmp_path / "test.db"

    create_test_database(database_path)

    monkeypatch.setattr(
        build_customer360,
        "DATABASE_PATH",
        database_path
    )

    build_customer360.build_customer360()

    connection = sqlite3.connect(database_path)

    result = connection.execute("""
        SELECT account_count, transaction_count,
               total_transaction_amount, last_transaction_date
        FROM customer_360
        WHERE customer_id = 'C002'
    """).fetchone()

    connection.close()

    assert result == (0, 0, 0.0, None)