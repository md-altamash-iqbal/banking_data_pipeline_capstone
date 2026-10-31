import csv
import json
import sqlite3
from pathlib import Path


# Project paths
PROJECT_ROOT = Path(__file__).resolve().parents[1]

DATABASE_PATH = PROJECT_ROOT / "database" / "cdp.db"
SCHEMA_PATH = PROJECT_ROOT / "sql" / "schema.sql"

CUSTOMERS_PATH = PROJECT_ROOT / "data" / "reference" / "customers.csv"
ACCOUNTS_PATH = PROJECT_ROOT / "data" / "reference" / "accounts.csv"


def read_csv(file_path):
    """Read a CSV file and return its rows as dictionaries."""
    with file_path.open(
        mode="r",
        encoding="utf-8-sig",
        newline=""
    ) as file:
        return list(csv.DictReader(file))


def record_exception(
    connection,
    source_table,
    record_id,
    reason,
    raw_record
):
    """Store a data-quality exception."""
    connection.execute(
        """
        INSERT INTO exception_records (
            source_table,
            source_record_id,
            reason,
            raw_record_json
        )
        VALUES (?, ?, ?, ?)
        """,
        (
            source_table,
            record_id,
            reason,
            json.dumps(raw_record)
        )
    )


def initialize_database(connection):
    """Create database tables using schema.sql."""
    schema = SCHEMA_PATH.read_text(encoding="utf-8-sig")
    connection.executescript(schema)


def load_customers(connection):
    """Validate and load customer reference records."""
    rows = read_csv(CUSTOMERS_PATH)

    seen_ids = set()
    loaded_count = 0

    for row in rows:
        customer_id = (row.get("customer_id") or "").strip()
        customer_name = (row.get("customer_name") or "").strip()

        if not customer_id or not customer_name:
            record_exception(
                connection,
                "customers_reference",
                customer_id or None,
                "Missing customer_id or customer_name",
                row
            )
            continue

        if customer_id in seen_ids:
            record_exception(
                connection,
                "customers_reference",
                customer_id,
                "Duplicate customer_id in reference file",
                row
            )
            continue

        seen_ids.add(customer_id)

        connection.execute(
            """
            INSERT INTO customers (
                customer_id,
                customer_name
            )
            VALUES (?, ?)
            ON CONFLICT(customer_id)
            DO UPDATE SET
                customer_name = excluded.customer_name
            """,
            (customer_id, customer_name)
        )

        loaded_count += 1

    return loaded_count


def load_accounts(connection):
    """Validate and load account reference records."""
    rows = read_csv(ACCOUNTS_PATH)

    seen_ids = set()
    loaded_count = 0

    for row in rows:
        account_id = (row.get("account_id") or "").strip()
        customer_id = (row.get("customer_id") or "").strip()
        branch_id = (row.get("branch_id") or "").strip()
        account_type = (row.get("account_type") or "").strip()
        account_status = (row.get("account_status") or "").strip()

        required_values = {
            "account_id": account_id,
            "customer_id": customer_id,
            "branch_id": branch_id,
            "account_type": account_type,
            "account_status": account_status
        }

        missing_fields = [
            field
            for field, value in required_values.items()
            if not value
        ]

        if missing_fields:
            record_exception(
                connection,
                "accounts_reference",
                account_id or None,
                f"Missing required fields: {', '.join(missing_fields)}",
                row
            )
            continue

        if account_id in seen_ids:
            record_exception(
                connection,
                "accounts_reference",
                account_id,
                "Duplicate account_id in reference file",
                row
            )
            continue

        seen_ids.add(account_id)

        customer_exists = connection.execute(
            """
            SELECT 1
            FROM customers
            WHERE customer_id = ?
            """,
            (customer_id,)
        ).fetchone()

        if customer_exists is None:
            record_exception(
                connection,
                "accounts_reference",
                account_id,
                f"Unknown customer_id: {customer_id}",
                row
            )
            continue

        connection.execute(
            """
            INSERT INTO accounts (
                account_id,
                customer_id,
                branch_id,
                account_type,
                account_status
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(account_id)
            DO UPDATE SET
                customer_id = excluded.customer_id,
                branch_id = excluded.branch_id,
                account_type = excluded.account_type,
                account_status = excluded.account_status
            """,
            (
                account_id,
                customer_id,
                branch_id,
                account_type,
                account_status
            )
        )

        loaded_count += 1

    return loaded_count


def main():
    """Run the complete reference-data loading process."""
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(DATABASE_PATH)
    connection.execute("PRAGMA foreign_keys = ON")

    try:
        initialize_database(connection)

        # Replace only exceptions from the reference-data load.
        connection.execute(
            """
            DELETE FROM exception_records
            WHERE source_table IN (
                'customers_reference',
                'accounts_reference'
            )
            """
        )

        customer_count = load_customers(connection)
        account_count = load_accounts(connection)

        connection.commit()

        print("REFERENCE DATA LOAD SUCCESSFUL")
        print("--------------------------------")
        print(f"Customers loaded: {customer_count}")
        print(f"Accounts loaded: {account_count}")

        customer_total = connection.execute(
            "SELECT COUNT(*) FROM customers"
        ).fetchone()[0]

        account_total = connection.execute(
            "SELECT COUNT(*) FROM accounts"
        ).fetchone()[0]

        exception_total = connection.execute(
            "SELECT COUNT(*) FROM exception_records"
        ).fetchone()[0]

        print(f"Total customers in database: {customer_total}")
        print(f"Total accounts in database: {account_total}")
        print(f"Reference exceptions: {exception_total}")
        print(f"Database: {DATABASE_PATH}")

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


if __name__ == "__main__":
    main()