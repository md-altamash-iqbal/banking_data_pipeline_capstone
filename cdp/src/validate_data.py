import json
import sqlite3
from pathlib import Path


# Project paths
PROJECT_ROOT = Path(__file__).resolve().parents[1]

CDP_DATABASE = PROJECT_ROOT / "database" / "cdp.db"

SILVER_DATABASE = (
    PROJECT_ROOT.parent
    / "capstone"
    / "database"
    / "capstone.db"
)


def record_exception(connection, record, reason):
    """Save a rejected transaction with its rejection reason."""

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
            "silver_transactions",
            record.get("transaction_id"),
            reason,
            json.dumps(record)
        )
    )


def load_silver_transactions(cdp_connection):
    """Read trusted transactions from the existing Silver database."""

    if not SILVER_DATABASE.exists():
        raise FileNotFoundError(
            f"Silver database not found: {SILVER_DATABASE}"
        )

    source_connection = sqlite3.connect(SILVER_DATABASE)
    source_connection.row_factory = sqlite3.Row

    try:
        rows = source_connection.execute(
            """
            SELECT
                transaction_id,
                account_id,
                transaction_date,
                transaction_type,
                amount,
                currency,
                branch_id
            FROM silver_records
            ORDER BY transaction_id
            """
        ).fetchall()

        return [dict(row) for row in rows]

    finally:
        source_connection.close()


def validate_and_load_transactions(cdp_connection, records):
    """Validate Silver records and load accepted transactions."""

    source_count = len(records)
    loaded_count = 0
    rejected_count = 0
    skipped_count = 0

    seen_transaction_ids = set()

    for record in records:

        transaction_id = record.get("transaction_id")
        account_id = record.get("account_id")

        # Check 1: Required fields
        required_fields = [
            "transaction_id",
            "account_id",
            "transaction_date",
            "transaction_type",
            "amount",
            "currency",
            "branch_id"
        ]

        missing_fields = [
            field
            for field in required_fields
            if record.get(field) is None
            or (
                isinstance(record.get(field), str)
                and not record[field].strip()
            )
        ]

        if missing_fields:
            record_exception(
                cdp_connection,
                record,
                f"Missing required fields: {', '.join(missing_fields)}"
            )
            rejected_count += 1
            continue

        # Check 2: Duplicate transaction ID within this load
        if transaction_id in seen_transaction_ids:
            record_exception(
                cdp_connection,
                record,
                "Duplicate transaction_id in source records"
            )
            rejected_count += 1
            continue

        seen_transaction_ids.add(transaction_id)

        # Check 3: Account must exist
        account_exists = cdp_connection.execute(
            """
            SELECT 1
            FROM accounts
            WHERE account_id = ?
            """,
            (account_id,)
        ).fetchone()

        if account_exists is None:
            record_exception(
                cdp_connection,
                record,
                f"Unknown account_id: {account_id}"
            )
            rejected_count += 1
            continue

        # Check 4: Do not insert an existing transaction again
        transaction_exists = cdp_connection.execute(
            """
            SELECT 1
            FROM transactions
            WHERE transaction_id = ?
            """,
            (transaction_id,)
        ).fetchone()

        if transaction_exists is not None:
            skipped_count += 1
            continue

        # Insert accepted transaction
        cdp_connection.execute(
            """
            INSERT INTO transactions (
                transaction_id,
                account_id,
                transaction_date,
                transaction_type,
                amount,
                currency,
                branch_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                transaction_id,
                account_id,
                record["transaction_date"],
                record["transaction_type"],
                record["amount"],
                record["currency"],
                record["branch_id"]
            )
        )

        loaded_count += 1

    return {
        "source": source_count,
        "loaded": loaded_count,
        "rejected": rejected_count,
        "skipped": skipped_count
    }


def main():
    """Run the Silver-to-CDP transaction ingestion."""

    if not CDP_DATABASE.exists():
        raise FileNotFoundError(
            f"CDP database not found: {CDP_DATABASE}"
        )

    connection = sqlite3.connect(CDP_DATABASE)
    connection.execute("PRAGMA foreign_keys = ON")

    try:
        records = load_silver_transactions(connection)

        # Keep the latest transaction-validation exception results.
        connection.execute(
            """
            DELETE FROM exception_records
            WHERE source_table = 'silver_transactions'
            """
        )

        results = validate_and_load_transactions(
            connection,
            records
        )

        connection.commit()

        transaction_total = connection.execute(
            "SELECT COUNT(*) FROM transactions"
        ).fetchone()[0]

        print("SILVER TRANSACTION INGESTION COMPLETED")
        print("--------------------------------------")
        print(f"Source records: {results['source']}")
        print(f"New transactions loaded: {results['loaded']}")
        print(f"Rejected records: {results['rejected']}")
        print(f"Already-existing records skipped: {results['skipped']}")
        print(f"Total transactions in CDP: {transaction_total}")

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


if __name__ == "__main__":
    main()