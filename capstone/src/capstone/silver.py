
"""Validate Bronze records and publish accepted rows to Silver."""

import csv
import json
import sqlite3
import uuid

from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATABASE_PATH = PROJECT_ROOT / "database" / "capstone.db"
ACCOUNTS_PATH = PROJECT_ROOT / "data" / "reference" / "accounts.csv"

ALLOWED_TYPES = {"CREDIT", "DEBIT"}
ALLOWED_CURRENCIES = {"USD"}

REQUIRED_FIELDS = (
    "transaction_id",
    "account_id",
    "transaction_date",
    "transaction_type",
    "amount",
    "currency",
)


SCHEMA = """
CREATE TABLE IF NOT EXISTS current_source_versions (
    source_filename TEXT PRIMARY KEY,
    source_file_id INTEGER NOT NULL,
    FOREIGN KEY (source_file_id)
        REFERENCES source_files(source_file_id)
);

CREATE TABLE IF NOT EXISTS silver_records (
    transaction_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    transaction_date TEXT NOT NULL,
    transaction_type TEXT NOT NULL,
    amount REAL NOT NULL CHECK (amount > 0),
    currency TEXT NOT NULL,
    branch_id TEXT NOT NULL,
    filename_date TEXT NOT NULL,
    source_filename TEXT NOT NULL,
    source_file_id INTEGER NOT NULL,
    source_row_number INTEGER NOT NULL,
    bronze_id INTEGER NOT NULL UNIQUE,
    validated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS exceptions_records (
    exception_id INTEGER PRIMARY KEY AUTOINCREMENT,
    bronze_id INTEGER NOT NULL UNIQUE,
    source_filename TEXT NOT NULL,
    branch_id TEXT NOT NULL,
    filename_date TEXT NOT NULL,
    source_row_number INTEGER NOT NULL,
    raw_record_json TEXT NOT NULL,
    rejection_reasons_json TEXT NOT NULL,
    detected_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS silver_run_audit (
    run_id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    completed_at TEXT NOT NULL,
    status TEXT NOT NULL,
    source_rows INTEGER NOT NULL,
    accepted_rows INTEGER NOT NULL,
    rejected_rows INTEGER NOT NULL,
    error_message TEXT
);
"""


def utc_now():
    """Return the current UTC timestamp."""
    return datetime.now(timezone.utc).isoformat()


def load_accounts():
    """Load account_id -> branch_id mappings from accounts.csv."""
    if not ACCOUNTS_PATH.exists():
        raise FileNotFoundError(
            f"Account reference file not found: {ACCOUNTS_PATH}"
        )

    with ACCOUNTS_PATH.open(
        "r", encoding="utf-8-sig", newline=""
    ) as source:
        reader = csv.DictReader(source)

        if not reader.fieldnames:
            raise ValueError("accounts.csv has no header")

        columns = {
            name.strip().lower(): name
            for name in reader.fieldnames
            if name and name.strip()
        }

        required = {"account_id", "branch_id"}
        missing = required - set(columns)

        if missing:
            raise ValueError(
                "accounts.csv is missing required columns: "
                + ", ".join(sorted(missing))
            )

        accounts = {}

        for row in reader:
            account_id = (
                row.get(columns["account_id"]) or ""
            ).strip()

            branch_id = (
                row.get(columns["branch_id"]) or ""
            ).strip()

            if not account_id:
                continue

            if account_id in accounts:
                raise ValueError(
                    f"Duplicate account_id in accounts.csv: {account_id}"
                )

            if not branch_id:
                raise ValueError(
                    f"Missing branch_id for account: {account_id}"
                )

            accounts[account_id] = branch_id

    return accounts


def validate_record(record, accounts):
    """Normalize a transaction and return all validation errors."""
    errors = []

    # Check required fields.
    for field in REQUIRED_FIELDS:
        value = record.get(field)

        if value is None or not str(value).strip():
            errors.append(f"MISSING_{field.upper()}")

    transaction_id = str(
        record.get("transaction_id") or ""
    ).strip()

    account_id = str(
        record.get("account_id") or ""
    ).strip()

    transaction_date = str(
        record.get("transaction_date") or ""
    ).strip()

    transaction_type = str(
        record.get("transaction_type") or ""
    ).strip().upper()

    currency = str(
        record.get("currency") or ""
    ).strip().upper()

    source_branch = str(
        record.get("_source_branch_id") or ""
    ).strip()

    normalized_amount = None

    # Validate date format and actual calendar date.
    if transaction_date:
        try:
            parsed_date = date.fromisoformat(transaction_date)

            if parsed_date.isoformat() != transaction_date:
                raise ValueError("Date is not canonical ISO format")

        except ValueError:
            errors.append("INVALID_TRANSACTION_DATE")

    # Validate amount.
    raw_amount = record.get("amount")

    if raw_amount is not None and str(raw_amount).strip():
        try:
            amount = Decimal(str(raw_amount).strip())

            if not amount.is_finite():
                errors.append("NONFINITE_AMOUNT")

            elif amount <= 0:
                errors.append("NONPOSITIVE_AMOUNT")

            else:
                normalized_amount = amount

        except (InvalidOperation, ValueError):
            errors.append("NONNUMERIC_AMOUNT")

    # Validate transaction type.
    if transaction_type and transaction_type not in ALLOWED_TYPES:
        errors.append("UNSUPPORTED_TRANSACTION_TYPE")

    # Validate currency.
    if currency and currency not in ALLOWED_CURRENCIES:
        errors.append("UNSUPPORTED_CURRENCY")

    # Validate account existence and branch ownership.
    if account_id:
        if account_id not in accounts:
            errors.append("UNKNOWN_ACCOUNT_ID")
        else:
            expected_branch = accounts[account_id]

            if source_branch and source_branch != expected_branch:
                errors.append("ACCOUNT_BRANCH_MISMATCH")

    normalized = {
        "transaction_id": transaction_id,
        "account_id": account_id,
        "transaction_date": transaction_date,
        "transaction_type": transaction_type,
        "amount": (
            float(normalized_amount)
            if normalized_amount is not None
            else None
        ),
        "currency": currency,
    }

    return normalized, errors


def run_silver_validation():
    """
    Validate Bronze records and publish Silver and Exceptions.

    Corrected-file policy:
    - current_source_versions identifies the active version per filename.
    - Previously ingested content can be reactivated without duplicate rows.
    - Records from older versions remain in Bronze.
    - Older-version rows are retained in Exceptions with
      SUPERSEDED_FILE_VERSION.
    - The first otherwise-valid transaction ID among current versions wins.
    - Later valid occurrences of that transaction ID are rejected.
    """
    run_id = str(uuid.uuid4())
    started_at = utc_now()

    accounts = load_accounts()

    with sqlite3.connect(DATABASE_PATH) as connection:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.executescript(SCHEMA)

        # Initialize missing pointers for legacy databases.
        # Existing pointers are preserved so previously ingested content
        # can be reactivated without replacing the current version.
        connection.execute(
            """
            INSERT OR IGNORE INTO current_source_versions (
                source_filename,
                source_file_id
            )
            SELECT source_filename, MAX(source_file_id)
            FROM source_files
            GROUP BY source_filename
            """
        )

        # Read all Bronze history and identify the active version
        # of each exact source filename.
        bronze_rows = connection.execute(
            """
            SELECT
                b.bronze_id,
                b.source_row_number,
                b.raw_record_json,
                sf.source_file_id,
                sf.source_filename,
                sf.branch_id,
                sf.filename_date,
                b.run_id AS bronze_run_id,
                CASE
                    WHEN csv.source_file_id = sf.source_file_id THEN 1
                    ELSE 0
                END AS is_current_version
            FROM bronze_records b
            JOIN source_files sf
                ON sf.source_file_id = b.source_file_id
            LEFT JOIN current_source_versions csv
                ON csv.source_filename = sf.source_filename
            ORDER BY
                sf.source_file_id,
                b.source_row_number
            """
        ).fetchall()

        prepared = []

        # Validate each Bronze record without changing the raw evidence.
        for row in bronze_rows:
            raw = json.loads(row["raw_record_json"])

            validation_record = dict(raw)
            validation_record["_source_branch_id"] = row["branch_id"]

            normalized, errors = validate_record(
                validation_record,
                accounts,
            )

            # Superseded versions are never eligible for Silver.
            if not row["is_current_version"]:
                errors.append("SUPERSEDED_FILE_VERSION")

            prepared.append(
                {
                    "metadata": row,
                    "raw": raw,
                    "normalized": normalized,
                    "errors": errors,
                }
            )

        # Duplicate policy: first otherwise-valid transaction ID wins.
        # Superseded or otherwise-invalid rows do not reserve an ID.
        seen_transaction_ids = set()

        for item in prepared:
            if item["errors"]:
                continue

            transaction_id = item["normalized"]["transaction_id"]

            if transaction_id in seen_transaction_ids:
                item["errors"].append("DUPLICATE_TRANSACTION_ID")
            else:
                seen_transaction_ids.add(transaction_id)

        accepted = [
            item for item in prepared if not item["errors"]
        ]

        rejected = [
            item for item in prepared if item["errors"]
        ]

        # Every Bronze row must be accounted for.
        source_count = len(bronze_rows)
        accepted_count = len(accepted)
        rejected_count = len(rejected)

        if accepted_count + rejected_count != source_count:
            raise RuntimeError(
                "Reconciliation failed: accepted + rejected "
                "does not equal source rows"
            )

        try:
            # Publish Silver, Exceptions, and the successful audit
            # atomically so partial snapshots are not committed.
            with connection:
                connection.execute("DELETE FROM silver_records")
                connection.execute("DELETE FROM exceptions_records")

                for item in accepted:
                    meta = item["metadata"]
                    record = item["normalized"]

                    connection.execute(
                        """
                        INSERT INTO silver_records (
                            transaction_id,
                            account_id,
                            transaction_date,
                            transaction_type,
                            amount,
                            currency,
                            branch_id,
                            filename_date,
                            source_filename,
                            source_file_id,
                            source_row_number,
                            bronze_id,
                            validated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            record["transaction_id"],
                            record["account_id"],
                            record["transaction_date"],
                            record["transaction_type"],
                            record["amount"],
                            record["currency"],
                            meta["branch_id"],
                            meta["filename_date"],
                            meta["source_filename"],
                            meta["source_file_id"],
                            meta["source_row_number"],
                            meta["bronze_id"],
                            utc_now(),
                        ),
                    )

                for item in rejected:
                    meta = item["metadata"]

                    connection.execute(
                        """
                        INSERT INTO exceptions_records (
                            bronze_id,
                            source_filename,
                            branch_id,
                            filename_date,
                            source_row_number,
                            raw_record_json,
                            rejection_reasons_json,
                            detected_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            meta["bronze_id"],
                            meta["source_filename"],
                            meta["branch_id"],
                            meta["filename_date"],
                            meta["source_row_number"],
                            json.dumps(
                                item["raw"],
                                ensure_ascii=False,
                                sort_keys=True,
                            ),
                            json.dumps(
                                sorted(set(item["errors"]))
                            ),
                            utc_now(),
                        ),
                    )

                connection.execute(
                    """
                    INSERT INTO silver_run_audit (
                        run_id,
                        started_at,
                        completed_at,
                        status,
                        source_rows,
                        accepted_rows,
                        rejected_rows,
                        error_message
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
                    """,
                    (
                        run_id,
                        started_at,
                        utc_now(),
                        "SUCCESS",
                        source_count,
                        accepted_count,
                        rejected_count,
                    ),
                )

        except Exception as exc:
            # The snapshot transaction has rolled back. Record the
            # failed validation run separately and preserve the error.
            with connection:
                connection.execute(
                    """
                    INSERT INTO silver_run_audit (
                        run_id,
                        started_at,
                        completed_at,
                        status,
                        source_rows,
                        accepted_rows,
                        rejected_rows,
                        error_message
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        started_at,
                        utc_now(),
                        "FAILED",
                        source_count,
                        0,
                        0,
                        str(exc),
                    ),
                )

            raise

    print(f"Silver run ID: {run_id}")
    print(f"Source rows: {source_count}")
    print(f"Accepted rows: {accepted_count}")
    print(f"Rejected rows: {rejected_count}")
    print(
        f"Reconciliation: {accepted_count} + {rejected_count} "
        f"= {source_count}"
    )
    print("Status: SUCCESS")


if __name__ == "__main__":
    run_silver_validation()
