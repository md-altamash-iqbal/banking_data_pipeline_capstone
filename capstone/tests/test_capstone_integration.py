
import csv
import json
import sqlite3

import pytest

from src.capstone import bronze, silver, gold


HEADERS = [
    "transaction_id",
    "account_id",
    "transaction_date",
    "transaction_type",
    "amount",
    "currency",
]


def write_transactions(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=HEADERS)
        writer.writeheader()
        writer.writerows(rows)


def transaction(transaction_id, date, amount):
    return {
        "transaction_id": transaction_id,
        "account_id": "A1001",
        "transaction_date": date,
        "transaction_type": "CREDIT",
        "amount": str(amount),
        "currency": "USD",
    }


@pytest.fixture
def isolated_pipeline(tmp_path, monkeypatch):
    landing = tmp_path / "landing"
    landing.mkdir()

    accounts = tmp_path / "accounts.csv"

    with accounts.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=["account_id", "branch_id"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "account_id": "A1001",
                "branch_id": "BR001",
            }
        )

    database = tmp_path / "capstone.db"
    gold_dir = tmp_path / "gold"
    gold_csv = gold_dir / "gold_summary.csv"

    monkeypatch.setattr(bronze, "LANDING_DIR", landing)
    monkeypatch.setattr(bronze, "DATABASE_PATH", database)

    monkeypatch.setattr(silver, "DATABASE_PATH", database)
    monkeypatch.setattr(silver, "ACCOUNTS_PATH", accounts)

    monkeypatch.setattr(gold, "DATABASE_PATH", database)
    monkeypatch.setattr(gold, "GOLD_DIR", gold_dir)
    monkeypatch.setattr(gold, "GOLD_CSV", gold_csv)

    return {
        "landing": landing,
        "database": database,
        "gold_csv": gold_csv,
    }


def run_bronze_silver():
    bronze.ingest_bronze()
    silver.run_silver_validation()


def test_corrected_file_uses_latest_version(isolated_pipeline):
    env = isolated_pipeline
    filename = "BR001_20260906_TRANSACTION.csv"
    path = env["landing"] / filename

    # Version A: original amount.
    write_transactions(
        path,
        [transaction("TX001", "2026-09-06", 10)],
    )
    run_bronze_silver()

    # Version B: corrected amount, with the same filename.
    write_transactions(
        path,
        [transaction("TX001", "2026-09-06", 15)],
    )
    run_bronze_silver()

    with sqlite3.connect(env["database"]) as conn:
        current_amount = conn.execute(
            """
            SELECT amount
            FROM silver_records
            WHERE transaction_id = 'TX001'
            """
        ).fetchone()[0]

        bronze_count = conn.execute(
            "SELECT COUNT(*) FROM bronze_records"
        ).fetchone()[0]

        reasons = [
            json.loads(row[0])
            for row in conn.execute(
                """
                SELECT rejection_reasons_json
                FROM exceptions_records
                """
            ).fetchall()
        ]

    assert current_amount == 15
    assert bronze_count == 2
    assert any(
        "SUPERSEDED_FILE_VERSION" in reason
        for reason in reasons
    )


def test_late_arriving_business_date_is_included(isolated_pipeline):
    env = isolated_pipeline

    # First file.
    write_transactions(
        env["landing"] / "BR001_20260906_TRANSACTION.csv",
        [transaction("TX001", "2026-09-06", 10)],
    )
    run_bronze_silver()

    # A later-arriving file introduces a new business date.
    write_transactions(
        env["landing"] / "BR001_20260907_TRANSACTION.csv",
        [transaction("TX002", "2026-09-07", 20)],
    )
    run_bronze_silver()

    with sqlite3.connect(env["database"]) as conn:
        rows = conn.execute(
            """
            SELECT transaction_id, transaction_date
            FROM silver_records
            ORDER BY transaction_date
            """
        ).fetchall()

    assert rows == [
        ("TX001", "2026-09-06"),
        ("TX002", "2026-09-07"),
    ]


def test_failed_silver_run_preserves_existing_gold(isolated_pipeline):
    env = isolated_pipeline
    database = env["database"]
    gold_csv = env["gold_csv"]

    gold_csv.parent.mkdir(parents=True, exist_ok=True)

    # Seed a previously published CSV.
    previous_csv = (
        "branch_id,transaction_date,transaction_type,"
        "transaction_count,total_amount\n"
        "BR001,2026-09-06,CREDIT,1,10.0\n"
    )
    gold_csv.write_text(previous_csv, encoding="utf-8")

    # Create the existing Gold table and a failed Silver audit.
    with sqlite3.connect(database) as conn:
        conn.execute(
            """
            CREATE TABLE gold_summary (
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

        conn.execute(
            """
            INSERT INTO gold_summary VALUES
            ('BR001', '2026-09-06', 'CREDIT', 1, 10.0)
            """
        )

        # Minimal tables needed for the Gold snapshot check.
        conn.execute(
            """
            CREATE TABLE silver_records (
                transaction_id TEXT PRIMARY KEY
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE exceptions_records (
                exception_id INTEGER PRIMARY KEY
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE silver_run_audit (
                run_id TEXT PRIMARY KEY,
                started_at TEXT,
                completed_at TEXT,
                status TEXT,
                source_rows INTEGER,
                accepted_rows INTEGER,
                rejected_rows INTEGER,
                error_message TEXT
            )
            """
        )

        conn.execute(
            """
            INSERT INTO silver_run_audit VALUES
            (
                'failed-test',
                '9999-12-31T23:59:59',
                '9999-12-31T23:59:59',
                'FAILED',
                1,
                0,
                1,
                'Simulated validation failure'
            )
            """
        )

        previous_table = conn.execute(
            "SELECT * FROM gold_summary"
        ).fetchall()

    with pytest.raises(RuntimeError, match="FAILED"):
        gold.build_gold()

    with sqlite3.connect(database) as conn:
        current_table = conn.execute(
            "SELECT * FROM gold_summary"
        ).fetchall()

    assert current_table == previous_table
    assert gold_csv.read_text(encoding="utf-8") == previous_csv


def test_reverting_to_previously_ingested_content(isolated_pipeline):
    env = isolated_pipeline
    filename = "BR001_20260906_TRANSACTION.csv"
    path = env["landing"] / filename

    # Version A: original amount.
    write_transactions(
        path,
        [transaction("TX001", "2026-09-06", 10)],
    )
    run_bronze_silver()

    # Version B: corrected amount.
    write_transactions(
        path,
        [transaction("TX001", "2026-09-06", 15)],
    )
    run_bronze_silver()

    # Restore the exact content of Version A.
    write_transactions(
        path,
        [transaction("TX001", "2026-09-06", 10)],
    )
    run_bronze_silver()

    with sqlite3.connect(env["database"]) as conn:
        amount = conn.execute(
            """
            SELECT amount
            FROM silver_records
            WHERE transaction_id = 'TX001'
            """
        ).fetchone()[0]

        versions = conn.execute(
            """
            SELECT source_file_id, file_hash
            FROM source_files
            WHERE source_filename = ?
            ORDER BY source_file_id
            """,
            (filename,),
        ).fetchall()

        current_version = conn.execute(
            """
            SELECT source_file_id
            FROM current_source_versions
            WHERE source_filename = ?
            """,
            (filename,),
        ).fetchone()

    # Restoring Version A reactivates its existing content.
    # Identical content is reused rather than stored as a third version.
    assert amount == 10
    assert len(versions) == 2
    assert versions[0][1] != versions[1][1]

    # Version A must be the active version after restoration.
    assert current_version is not None
    assert current_version[0] == versions[0][0]
