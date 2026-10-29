
import csv
import sqlite3

import pytest

import src.capstone.bronze as bronze


@pytest.fixture
def isolated_bronze(tmp_path, monkeypatch):
    """Use temporary landing files and a temporary SQLite database."""
    landing_dir = tmp_path / "landing"
    database_path = tmp_path / "database" / "test_capstone.db"

    landing_dir.mkdir(parents=True)
    database_path.parent.mkdir(parents=True)

    monkeypatch.setattr(bronze, "LANDING_DIR", landing_dir)
    monkeypatch.setattr(bronze, "DATABASE_PATH", database_path)

    return landing_dir, database_path


def write_transaction_file(path, transaction_id="TXN001"):
    """Create a small CSV using the expected filename pattern."""
    fieldnames = [
        "transaction_id",
        "account_id",
        "transaction_date",
        "transaction_type",
        "amount",
        "currency",
    ]

    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow({
            "transaction_id": transaction_id,
            "account_id": "A1001",
            "transaction_date": "2026-09-06",
            "transaction_type": "CREDIT",
            "amount": "250.50",
            "currency": "USD",
        })


def query_database(database_path, query, parameters=()):
    with sqlite3.connect(database_path) as connection:
        return connection.execute(query, parameters).fetchall()


def test_ingests_recognized_file_and_preserves_raw_values(
    isolated_bronze, capsys
):
    landing_dir, database_path = isolated_bronze

    filename = "BR001_20260906_TRANSACTION.csv"
    write_transaction_file(landing_dir / filename)

    bronze.ingest_bronze()

    records = query_database(
        database_path,
        """
        SELECT sf.source_filename, br.source_row_number,
               br.raw_record_json
        FROM bronze_records AS br
        JOIN source_files AS sf
          ON sf.source_file_id = br.source_file_id
        """,
    )

    assert len(records) == 1
    assert records[0][0] == filename
    assert records[0][1] == 1
    assert '"amount": "250.50"' in records[0][2]

    run = query_database(
        database_path,
        "SELECT status, files_ingested, rows_ingested FROM pipeline_runs",
    )

    assert run == [("SUCCESS", 1, 1)]


def test_unchanged_file_is_not_ingested_twice(
    isolated_bronze, capsys
):
    landing_dir, database_path = isolated_bronze

    write_transaction_file(
        landing_dir / "BR001_20260906_TRANSACTION.csv"
    )

    bronze.ingest_bronze()
    bronze.ingest_bronze()

    bronze_count = query_database(
        database_path,
        "SELECT COUNT(*) FROM bronze_records",
    )[0][0]

    source_file_count = query_database(
        database_path,
        "SELECT COUNT(*) FROM source_files",
    )[0][0]

    skipped_count = query_database(
        database_path,
        """
        SELECT COUNT(*)
        FROM file_ingestion_audit
        WHERE outcome = 'SKIPPED_UNCHANGED'
        """,
    )[0][0]

    latest_run = query_database(
        database_path,
        """
        SELECT status, files_ingested, rows_ingested
        FROM pipeline_runs
        ORDER BY started_at DESC, rowid DESC
        LIMIT 1
        """,
    )[0]

    assert bronze_count == 1
    assert source_file_count == 1
    assert skipped_count == 1
    assert latest_run == ("SUCCESS", 0, 0)


def test_unrecognized_csv_is_ignored(isolated_bronze, capsys):
    landing_dir, database_path = isolated_bronze

    write_transaction_file(
        landing_dir / "BR001_20260906_TRANSACTION.csv"
    )
    write_transaction_file(
        landing_dir / "notes.csv",
        transaction_id="UNRELATED001",
    )

    bronze.ingest_bronze()

    source_files = query_database(
        database_path,
        "SELECT source_filename FROM source_files",
    )

    assert source_files == [
        ("BR001_20260906_TRANSACTION.csv",)
    ]

    output = capsys.readouterr().out
    assert "IGNORED: Unrecognized filename: notes.csv" in output


def test_missing_csv_header_fails_the_run(isolated_bronze):
    landing_dir, database_path = isolated_bronze

    filename = "BR001_20260906_TRANSACTION.csv"
    (landing_dir / filename).write_text("", encoding="utf-8")

    with pytest.raises(ValueError, match="CSV header is missing"):
        bronze.ingest_bronze()

    runs = query_database(
        database_path,
        "SELECT status, error_message FROM pipeline_runs",
    )

    assert len(runs) == 1
    assert runs[0][0] == "FAILED"
    assert "CSV header is missing" in runs[0][1]

