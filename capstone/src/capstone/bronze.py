
"""Discover branch transaction CSVs and ingest raw rows into Bronze."""

import csv
import hashlib
import json
import re
import sqlite3
import uuid

from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
LANDING_DIR = PROJECT_ROOT / "data" / "landing"
DATABASE_PATH = PROJECT_ROOT / "database" / "capstone.db"

FILENAME_PATTERN = re.compile(
    r"^(?P<branch_id>BR\d+)_(?P<file_date>\d{8})_TRANSACTION\.csv$",
    re.IGNORECASE,
)


SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    status TEXT NOT NULL,
    files_discovered INTEGER NOT NULL DEFAULT 0,
    files_ingested INTEGER NOT NULL DEFAULT 0,
    rows_ingested INTEGER NOT NULL DEFAULT 0,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS source_files (
    source_file_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_filename TEXT NOT NULL,
    branch_id TEXT NOT NULL,
    filename_date TEXT NOT NULL,
    file_hash TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    source_row_count INTEGER NOT NULL DEFAULT 0,
    UNIQUE(source_filename, file_hash)
);

CREATE TABLE IF NOT EXISTS current_source_versions (
    source_filename TEXT PRIMARY KEY,
    source_file_id INTEGER NOT NULL,
    FOREIGN KEY (source_file_id)
        REFERENCES source_files(source_file_id)
);

CREATE TABLE IF NOT EXISTS bronze_records (
    bronze_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file_id INTEGER NOT NULL,
    source_row_number INTEGER NOT NULL,
    raw_record_json TEXT NOT NULL,
    run_id TEXT NOT NULL,
    ingested_at TEXT NOT NULL,
    FOREIGN KEY (source_file_id)
        REFERENCES source_files(source_file_id),
    FOREIGN KEY (run_id)
        REFERENCES pipeline_runs(run_id),
    UNIQUE(source_file_id, source_row_number)
);

CREATE TABLE IF NOT EXISTS file_ingestion_audit (
    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    source_filename TEXT NOT NULL,
    file_hash TEXT NOT NULL,
    outcome TEXT NOT NULL,
    source_rows INTEGER NOT NULL DEFAULT 0,
    message TEXT,
    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id)
);
"""


def utc_now():
    """Return an ISO-8601 UTC timestamp."""
    return datetime.now(timezone.utc).isoformat()


def discover_files():
    """Return recognized CSV files in deterministic filename order."""
    if not LANDING_DIR.exists():
        raise FileNotFoundError(
            f"Landing directory does not exist: {LANDING_DIR}"
        )

    recognized = []

    for path in sorted(LANDING_DIR.iterdir()):
        if not path.is_file() or path.suffix.lower() != ".csv":
            continue

        match = FILENAME_PATTERN.fullmatch(path.name)

        if not match:
            print(f"IGNORED: Unrecognized filename: {path.name}")
            continue

        # Reject filenames containing invalid calendar dates.
        try:
            file_date = datetime.strptime(
                match.group("file_date"), "%Y%m%d"
            ).date().isoformat()
        except ValueError as exc:
            raise ValueError(
                f"{path.name}: filename contains an invalid date"
            ) from exc

        recognized.append(
            {
                "path": path,
                "filename": path.name,
                "branch_id": match.group("branch_id").upper(),
                "filename_date": file_date,
            }
        )

    return recognized


def file_sha256(path):
    """Calculate a file content hash without modifying the source file."""
    digest = hashlib.sha256()

    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


def initialize_database(connection):
    """Create Bronze schema and initialize current-version pointers."""
    connection.executescript(SCHEMA)

    # Migrate existing databases by marking the latest known version
    # current when a pointer has not yet been initialized.
    existing_files = connection.execute(
        """
        SELECT source_filename, MAX(source_file_id)
        FROM source_files
        GROUP BY source_filename
        """
    ).fetchall()

    for filename, source_file_id in existing_files:
        connection.execute(
            """
            INSERT OR IGNORE INTO current_source_versions (
                source_filename,
                source_file_id
            )
            VALUES (?, ?)
            """,
            (filename, source_file_id),
        )


def _record_run_failure(run_id, error_message):
    """Persist failure status independently of the ingestion transaction."""
    with sqlite3.connect(DATABASE_PATH) as connection:
        connection.execute(
            """
            UPDATE pipeline_runs
            SET completed_at = ?,
                status = 'FAILED',
                files_ingested = 0,
                rows_ingested = 0,
                error_message = ?
            WHERE run_id = ?
            """,
            (utc_now(), error_message, run_id),
        )


def ingest_bronze():
    """
    Discover files and preserve raw rows and file versions.

    Idempotency policy:
    - Same filename and same current SHA-256: skip re-ingestion.
    - Previously ingested content can be reactivated without duplicate rows.
    - Same filename and new SHA-256: ingest a new immutable version.
    - Historical source_files and bronze_records are never deleted.
    - A failed ingestion rolls back the ingestion batch and retains its
      FAILED run audit.
    """
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)

    run_id = str(uuid.uuid4())
    started_at = utc_now()

    # Discover before creating the run record.
    discovered = discover_files()

    # Commit the RUNNING record before attempting file ingestion.
    with sqlite3.connect(DATABASE_PATH) as connection:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")

        initialize_database(connection)

        connection.execute(
            """
            INSERT INTO pipeline_runs (
                run_id,
                started_at,
                status,
                files_discovered
            )
            VALUES (?, ?, ?, ?)
            """,
            (run_id, started_at, "RUNNING", len(discovered)),
        )

    files_ingested = 0
    rows_ingested = 0

    try:
        # Keep file-version and raw-row changes in one transaction.
        with sqlite3.connect(DATABASE_PATH) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys = ON")

            for item in discovered:
                path = item["path"]
                digest = file_sha256(path)

                # Bronze retains the CSV field names and raw values.
                with path.open(
                    "r", encoding="utf-8-sig", newline=""
                ) as source:
                    reader = csv.DictReader(source)

                    if not reader.fieldnames:
                        raise ValueError(
                            f"{path.name}: CSV header is missing"
                        )

                    records = list(reader)

                source_rows = len(records)

                # Check whether this exact filename and content
                # have already been stored.
                existing = connection.execute(
                    """
                    SELECT source_file_id
                    FROM source_files
                    WHERE source_filename = ?
                      AND file_hash = ?
                    """,
                    (item["filename"], digest),
                ).fetchone()

                if existing:
                    # The file content exists in historical Bronze data.
                    current = connection.execute(
                        """
                        SELECT source_file_id
                        FROM current_source_versions
                        WHERE source_filename = ?
                        """,
                        (item["filename"],),
                    ).fetchone()

                    if (
                        current is not None
                        and current["source_file_id"]
                        == existing["source_file_id"]
                    ):
                        # Same content is already the current version.
                        outcome = "SKIPPED_UNCHANGED"
                        message = (
                            "Same filename and content are already current."
                        )
                    else:
                        # Reactivate the historical version without
                        # inserting duplicate source or Bronze records.
                        connection.execute(
                            """
                            INSERT INTO current_source_versions (
                                source_filename,
                                source_file_id
                            )
                            VALUES (?, ?)
                            ON CONFLICT(source_filename)
                            DO UPDATE SET
                                source_file_id = excluded.source_file_id
                            """,
                            (
                                item["filename"],
                                existing["source_file_id"],
                            ),
                        )

                        outcome = "REACTIVATED_PREVIOUS_VERSION"
                        message = (
                            "Previously ingested content reactivated as "
                            "current; existing Bronze rows reused."
                        )

                    connection.execute(
                        """
                        INSERT INTO file_ingestion_audit (
                            run_id,
                            source_filename,
                            file_hash,
                            outcome,
                            source_rows,
                            message
                        )
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            run_id,
                            item["filename"],
                            digest,
                            outcome,
                            source_rows,
                            message,
                        ),
                    )
                    continue

                # A new hash for this filename creates a new version.
                # Existing versions and their Bronze rows are preserved.
                previous_version = connection.execute(
                    """
                    SELECT source_file_id
                    FROM source_files
                    WHERE source_filename = ?
                    ORDER BY source_file_id DESC
                    LIMIT 1
                    """,
                    (item["filename"],),
                ).fetchone()

                cursor = connection.execute(
                    """
                    INSERT INTO source_files (
                        source_filename,
                        branch_id,
                        filename_date,
                        file_hash,
                        first_seen_at,
                        source_row_count
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        item["filename"],
                        item["branch_id"],
                        item["filename_date"],
                        digest,
                        utc_now(),
                        source_rows,
                    ),
                )

                source_file_id = cursor.lastrowid

                # Set the newly ingested version as current.
                connection.execute(
                    """
                    INSERT INTO current_source_versions (
                        source_filename,
                        source_file_id
                    )
                    VALUES (?, ?)
                    ON CONFLICT(source_filename)
                    DO UPDATE SET
                        source_file_id = excluded.source_file_id
                    """,
                    (item["filename"], source_file_id),
                )

                # Insert each raw row exactly once for this version.
                for row_number, record in enumerate(records, start=1):
                    connection.execute(
                        """
                        INSERT INTO bronze_records (
                            source_file_id,
                            source_row_number,
                            raw_record_json,
                            run_id,
                            ingested_at
                        )
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            source_file_id,
                            row_number,
                            json.dumps(
                                record,
                                ensure_ascii=False,
                                sort_keys=True,
                            ),
                            run_id,
                            utc_now(),
                        ),
                    )

                if previous_version:
                    message = (
                        "Corrected file version ingested; prior Bronze "
                        "version retained for lineage."
                    )
                else:
                    message = "Raw rows stored successfully."

                connection.execute(
                    """
                    INSERT INTO file_ingestion_audit (
                        run_id,
                        source_filename,
                        file_hash,
                        outcome,
                        source_rows,
                        message
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        item["filename"],
                        digest,
                        "INGESTED",
                        source_rows,
                        message,
                    ),
                )

                files_ingested += 1
                rows_ingested += source_rows

        # Record success after the ingestion transaction commits.
        with sqlite3.connect(DATABASE_PATH) as connection:
            connection.execute(
                """
                UPDATE pipeline_runs
                SET completed_at = ?,
                    status = 'SUCCESS',
                    files_ingested = ?,
                    rows_ingested = ?,
                    error_message = NULL
                WHERE run_id = ?
                """,
                (utc_now(), files_ingested, rows_ingested, run_id),
            )

    except Exception as exc:
        # The ingestion transaction has rolled back. Save the failure
        # status through a separate connection.
        _record_run_failure(run_id, str(exc))
        raise

    print(f"Run ID: {run_id}")
    print(f"Files discovered: {len(discovered)}")
    print(f"New files ingested: {files_ingested}")
    print(f"New Bronze rows: {rows_ingested}")
    print(f"Database: {DATABASE_PATH}")


if __name__ == "__main__":
    ingest_bronze()
