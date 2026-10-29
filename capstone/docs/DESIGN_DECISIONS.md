# Banking Capstone — Design Decisions

## 1. Architecture

The pipeline follows a layered architecture:

**Landing → Bronze → Silver → Exceptions + Gold**

Each layer has a separate responsibility, making the pipeline easier to test, audit, and maintain.

## 2. Bronze: Raw Ingestion

- Recognized filenames follow the pattern `BR001_YYYYMMDD_TRANSACTION.csv`, with the branch identifier and date varying by file.
- Source transaction values are preserved as strings in raw JSON records.
- SHA-256 hashes identify file content.
- Reprocessing an unchanged filename and content produces a `SKIPPED_UNCHANGED` audit outcome.
- Changed content under the same filename is ingested as a new source-file version.
- Historical source-file metadata and Bronze records are retained for traceability.
- Row numbers, ingestion timestamps, and run identifiers support auditing.
- A failed ingestion batch is rolled back, and the failure is recorded separately.

## 3. Silver: Validation and Standardization

- Required fields are checked before records are accepted.
- Transaction dates must be valid ISO calendar dates.
- Amounts must be numeric, finite, and positive.
- Transaction types are restricted to `CREDIT` and `DEBIT`.
- Currency is restricted to `USD`.
- Account IDs must exist in the reference data.
- Transaction branch ownership must match the account's branch.
- Rejected records retain their raw data and rejection reasons.
- Accepted and rejected counts are reconciled against the source row count.
- Silver and Exceptions are published within a database transaction.

### Corrected-file policy

For each exact source filename, the highest `source_file_id` is treated as the current version.

Rows from older versions remain in Bronze for auditability but are excluded from accepted Silver data and recorded in Exceptions with `SUPERSEDED_FILE_VERSION`.

The integration test `test_corrected_file_uses_latest_version` verifies that a corrected transaction amount is used while the previous Bronze record remains available.

### Duplicate transaction policy

Among otherwise-valid records from current file versions, the first valid occurrence of a transaction ID is accepted. Later valid occurrences are rejected with `DUPLICATE_TRANSACTION_ID`.

## 4. Late-Arriving Files

A late-arriving file with a new recognized filename is ingested through the normal Bronze process.

The transaction date is retained from the transaction record, while the filename date is stored as source metadata. Gold groups accepted records by branch, transaction date, and transaction type.

The integration test `test_late_arriving_business_date_is_included` verifies that a newly arriving file for a second business date is included in Silver without removing the previously accepted date.

## 5. Gold: Aggregation and Publication

Gold groups accepted Silver records by:

- Branch ID
- Transaction date
- Transaction type

Each group contains a transaction count and total amount.

PySpark performs the primary aggregation. An independent SQLite SQL aggregation verifies the results before publication.

The outputs are:

- SQLite table: `gold_summary`
- CSV export: `output/gold/gold_summary.csv`

Publication is blocked if the latest Silver audit is missing or unsuccessful, reconciliation fails, Silver or Exceptions table counts disagree with the audit, Silver contains no accepted records, or PySpark and SQLite results differ.

## 6. Idempotency and Reruns

Bronze uses the filename-and-content-hash combination to skip unchanged files.

A changed file with the same filename is ingested as a new version, while the historical version remains available.

The current implementation identifies the latest version using the highest `source_file_id` for that filename. Reverting a file to content that was previously ingested requires additional testing because the existing hash may be skipped without becoming the latest version.

## 7. Publication Safety and Known Limitation

Gold prepares a temporary CSV, validates the Silver snapshot, and writes the Gold SQLite table within a database transaction. It then replaces the published CSV using `os.replace`.

The database transaction and filesystem replacement are separate operations. If the database commit succeeds but CSV replacement fails, the database and CSV may represent different versions.

The current tests verify that a failed latest Silver audit blocks Gold publication and preserves the existing Gold table and CSV. They do not yet establish atomic consistency across a failure occurring specifically between the database commit and CSV replacement.

A dedicated publication-failure and recovery test remains necessary.

## 8. Technology Choices

- **Python:** pipeline orchestration and validation logic.
- **SQLite:** local persistent storage, audit records, and independent SQL verification.
- **PySpark:** DataFrame processing and Gold aggregation.
- **CSV:** source inputs and Gold export.
- **pytest:** automated tests for ingestion, validation, publication gates, and orchestration.

## 9. Verified Test Coverage

The latest reported full test run completed with **31 passed and 0 failed in 1.14 seconds**.

The suite includes tests for:

- Bronze ingestion, raw-value preservation, unchanged-file idempotency, unrecognized filenames, and missing CSV headers.
- Silver validation of required fields, dates, amounts, transaction types, currencies, accounts, and branch ownership.
- Gold publication gates, reconciliation, table counts, and numeric result normalization.
- Pipeline stage ordering and stopping after Bronze or Silver failure.
- Corrected-file handling, late-arriving business dates, and preservation of existing Gold output when the latest Silver audit is failed.

These results describe the reported test run. The test suite must be rerun after subsequent implementation changes.

## 10. Remaining Improvements

- Test reverting a corrected file to previously ingested content.
- Test a failure specifically during CSV replacement after the Gold database transaction commits.
- Define and verify recovery that reconciles the Gold SQLite table and CSV.
- Add a complete end-to-end publishing integration test.
- Capture the final pipeline output and test results as project evidence.
- Ensure the root project README links to the detailed documentation.
