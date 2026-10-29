# Banking Capstone — Runbook

## 1. Purpose

This runbook explains how to execute, verify, troubleshoot, and recover the Week 8 Integrated Banking Data Engineering Capstone.

## 2. Prerequisites

- Python and the project's virtual environment.
- Required project dependencies, including PySpark and pytest.
- Transaction CSV files in `data/landing/`.
- Account reference data in `data/reference/accounts.csv`.

Run all commands from the `capstone` directory.

## 3. Run the Complete Pipeline

Activate the project's virtual environment if it is not already active. Then run:

```powershell
python -m src.capstone.pipeline
```

The pipeline executes these stages in order:

1. **Bronze:** discovers recognized files and ingests raw records.
2. **Silver:** validates transactions and separates accepted and rejected records.
3. **Gold:** aggregates accepted Silver records and publishes the summary.

The pipeline stops if a stage raises an exception.

A successful run should report successful Bronze and Silver stages, matching PySpark and SQLite results, and successful Gold publication.

## 4. Run Automated Tests

Run the complete suite:

```powershell
python -m pytest tests/ -v
```

Run individual test modules:

```powershell
python -m pytest tests/test_bronze_ingestion.py -v
python -m pytest tests/test_silver_validation.py -v
python -m pytest tests/test_gold.py -v
python -m pytest tests/test_pipeline.py -v
python -m pytest tests/test_capstone_integration.py -v
```

The latest reported full test run completed with **31 passed, 0 failed in 1.14 seconds**. Rerun the tests after any subsequent implementation changes.

## 5. Verify Pipeline Outputs

### Bronze

Inspect these tables in `database/capstone.db`:

- `pipeline_runs`
- `source_files`
- `bronze_records`
- `file_ingestion_audit`

For unchanged files, verify that the audit records `SKIPPED_UNCHANGED` and no additional Bronze rows are inserted.

For a corrected file, inspect its source-file versions and ingestion audit records.

### Silver and Exceptions

Inspect:

- `silver_records`
- `exceptions_records`
- `silver_run_audit`

Verify that:

`source_rows = accepted_rows + rejected_rows`

For corrected files, check that older versions are recorded in Exceptions with `SUPERSEDED_FILE_VERSION` and do not contribute to accepted Silver records.

For invalid records, inspect `rejection_reasons_json`.

### Gold

Verify the SQLite table:

`gold_summary`

Verify the CSV export:

`output/gold/gold_summary.csv`

Confirm that the Gold stage reports `PySpark vs SQLite: MATCH` and `Gold publication: SUCCESS`.

The reported successful pipeline run produced 6 Gold aggregation groups from 10 accepted Silver records.

## 6. Corrected and Late-Arriving Files

### Corrected files

The current policy ingests changed content under the same filename as a new source-file version while retaining previous Bronze records.

Silver treats the highest `source_file_id` for that exact filename as the current version.

The integration test `test_corrected_file_uses_latest_version` verifies that the corrected amount is used and historical Bronze rows remain available.

A file reverting to previously ingested content requires additional verification. The existing hash-based skip logic may skip the content without making that older version current.

### Late-arriving files

A newly arriving file with a recognized filename is processed through the normal pipeline.

The integration test `test_late_arriving_business_date_is_included` verifies that a new business date is added while the earlier accepted date remains present.

## 7. Failure Handling and Recovery

### Bronze ingestion failure

1. Read the exception message in the terminal.
2. Check the CSV header, filename, and input data.
3. Inspect `pipeline_runs` and `file_ingestion_audit`.
4. Confirm that a failed ingestion is audited and the ingestion transaction has rolled back.
5. Fix the input or implementation issue and rerun the relevant tests.

The missing-header regression test currently passes in the latest reported test suite.

### Silver validation failure

1. Inspect the latest row in `silver_run_audit`.
2. Check the validation error and the affected source records.
3. Correct the input or reference data.
4. Rerun the pipeline and verify reconciliation.

Gold must refuse publication when the latest Silver audit is unsuccessful. The integration test `test_failed_silver_run_preserves_existing_gold` verifies that the previous Gold table and CSV remain unchanged when publication is blocked by a failed Silver audit.

### Gold publication failure

1. Inspect the exception message and latest Silver audit.
2. Check the Gold SQLite table and