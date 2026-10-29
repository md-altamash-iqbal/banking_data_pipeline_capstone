# Week 8 — Integrated Banking Data Engineering Capstone

## Project Overview

This project implements a local banking transaction pipeline using Python, SQLite, PySpark, CSV, and pytest. It ingests branch transaction files, validates records against business rules and account reference data, separates accepted and rejected records, and publishes aggregated Gold analytics.

The pipeline emphasizes traceability, data-quality validation, rerun safety, reconciliation, and verification of Gold aggregation results.

## Architecture

```text
Landing CSV Files
       |
       v
Bronze — Raw Ingestion and File Audit
       |
       v
Silver — Validation and Standardization
       |                    |
       v                    v
Accepted Silver       Exceptions
       |
       v
Gold — Branch / Date / Transaction Type Aggregation
       |
       +---- SQLite: gold_summary
       |
       +---- CSV: output/gold/gold_summary.csv
```

## Technology Stack

- Python
- SQLite
- PySpark
- CSV
- pytest

## Project Structure

```text
capstone/
├── data/
│   ├── landing/
│   └── reference/
├── database/
│   └── capstone.db
├── output/
│   └── gold/
│       └── gold_summary.csv
├── src/
│   └── capstone/
│       ├── bronze.py
│       ├── silver.py
│       ├── gold.py
│       └── pipeline.py
├── tests/
│   ├── test_bronze_ingestion.py
│   ├── test_capstone_integration.py
│   ├── test_gold.py
│   ├── test_pipeline.py
│   └── test_silver_validation.py
└── docs/
    ├── README.md
    ├── RUNBOOK.md
    ├── DESIGN_DECISIONS.md
    └── TEST_RESULTS.md
```

## Pipeline Stages

### Bronze — Raw Ingestion

- Discovers transaction CSV files matching the expected filename pattern.
- Ignores unrecognized filenames.
- Stores source-file metadata and raw transaction rows.
- Uses SHA-256 file hashes to skip unchanged files on reruns.
- Preserves historical source-file versions.
- Records ingestion outcomes and run status.

### Silver — Validation and Standardization

- Validates required fields and transaction dates.
- Checks amounts, transaction types, currencies, account references, and branch ownership.
- Separates accepted transactions from rejected records.
- Records rejection reasons.
- Reconciles source, accepted, and rejected row counts.
- Excludes superseded file versions from accepted Silver data.
- Applies the configured duplicate transaction ID policy.

### Gold — Analytics and Publication

- Aggregates accepted Silver transactions by branch, transaction date, and transaction type.
- Calculates transaction counts and total amounts.
- Compares PySpark results with an independent SQLite SQL aggregation.
- Blocks publication when the latest Silver audit or reconciliation checks fail.
- Publishes results to the SQLite `gold_summary` table and a CSV export.

## Run the Pipeline

Run the following commands from the capstone directory with the project's virtual environment activated:

```powershell
python -m src.capstone.pipeline
```

A successful run should show successful Bronze and Silver stages, matching PySpark and SQLite Gold results, and successful Gold publication.

## Run Automated Tests

```powershell
python -m pytest tests/ -v
```

The tests use isolated fixtures for ingestion, validation, Gold publication gates, pipeline orchestration, corrected files, late-arriving business dates, and preservation of previous Gold output when the latest Silver audit is failed.

## Latest Reported Pipeline Results

The latest successful end-to-end run processed the existing three landing files.

| Metric | Result |
|---|---:|
| Files discovered | 3 |
| New files ingested | 0 |
| New Bronze rows | 0 |
| Bronze source rows available | 24 |
| Silver accepted rows | 10 |
| Silver rejected rows | 14 |
| Gold aggregation groups | 6 |
| PySpark versus SQLite | MATCH |
| Gold publication | SUCCESS |

Silver reconciliation:

`10 accepted + 14 rejected = 24 source rows`

The zero new Bronze rows are expected when the discovered files have already been ingested with unchanged content.

## Latest Reported Test Results

The latest full test run reported:

- **31 passed**
- **0 failed**
- **1.14 seconds**

The test suite includes:

- Bronze ingestion, unchanged-file handling, unrecognized filenames, and missing-header failure.
- Silver validation and rejection reasons.
- Gold validation gates, reconciliation, table-count checks, and result normalization.
- Pipeline stage ordering and failure stopping behavior.
- Corrected-file version handling.
- Late-arriving business-date handling.
- Preservation of previous Gold outputs when publication is blocked by a failed Silver audit.

These results describe the reported test run. Rerun the suite after subsequent code changes.

## Known Limitations and Further Work

- Test reverting a corrected file to previously ingested content.
- Test a failure specifically during CSV replacement after the Gold database transaction commits.
- Verify recovery and consistency between the Gold SQLite table and CSV export.
- Capture final pipeline output and test results as project evidence.

A failed Silver audit is tested as a publication-blocking condition. Atomic consistency across the SQLite database and CSV file during a filesystem replacement failure has not yet been established.

## Documentation

- `docs/README.md` — project overview, architecture, execution, and current status.
- `docs/RUNBOOK.md` — execution, verification, recovery, and troubleshooting.
- `docs/DESIGN_DECISIONS.md` — architecture, versioning, validation, and reliability decisions.
- `docs/TEST_RESULTS.md` — test outcomes and remaining verification work.
