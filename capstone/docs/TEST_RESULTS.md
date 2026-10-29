# Banking Capstone — Test Results

## 1. Test Environment

- Operating system: Windows
- Python: 3.14.7
- pytest: 9.1.1
- PySpark: used for Gold aggregation and pipeline execution

## 2. Latest Full Test Run

Command:

```powershell
python -m pytest tests/ -v
```

**Result: 31 passed, 0 failed out of 31 tests.**

| Test suite | Passed | Failed | Total |
|---|---:|---:|---:|
| Bronze ingestion | 4 | 0 | 4 |
| Capstone integration | 3 | 0 | 3 |
| Gold validation and normalization | 8 | 0 | 8 |
| Pipeline orchestration | 3 | 0 | 3 |
| Silver validation | 13 | 0 | 13 |
| **Total** | **31** | **0** | **31** |

Execution time: 1.14 seconds.

These results reflect the latest test output provided on 29 October 2026. Rerun the suite after subsequent code changes.

## 3. Verified Test Coverage

### Bronze Ingestion

- Recognized CSV ingestion.
- Raw string value preservation.
- Unchanged-file idempotency.
- Ignoring unrecognized filenames.
- Missing CSV header raises a `ValueError` and persists the failed-run audit.

### Silver Validation

- Valid transaction acceptance.
- Required-field validation.
- Invalid, nonpositive, and non-finite amount rejection.
- Invalid transaction date rejection.
- Unsupported transaction type and currency rejection.
- Unknown account rejection.
- Account/branch mismatch rejection.
- Lowercase value normalization.

### Gold Validation and Normalization

- Valid Silver snapshot acceptance.
- Blocking publication after a failed Silver run.
- Blocking publication when the Silver audit is missing.
- Blocking reconciliation failures.
- Blocking Silver and Exceptions table count mismatches.
- Normalizing numeric types and rounding amounts to two decimal places.

### Pipeline Orchestration

- Stages execute in Bronze → Silver → Gold order.
- Bronze failure stops downstream processing.
- Silver failure stops Gold processing.

### Capstone Integration

- `test_corrected_file_uses_latest_version`: verifies that a corrected transaction amount is used while historical Bronze rows remain available.
- `test_late_arriving_business_date_is_included`: verifies that a newly arriving file adds transactions for a second business date.
- `test_failed_silver_run_preserves_existing_gold`: verifies that a failed latest Silver audit blocks publication and preserves the existing Gold table and CSV.

## 4. Previously Outstanding Bronze Failure — Resolved

Test:

`tests/test_bronze_ingestion.py::test_missing_csv_header_fails_the_run`

The previous issue was that the failed-run audit was not preserved after ingestion rollback.

The implementation now commits the initial run record separately and records failure status using a separate database transaction after ingestion rollback.

**Verified result:** the regression test passes in the latest full suite.

## 5. End-to-End Pipeline Verification

Command:

```powershell
python -m src.capstone.pipeline
```

**Reported result: SUCCESS — 28 October 2026.**

| Metric | Result |
|---|---:|
| Files discovered | 3 |
| New files ingested | 0 |
| New Bronze rows | 0 |
| Bronze source rows available | 24 |
| Silver accepted | 10 |
| Silver rejected | 14 |
| Gold summary groups | 6 |
| PySpark vs SQLite | MATCH |
| Gold publication | SUCCESS |

Reconciliation:

`10 accepted + 14 rejected = 24 source rows`

Gold output locations:

- SQLite database: `database/capstone.db`
- CSV export: `output/gold/gold_summary.csv`

The zero new ingested files and rows are consistent with the unchanged-file idempotency behavior.

## 6. Environment Notes

Spark displayed Windows Hadoop and pandas compatibility warnings, including missing `winutils.exe`, unset `HADOOP_HOME`, and a pandas compatibility warning. These warnings did not prevent the reported pipeline run from completing successfully.

## 7. Remaining Limitations

The following behaviors still require dedicated verification:

- Reverting a corrected file to previously ingested content and confirming the intended current-version policy.
- Failure specifically during CSV replacement after the Gold SQLite transaction commits.
- Recovery and consistency between the Gold database table and CSV after a publication failure.
- Final submission evidence and confirmation that every Week 8 assignment requirement has been met.

The existing failed-Silver-run test does not prove atomic consistency when the filesystem replacement itself fails.

## 8. Next Steps

1. Add a test for file-content reversion.
2. Add a publication-failure and recovery test.
3. Capture the final pipeline and test output as submission evidence.
4. Review the README, runbook, design decisions, architecture diagram, and sample outputs against the assignment requirements.

## 9. Current Status

**Automated tests: PASS — 31/31.**

**Reported end-to-end pipeline: PASS.**

The current tests verify ingestion idempotency, validation, reconciliation, corrected-file handling, late-arriving business dates, publication blocking after a failed Silver audit, and PySpark/SQLite aggregation parity.

Publication recovery after a filesystem replacement failure remains unverified.
