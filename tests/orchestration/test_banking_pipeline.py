
import json
import shutil
import sqlite3
from pathlib import Path

from dagster import RetryPolicy, job, op

from orchestration.definitions import banking_pipeline, REPORT_PATH


# --------------------------------------------------
# TEST 1: JOB REGISTRATION
# --------------------------------------------------

def test_banking_pipeline_job_is_registered():
    """Verify that all required pipeline ops are registered."""
    assert banking_pipeline.name == "banking_pipeline"

    op_names = {node.name for node in banking_pipeline.nodes}

    assert op_names == {
        "extract_banking_data",
        "validate_banking_data_op",
        "transform_banking_data",
        "publish_banking_report",
    }


# --------------------------------------------------
# TEST 2: MISSING DATABASE
# --------------------------------------------------

def test_pipeline_fails_when_database_is_missing(
    tmp_path,
    monkeypatch,
):
    """Verify that a missing database blocks downstream ops."""
    missing_database = tmp_path / "missing_banking.db"

    monkeypatch.setenv(
        "BANKING_DB_PATH",
        str(missing_database),
    )

    result = banking_pipeline.execute_in_process(
        raise_on_error=False,
    )

    assert not result.success

    failed_steps = {
        event.step_key
        for event in result.all_events
        if event.is_step_failure
    }

    assert "extract_banking_data" in failed_steps

    completed_steps = {
        event.step_key
        for event in result.all_events
        if event.is_step_success
    }

    assert "validate_banking_data_op" not in completed_steps
    assert "transform_banking_data" not in completed_steps
    assert "publish_banking_report" not in completed_steps


# --------------------------------------------------
# TEST 3: DQ FAILURE BLOCKS REPORT
# --------------------------------------------------

def test_dq_failure_blocks_report(tmp_path, monkeypatch):
    """Verify invalid data prevents report publication."""
    source_db = Path("database/banking.db").resolve()
    test_db = tmp_path / "banking_dq_failure.db"
    test_report = tmp_path / "banking_report.json"

    assert source_db.is_file()

    # Copy the source DB; never modify the original.
    shutil.copy2(source_db, test_db)

    # Insert an orphan transaction into the test copy.
    with sqlite3.connect(test_db) as connection:
        connection.execute(
            """
            INSERT INTO transactions (
                transaction_id,
                account_id,
                transaction_date,
                transaction_type,
                amount,
                currency
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                "DQ_TEST_INVALID_001",
                "ACCOUNT_DOES_NOT_EXIST",
                "2026-10-17",
                "CREDIT",
                100.0,
                "INR",
            ),
        )

    # Confirm the invalid record exists.
    with sqlite3.connect(test_db) as connection:
        invalid_row = connection.execute(
            """
            SELECT transaction_id, account_id
            FROM transactions
            WHERE transaction_id = ?
            """,
            ("DQ_TEST_INVALID_001",),
        ).fetchone()

    assert invalid_row == (
        "DQ_TEST_INVALID_001",
        "ACCOUNT_DOES_NOT_EXIST",
    )

    # Preserve the production report, if present.
    original_report = (
        REPORT_PATH.read_bytes()
        if REPORT_PATH.exists()
        else None
    )

    monkeypatch.setattr(
        "orchestration.definitions.REPORT_PATH",
        test_report,
    )
    monkeypatch.setenv("BANKING_DB_PATH", str(test_db))

    result = banking_pipeline.execute_in_process(
        raise_on_error=False,
    )

    assert not result.success

    failed_steps = {
        event.step_key
        for event in result.all_events
        if event.is_step_failure
    }

    assert "validate_banking_data_op" in failed_steps

    successful_steps = {
        event.step_key
        for event in result.all_events
        if event.is_step_success
    }

    assert "transform_banking_data" not in successful_steps
    assert "publish_banking_report" not in successful_steps

    # Failed validation must not publish a report.
    assert not test_report.exists()

    # The production report must remain unchanged.
    if original_report is not None:
        assert REPORT_PATH.read_bytes() == original_report


# --------------------------------------------------
# TEST 4: VALID RUN AND SAFE RERUN
# --------------------------------------------------

def test_valid_run_and_safe_rerun(tmp_path, monkeypatch):
    """Verify repeated runs do not duplicate source transactions."""
    source_db = Path("database/banking.db").resolve()
    test_db = tmp_path / "banking_recovery.db"
    test_report = tmp_path / "banking_report.json"

    assert source_db.is_file()

    # Use an isolated database copy.
    shutil.copy2(source_db, test_db)

    with sqlite3.connect(test_db) as connection:
        baseline_count = connection.execute(
            "SELECT COUNT(*) FROM transactions"
        ).fetchone()[0]

        baseline_unique_count = connection.execute(
            """
            SELECT COUNT(DISTINCT transaction_id)
            FROM transactions
            """
        ).fetchone()[0]

    monkeypatch.setenv("BANKING_DB_PATH", str(test_db))
    monkeypatch.setattr(
        "orchestration.definitions.REPORT_PATH",
        test_report,
    )

    # First run.
    first_result = banking_pipeline.execute_in_process(
        raise_on_error=False,
    )

    assert first_result.success
    assert test_report.is_file()

    first_report = json.loads(
        test_report.read_text(encoding="utf-8")
    )

    # Second run.
    second_result = banking_pipeline.execute_in_process(
        raise_on_error=False,
    )

    assert second_result.success

    second_report = json.loads(
        test_report.read_text(encoding="utf-8")
    )

    with sqlite3.connect(test_db) as connection:
        final_count = connection.execute(
            "SELECT COUNT(*) FROM transactions"
        ).fetchone()[0]

        final_unique_count = connection.execute(
            """
            SELECT COUNT(DISTINCT transaction_id)
            FROM transactions
            """
        ).fetchone()[0]

    assert final_count == baseline_count
    assert final_unique_count == baseline_unique_count

    # The generated timestamp changes; business summaries should not.
    assert first_report["summary"] == second_report["summary"]

    assert final_count == 20
    assert final_unique_count == 20


# --------------------------------------------------
# TEST 5: DAGSTER RETRY MECHANISM
# --------------------------------------------------

def test_transient_failure_is_retried():
    """Verify retries recover from a simulated transient failure."""
    attempts = {"count": 0}

    @op(
        retry_policy=RetryPolicy(
            max_retries=2,
            delay=0,
        )
    )
    def flaky_operation(context):
        attempts["count"] += 1

        context.log.info(
            f"Attempt number: {attempts['count']}"
        )

        if attempts["count"] < 3:
            raise RuntimeError(
                "Simulated transient failure"
            )

        return "success"

    @job
    def retry_test_job():
        flaky_operation()

    result = retry_test_job.execute_in_process(
        raise_on_error=False,
    )

    assert result.success
    assert attempts["count"] == 3
