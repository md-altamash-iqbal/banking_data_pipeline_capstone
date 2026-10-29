
import pytest

from src.capstone.silver import validate_record


@pytest.fixture
def accounts():
    """Small account reference for isolated unit tests."""
    return {
        "A1001": "BR001",
        "A2001": "BR002",
    }


@pytest.fixture
def valid_record():
    """A transaction that should pass every validation check."""
    return {
        "transaction_id": "TXN_TEST_001",
        "account_id": "A1001",
        "transaction_date": "2026-09-06",
        "transaction_type": "CREDIT",
        "amount": "250.50",
        "currency": "USD",
        "_source_branch_id": "BR001",
    }


def test_valid_transaction_is_accepted(valid_record, accounts):
    normalized, errors = validate_record(valid_record, accounts)

    assert errors == []
    assert normalized["transaction_id"] == "TXN_TEST_001"
    assert normalized["account_id"] == "A1001"
    assert normalized["transaction_type"] == "CREDIT"
    assert normalized["amount"] == 250.50
    assert normalized["currency"] == "USD"


def test_missing_transaction_id_is_rejected(valid_record, accounts):
    valid_record["transaction_id"] = ""

    _, errors = validate_record(valid_record, accounts)

    assert "MISSING_TRANSACTION_ID" in errors


@pytest.mark.parametrize(
    ("amount", "expected_error"),
    [
        ("abc", "NONNUMERIC_AMOUNT"),
        ("0", "NONPOSITIVE_AMOUNT"),
        ("-25", "NONPOSITIVE_AMOUNT"),
        ("NaN", "NONFINITE_AMOUNT"),
        ("Infinity", "NONFINITE_AMOUNT"),
    ],
)
def test_invalid_amount_is_rejected(
    valid_record, accounts, amount, expected_error
):
    valid_record["amount"] = amount

    _, errors = validate_record(valid_record, accounts)

    assert expected_error in errors


def test_invalid_date_is_rejected(valid_record, accounts):
    valid_record["transaction_date"] = "2026-02-30"

    _, errors = validate_record(valid_record, accounts)

    assert "INVALID_TRANSACTION_DATE" in errors


def test_unsupported_transaction_type_is_rejected(
    valid_record, accounts
):
    valid_record["transaction_type"] = "TRANSFER"

    _, errors = validate_record(valid_record, accounts)

    assert "UNSUPPORTED_TRANSACTION_TYPE" in errors


def test_unsupported_currency_is_rejected(valid_record, accounts):
    valid_record["currency"] = "EUR"

    _, errors = validate_record(valid_record, accounts)

    assert "UNSUPPORTED_CURRENCY" in errors


def test_unknown_account_is_rejected(valid_record, accounts):
    valid_record["account_id"] = "A9999"

    _, errors = validate_record(valid_record, accounts)

    assert "UNKNOWN_ACCOUNT_ID" in errors


def test_account_branch_mismatch_is_rejected(
    valid_record, accounts
):
    valid_record["_source_branch_id"] = "BR002"

    _, errors = validate_record(valid_record, accounts)

    assert "ACCOUNT_BRANCH_MISMATCH" in errors


def test_lowercase_values_are_normalized(valid_record, accounts):
    valid_record["transaction_type"] = "credit"
    valid_record["currency"] = "usd"

    normalized, errors = validate_record(valid_record, accounts)

    assert errors == []
    assert normalized["transaction_type"] == "CREDIT"
    assert normalized["currency"] == "USD"
