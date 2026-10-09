import random
from datetime import date, timedelta
from pathlib import Path

import pandas as pd


# ============================================================
# 1. Project paths
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[2]

OUTPUT_DIR = BASE_DIR / "data" / "generated"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = OUTPUT_DIR / "synthetic_transactions.csv"


# ============================================================
# 2. Reproducibility
# ============================================================

random.seed(42)


# ============================================================
# 3. Existing banking reference data
# ============================================================

CUSTOMER_IDS = [
    "C1001",
    "C1002",
    "C1003",
    "C1004",
    "C1005",
    "C1006",
]

ACCOUNT_DATA = [
    ("A1001", "C1001", "BR001", "CHECKING"),
    ("A1002", "C1001", "BR001", "SAVINGS"),
    ("A1007", "C1002", "BR001", "CHECKING"),
    ("A2001", "C1003", "BR002", "CHECKING"),
    ("A2005", "C1003", "BR002", "SAVINGS"),
    ("A2007", "C1004", "BR002", "CHECKING"),
    ("A3001", "C1005", "BR003", "CHECKING"),
    ("A3002", "C1005", "BR003", "SAVINGS"),
    ("A3004", "C1006", "BR003", "CHECKING"),
    ("A3007", "C1006", "BR003", "SAVINGS"),
]


# ============================================================
# 4. Configuration
# ============================================================

NUMBER_OF_TRANSACTIONS = 100_000

TRANSACTION_TYPES = [
    "CREDIT",
    "DEBIT",
]

CURRENCIES = [
    "USD",
]

START_DATE = date(2026, 1, 1)
NUMBER_OF_DAYS = 365


# ============================================================
# 5. Generate transactions
# ============================================================

transactions = []

for i in range(1, NUMBER_OF_TRANSACTIONS + 1):

    account_id, customer_id, branch_id, account_type = random.choice(
        ACCOUNT_DATA
    )

    transaction_type = random.choice(TRANSACTION_TYPES)

    transaction_date = START_DATE + timedelta(
        days=random.randint(0, NUMBER_OF_DAYS - 1)
    )

    amount = round(random.uniform(10, 5000), 2)

    transactions.append(
        {
            "transaction_id": f"SYN{i:06d}",
            "account_id": account_id,
            "customer_id": customer_id,
            "branch_id": branch_id,
            "account_type": account_type,
            "transaction_date": transaction_date.isoformat(),
            "transaction_type": transaction_type,
            "amount": amount,
            "currency": random.choice(CURRENCIES),
        }
    )


# ============================================================
# 6. Create DataFrame
# ============================================================

df = pd.DataFrame(transactions)


# ============================================================
# 7. Save CSV
# ============================================================

df.to_csv(
    OUTPUT_FILE,
    index=False
)


# ============================================================
# 8. Verification
# ============================================================

print("=" * 60)
print("SYNTHETIC BANKING DATA GENERATED")
print("=" * 60)

print("Output file:")
print(OUTPUT_FILE)

print("\nRow count:")
print(len(df))

print("\nColumns:")
print(list(df.columns))

print("\nSample rows:")
print(df.head())

print("\nTransaction type distribution:")
print(df["transaction_type"].value_counts())

print("\nTotal amount:")
print(round(df["amount"].sum(), 2))

print("\nUnique accounts:")
print(df["account_id"].nunique())

print("\nUnique customers:")
print(df["customer_id"].nunique())

print("\nUnique branches:")
print(df["branch_id"].nunique())

print("\nGeneration complete.")