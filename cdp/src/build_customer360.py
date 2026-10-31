import sqlite3
import csv
from pathlib import Path



PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATABASE_PATH = PROJECT_ROOT / "database" / "cdp.db"
OUTPUT_PATH = PROJECT_ROOT / "output" / "gold" / "customer_360.csv"


CREATE_CUSTOMER360_SQL = """
CREATE TABLE IF NOT EXISTS customer_360 (
    customer_id TEXT PRIMARY KEY,
    customer_name TEXT NOT NULL,
    account_count INTEGER NOT NULL,
    transaction_count INTEGER NOT NULL,
    total_transaction_amount REAL NOT NULL,
    last_transaction_date TEXT,

    FOREIGN KEY (customer_id)
        REFERENCES customers(customer_id)
);
"""


BUILD_CUSTOMER360_SQL = """
INSERT INTO customer_360 (
    customer_id,
    customer_name,
    account_count,
    transaction_count,
    total_transaction_amount,
    last_transaction_date
)
WITH account_metrics AS (
    SELECT
        customer_id,
        COUNT(*) AS account_count
    FROM accounts
    GROUP BY customer_id
),
transaction_metrics AS (
    SELECT
        a.customer_id,
        COUNT(t.transaction_id) AS transaction_count,
        COALESCE(SUM(t.amount), 0) AS total_transaction_amount,
        MAX(t.transaction_date) AS last_transaction_date
    FROM accounts a
    LEFT JOIN transactions t
        ON a.account_id = t.account_id
    GROUP BY a.customer_id
)
SELECT
    c.customer_id,
    c.customer_name,
    COALESCE(am.account_count, 0),
    COALESCE(tm.transaction_count, 0),
    COALESCE(tm.total_transaction_amount, 0),
    tm.last_transaction_date
FROM customers c
LEFT JOIN account_metrics am
    ON c.customer_id = am.customer_id
LEFT JOIN transaction_metrics tm
    ON c.customer_id = tm.customer_id;
"""

def export_customer360(connection):
    """Export the Customer 360 Gold table to CSV."""

    OUTPUT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    rows = connection.execute("""
        SELECT
            customer_id,
            customer_name,
            account_count,
            transaction_count,
            total_transaction_amount,
            last_transaction_date
        FROM customer_360
        ORDER BY customer_id
    """).fetchall()

    columns = [
        "customer_id",
        "customer_name",
        "account_count",
        "transaction_count",
        "total_transaction_amount",
        "last_transaction_date"
    ]

    with OUTPUT_PATH.open(
        mode="w",
        encoding="utf-8",
        newline=""
    ) as file:

        writer = csv.writer(file)
        writer.writerow(columns)
        writer.writerows(rows)

    print(f"CSV published: {OUTPUT_PATH}")


def build_customer360():
    """Rebuild the Customer 360 Gold table from trusted CDP data."""

    if not DATABASE_PATH.exists():
        raise FileNotFoundError(
            f"CDP database not found: {DATABASE_PATH}"
        )

    connection = sqlite3.connect(DATABASE_PATH)
    connection.execute("PRAGMA foreign_keys = ON")

    try:
        connection.execute(CREATE_CUSTOMER360_SQL)

        # Clear previous Gold results before rebuilding.
        connection.execute("DELETE FROM customer_360")

        # Rebuild the complete customer-level dataset.
        connection.execute(BUILD_CUSTOMER360_SQL)

        connection.commit()

        export_customer360(connection)

        total_customers = connection.execute(
            "SELECT COUNT(*) FROM customer_360"
        ).fetchone()[0]

        print("CUSTOMER 360 GOLD BUILD SUCCESSFUL")
        print("----------------------------------")
        print(f"Customers published: {total_customers}")

        print("\nCUSTOMER 360 RESULTS:")

        rows = connection.execute(
            """
            SELECT
                customer_id,
                customer_name,
                account_count,
                transaction_count,
                total_transaction_amount,
                last_transaction_date
            FROM customer_360
            ORDER BY customer_id
            """
        ).fetchall()

        for row in rows:
            print(row)

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


if __name__ == "__main__":
    build_customer360()