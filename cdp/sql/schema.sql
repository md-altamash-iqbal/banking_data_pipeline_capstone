PRAGMA foreign_keys = ON;

-- 1. Customer reference table
CREATE TABLE IF NOT EXISTS customers (
    customer_id TEXT PRIMARY KEY,
    customer_name TEXT NOT NULL
);

-- 2. Account reference table
CREATE TABLE IF NOT EXISTS accounts (
    account_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    branch_id TEXT NOT NULL,
    account_type TEXT NOT NULL,
    account_status TEXT NOT NULL,

    FOREIGN KEY (customer_id)
        REFERENCES customers(customer_id)
);

-- 3. Trusted transactions copied from the existing Silver layer
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    transaction_date TEXT NOT NULL,
    transaction_type TEXT NOT NULL,
    amount REAL NOT NULL,
    currency TEXT NOT NULL,
    branch_id TEXT NOT NULL,

    FOREIGN KEY (account_id)
        REFERENCES accounts(account_id)
);

-- 4. Data-quality exception records
CREATE TABLE IF NOT EXISTS exception_records (
    exception_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_table TEXT NOT NULL,
    source_record_id TEXT,
    reason TEXT NOT NULL,
    raw_record_json TEXT,
    detected_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Helpful indexes for Customer 360 queries
CREATE INDEX IF NOT EXISTS idx_accounts_customer_id
ON accounts(customer_id);

CREATE INDEX IF NOT EXISTS idx_transactions_account_id
ON transactions(account_id);

CREATE INDEX IF NOT EXISTS idx_transactions_date
ON transactions(transaction_date);
