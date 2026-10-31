# CDP Readiness Extension – Customer 360

## 1. Project Overview

This project extends the Week 8 Integrated Banking Data Pipeline
by building a customer-level Gold data product and a small
read-only API.

The extension reuses trusted transaction records from the existing
Silver layer and combines them with synthetic customer and account
reference data.

The objective is to create a reliable Customer 360 view that
provides a consolidated summary for each customer.

This is a CDP-ready data product demonstration, not a complete
Customer Data Platform implementation.

## 2. Business Objective

Banks maintain customer information across multiple accounts
and transaction systems.

A customer may have:
- Multiple accounts.
- Multiple transactions.
- Accounts without transactions.
- No accounts or transactions.

This project provides one consolidated customer-level record
containing:

- Customer ID
- Customer name
- Account count
- Transaction count
- Total transaction amount
- Last transaction date

Customers without transactions are retained in the Gold output.

## 3. Technology Stack

| Technology | Purpose |
|---|---|
| Python | Data processing and API |
| SQLite | Data storage |
| SQL | Validation and Customer 360 aggregation |
| CSV | Reference data and Gold snapshot |
| HTTP Server | Read-only API |
| Pytest | Automated testing |
| PowerShell | Windows development environment |

No external API framework or cloud CDP platform is required.

## 4. Project Architecture

Existing Week 8 Silver Database
        |
        v
Trusted Silver Transactions
        |
        +----------------------+
        |                      |
        v                      v
Synthetic Customers       Synthetic Accounts
        |                      |
        +----------+-----------+
                   |
                   v
             CDP SQLite DB
                   |
                   v
          Data Quality Validation
                   |
                   v
           Customer 360 Gold
                   |
             +-----+-----+
             |           |
             v           v
         SQLite       CSV Snapshot
             |
             v
       Read-only API

Rejected records are stored in the exception_records table.

## 5. Data Model

### Customers

Grain: One row per customer.

| Column | Description |
|---|---|
| customer_id | Unique customer identifier |
| customer_name | Synthetic customer name |

### Accounts

Grain: One row per account.

| Column | Description |
|---|---|
| account_id | Unique account identifier |
| customer_id | Associated customer |
| branch_id | Branch identifier |
| account_type | Account type |
| account_status | Account status |

### Transactions

Grain: One row per transaction.

| Column | Description |
|---|---|
| transaction_id | Unique transaction identifier |
| account_id | Associated account |
| transaction_date | Transaction date |
| transaction_type | CREDIT or DEBIT |
| amount | Transaction amount |
| currency | Currency |
| branch_id | Branch identifier |

### Customer 360 Gold

Grain: One row per customer.

| Column | Description |
|---|---|
| customer_id | Customer identifier |
| customer_name | Customer name |
| account_count | Number of accounts |
| transaction_count | Number of transactions |
| total_transaction_amount | Sum of transaction amounts |
| last_transaction_date | Latest transaction date |

## 6. Identity Resolution

The project uses customer_id as the deterministic customer
identity key.

Relationships:

- customers.customer_id identifies a customer.
- accounts.customer_id connects accounts to customers.
- transactions.account_id connects transactions to accounts.

Customers are never merged based only on their names.

The project uses synthetic customer names and identifiers.

## 7. Data Quality Rules

The transaction ingestion process checks:

1. Required fields must not be missing.
2. Transaction IDs must be unique.
3. Account IDs must exist in the account reference table.
4. Existing transaction IDs are skipped during reruns.

Rejected records are stored in exception_records with
their rejection reasons.

## 8. Customer 360 Aggregation

The Customer 360 Gold table is built using SQL.

Separate aggregations are performed for account counts
and transaction metrics to prevent join fan-out.

LEFT JOIN logic retains customers without accounts
or transactions.

For customers without transactions:

- transaction_count = 0
- total_transaction_amount = 0
- last_transaction_date = NULL

## 9. Setup Instructions

Run these commands from the repository root:

```powershell
cd D:\mintways\banking_data_pipeline

.\.venv\Scripts\Activate.ps1