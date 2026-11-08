
# Week 9 - Standalone Banking Analytics with dbt

## Project Overview

This project implements a standalone banking analytics
transformation pipeline using dbt Core and DuckDB.

The project uses three synthetic CSV datasets:
- customers.csv
- accounts.csv
- transactions.csv

## Technology Stack

- Python 3.14.7
- dbt Core 1.12.6
- dbt-duckdb 1.11.1
- DuckDB
- SQL
- YAML
- GitHub Actions

## Project Architecture

CSV Seeds
    |
Staging Models
    |
Intermediate Model
    |
Dimension and Fact Tables
    |
Data Quality Tests

## Models

### Staging
- stg_customers
- stg_accounts
- stg_transactions

### Intermediate
- int_transaction_enriched

### Marts
- dim_accounts
- fct_daily_account_activity

## Business Logic

DEPOSIT = Positive amount

PURCHASE = Negative amount

WITHDRAWAL = Negative amount

## Data Quality

- Unique tests
- Not-null tests
- Relationship tests
- Accepted-values tests
- Custom nonnegative amount test

## Execution Results

Seeds: 3 passed
Models: 6 passed
Data tests: 19 passed

Total operations: 28 passed

## Financial Reconciliation

Transactions: 48
Deposits: 3760 USD
Outflows: 1250 USD
Net Amount: 2510 USD

## Running the Project

1. Activate the virtual environment.
2. Install requirements.
3. Configure profiles.yml.
4. Run dbt seed.
5. Run dbt build.
6. Generate documentation.

## Lineage Graph

See evidence/lineage_graph.png.

## CI/CD

GitHub Actions workflow:
.github/workflows/week9_dbt_ci.yml

## Limitations

- Synthetic banking datasets.
- Local DuckDB development database.
- No production deployment.
