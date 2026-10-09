# Star Schema

The analytical layer is built from the trusted operational database `banking.db` and stored in `analytics.db`.

## Analytical Star Schema

```mermaid
flowchart TD

    C["dim_customer<br/><br/>PK customer_id<br/>customer_name"]

    A["dim_account<br/><br/>PK account_id<br/>account_type"]

    B["dim_branch<br/><br/>PK branch_id<br/>branch_name"]

    D["dim_date<br/><br/>PK date_key<br/>full_date"]

    F[("fact_transaction<br/><br/>PK transaction_id<br/>FK customer_id<br/>FK account_id<br/>FK branch_id<br/>FK date_key<br/>transaction_type<br/>amount<br/>currency")]

    C --> F
    A --> F
    B --> F
    D --> F
```

## Star Schema Structure

```text
<<<<<<< HEAD
                         ┌──────────────────────┐
                         │     dim_customer     │
                         │──────────────────────│
                         │ PK customer_id       │
                         │ customer_name        │
                         └──────────┬───────────┘
                                    │
                                    │
┌──────────────────────┐            │            ┌──────────────────────┐
│     dim_branch       │            │            │     dim_account      │
│──────────────────────│            │            │──────────────────────│
│ PK branch_id         │            │            │ PK account_id        │
│ branch_name          │            │            │ account_type         │
└──────────┬───────────┘            │            └──────────┬───────────┘
           │                        │                       │
           │                        │                       │
           └────────────────┐       │       ┌───────────────┘
                            │       │       │
                            ▼       ▼       ▼
                     ┌─────────────────────────┐
                     │    fact_transaction     │
                     │─────────────────────────│
                     │ PK transaction_id       │
                     │ FK customer_id          │
                     │ FK account_id           │
                     │ FK branch_id            │
                     │ FK date_key             │
                     │ transaction_type        │
                     │ amount                  │
                     │ currency                │
                     └────────────┬────────────┘
                                  │
                                  │
                         ┌────────▼───────────┐
                         │      dim_date      │
                         │────────────────────│
                         │ PK date_key        │
                         │ full_date          │
                         └────────────────────┘
```

## Tables

| Table | Type | Purpose |
|---|---|---|
| `fact_transaction` | Fact | Stores transaction-level analytical data |
| `dim_customer` | Dimension | Stores customer information |
| `dim_account` | Dimension | Stores account information |
| `dim_branch` | Dimension | Stores branch information |
| `dim_date` | Dimension | Stores transaction date information |

## Fact Table Grain

The grain of `fact_transaction` is:

> **One row represents one banking transaction identified by `transaction_id`.**

The fact table connects the transaction to the customer, account, branch, and date dimensions.

## Data Flow

```text
banking.db
    │
    │ Analytical Transformation
    ▼
analytics.db
    │
    ├── dim_customer
    ├── dim_account
    ├── dim_branch
    ├── dim_date
    │
    └── fact_transaction
```

## Why It Is a Star Schema

`fact_transaction` is the central fact table, while the four dimension tables surround it:

```text
             dim_customer
                  │
                  │
dim_branch ── fact_transaction ── dim_account
                  │
                  │
               dim_date
```

This structure makes analytical queries easier for questions such as:

- Transaction amount by branch
- Transaction amount by customer
- Transaction amount by account
- Daily transaction analysis
- Transaction type analysis
- Customer and branch analysis
=======
┌──────────────────────────────┐
│        Raw Branch CSVs       │
│        data/raw/             │
│  BR001 / BR002 / BR003       │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│          Ingestion           │
│     src/ingestion/           │
│                              │
│ Read CSV → Parse Records     │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│   Validation + Data Quality  │
│     validation.py            │
│     dq_metrics.py            │
│                              │
│ Valid → validated data       │
│ Invalid → DQ output          │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│       banking.db             │
│                              │
│ branches                     │
│ customers                    │
│ accounts                     │
│ transactions                 │
└──────────────┬───────────────┘
               │
               │ Daily transaction files
               │
               ▼
┌──────────────────────────────┐
│      Incremental Load        │
│ src/operational/             │
│ incremental_load.py          │
│                              │
│ INSERT new transaction IDs   │
│ UPDATE existing IDs          │
│ using UPSERT                 │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│     Updated banking.db       │
│                              │
│ Final transactions: 20       │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│   Analytical Transformation  │
│ src/analytics/               │
│ build_analytics.py           │
│                              │
│ Normalize dates              │
│ Build dimensions             │
│ Build transaction fact       │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│        analytics.db          │
│                              │
│ dim_branch                   │
│ dim_customer                 │
│ dim_account                  │
│ dim_date                     │
│ fact_transaction             │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│       Analytical SQL         │
│    sql/analytical_queries    │
│                              │
│ Aggregations                 │
│ CASE                         │
│ CTEs                         │
│ Window functions             │
│ Business questions           │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│       Business Results       │
│                              │
│ Transaction totals           │
│ Branch analysis              │
│ Customer/account activity    │
│ Daily analysis               │
│ Transaction classification   │
└──────────────────────────────┘
>>>>>>> 573e711 (Extend banking pipeline with Week 6 PySpark)
