# CDP Readiness Extension – Privacy and Design

## 1. Purpose

This document explains the customer identity model, data privacy decisions, design limitations, and security considerations for the CDP Readiness Extension.

The project demonstrates a local Customer 360 data product using synthetic banking data. It is not a production Customer Data Platform.

## 2. Customer Identity

The project uses `customer_id` as the unique customer identity key.

- Each customer has one unique `customer_id`.
- Each account has one unique `account_id`.
- Each account references a customer through `customer_id`.
- Each transaction references an account through `account_id`.

Customer records are not merged based only on customer names. Names are descriptive attributes, not identity keys.

## 3. Data Grain

| Dataset | Grain |
|---|---|
| Customers | One record per customer |
| Accounts | One record per account |
| Transactions | One record per transaction |
| Customer 360 | One record per customer |

The Customer 360 Gold dataset provides account and transaction summary metrics at customer level.

## 4. Sensitive Data Excluded

The project uses synthetic customer names and identifiers.

It does not use or expose:

- Real customer names or contact details.
- Phone numbers or email addresses.
- Residential addresses.
- Authentication credentials.
- Full transaction histories through the API.
- Production banking account identifiers.

## 5. Identity Matching Limitations

The current implementation uses deterministic identifier-based relationships.

It does not implement:

- Fuzzy name matching.
- Probabilistic identity resolution.
- Duplicate-person detection across systems.
- Household or family relationship matching.
- Master data management.

A production identity-resolution process would require approved matching rules, governance, and human review for uncertain matches.

## 6. API Security

The local API:

- Uses HTTP GET for customer summary retrieval.
- Does not provide write endpoints.
- Binds to `127.0.0.1`.
- Uses parameterized SQL for customer lookup.
- Returns HTTP 404 for an unknown customer.

The project does not implement production authentication, authorization, rate limiting, or centralized audit logging.

## 7. Production Controls to Consider

A real banking implementation would require:

- Authentication and role-based authorization.
- Consent and purpose-based data access.
- Encryption in transit and at rest.
- Audit logging and monitoring.
- Rate limiting and abuse protection.
- Data retention and deletion policies.
- Access reviews and security testing.
- Regulatory and privacy compliance checks.

These controls are recommendations for a production environment, not implemented features of this demonstration.

## 8. Limitations

- The data is synthetic and limited in volume.
- The API runs locally and is not deployed.
- Identity matching relies on exact identifiers.
- No real-time event streaming is implemented.
- No external CDP platform is connected.
- No production authentication or authorization is implemented.

## 9. Conclusion

This extension demonstrates customer-level data integration, deterministic identity relationships, data-quality exception handling, reproducible Customer 360 aggregation, and read-only API access while keeping the implementation limited to synthetic data and local development.