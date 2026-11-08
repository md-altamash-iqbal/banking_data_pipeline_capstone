
SELECT
    transaction_date,
    account_id,

    COUNT(transaction_id) AS transaction_count,

    SUM(
        CASE
            WHEN transaction_type = 'DEPOSIT'
                THEN amount
            ELSE 0
        END
    ) AS total_deposits,

    SUM(
        CASE
            WHEN transaction_type IN ('PURCHASE', 'WITHDRAWAL')
                THEN amount
            ELSE 0
        END
    ) AS total_outflows,

    SUM(signed_amount) AS net_amount

FROM {{ ref('int_transaction_enriched') }}

GROUP BY
    transaction_date,
    account_id
