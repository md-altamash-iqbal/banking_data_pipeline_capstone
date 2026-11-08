
SELECT
    t.transaction_id,
    t.account_id,
    a.customer_id,
    c.customer_name,
    c.city,
    c.customer_segment,

    t.transaction_date,
    t.transaction_type,
    t.amount,
    t.currency,

    CASE
        WHEN t.transaction_type = 'DEPOSIT'
            THEN t.amount

        WHEN t.transaction_type IN ('PURCHASE', 'WITHDRAWAL')
            THEN -t.amount

        ELSE NULL
    END AS signed_amount

FROM {{ ref('stg_transactions') }} AS t

INNER JOIN {{ ref('stg_accounts') }} AS a
    ON t.account_id = a.account_id

INNER JOIN {{ ref('stg_customers') }} AS c
    ON a.customer_id = c.customer_id
