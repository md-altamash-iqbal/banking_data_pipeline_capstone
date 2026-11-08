
SELECT
    transaction_id,
    account_id,
    amount

FROM {{ ref('stg_transactions') }}

WHERE amount < 0
