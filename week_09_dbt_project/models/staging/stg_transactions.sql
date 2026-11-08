
SELECT
    CAST(transaction_id AS VARCHAR) AS transaction_id,
    CAST(account_id AS VARCHAR) AS account_id,
    CAST(transaction_date AS DATE) AS transaction_date,
    CAST(amount AS DECIMAL(18, 2)) AS amount,
    UPPER(TRIM(transaction_type)) AS transaction_type,
    UPPER(TRIM(currency)) AS currency

FROM {{ ref('transactions') }}
