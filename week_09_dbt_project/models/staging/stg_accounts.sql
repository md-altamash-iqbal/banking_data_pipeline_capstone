
SELECT
    CAST(account_id AS VARCHAR) AS account_id,
    CAST(customer_id AS VARCHAR) AS customer_id,
    TRIM(account_type) AS account_type,
    CAST(opened_date AS DATE) AS opened_date,
    UPPER(TRIM(account_status)) AS account_status

FROM {{ ref('accounts') }}
