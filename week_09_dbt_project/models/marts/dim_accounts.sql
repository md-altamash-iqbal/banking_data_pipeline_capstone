
SELECT
    a.account_id,
    a.customer_id,
    c.customer_name,
    c.city,
    c.customer_segment,
    a.account_type,
    a.opened_date,
    a.account_status

FROM {{ ref('stg_accounts') }} AS a

INNER JOIN {{ ref('stg_customers') }} AS c
    ON a.customer_id = c.customer_id
