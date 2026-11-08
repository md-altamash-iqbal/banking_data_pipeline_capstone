
SELECT
    CAST(customer_id AS VARCHAR) AS customer_id,
    TRIM(customer_name) AS customer_name,
    TRIM(city) AS city,
    TRIM(customer_segment) AS customer_segment

FROM {{ ref('customers') }}
