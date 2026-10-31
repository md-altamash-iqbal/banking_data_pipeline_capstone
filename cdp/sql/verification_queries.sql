-- Q1. Total customers in Customer 360
SELECT COUNT(*) AS total_customers
FROM customer_360;


-- Q2. Check for duplicate customer IDs
SELECT
    customer_id,
    COUNT(*) AS duplicate_count
FROM customer_360
GROUP BY customer_id
HAVING COUNT(*) > 1;


-- Q3. Customers with no accounts or transactions
SELECT
    customer_id,
    customer_name,
    account_count,
    transaction_count,
    total_transaction_amount,
    last_transaction_date
FROM customer_360
WHERE account_count = 0
   OR transaction_count = 0;


-- Q4. Verify customer account counts
SELECT
    c.customer_id,
    c.customer_name,
    COUNT(a.account_id) AS actual_account_count,
    g.account_count AS gold_account_count
FROM customers c
LEFT JOIN accounts a
    ON c.customer_id = a.customer_id
JOIN customer_360 g
    ON c.customer_id = g.customer_id
GROUP BY
    c.customer_id,
    c.customer_name,
    g.account_count
ORDER BY c.customer_id;


-- Q5. Verify transaction counts and totals
-- Aggregate transactions per customer before joining to Gold.
WITH transaction_metrics AS (
    SELECT
        a.customer_id,
        COUNT(t.transaction_id) AS actual_transaction_count,
        COALESCE(SUM(t.amount), 0) AS actual_total_amount,
        MAX(t.transaction_date) AS actual_last_transaction_date
    FROM accounts a
    LEFT JOIN transactions t
        ON a.account_id = t.account_id
    GROUP BY a.customer_id
)
SELECT
    g.customer_id,
    g.transaction_count AS gold_transaction_count,
    COALESCE(tm.actual_transaction_count, 0) AS actual_transaction_count,
    g.total_transaction_amount AS gold_total_amount,
    COALESCE(tm.actual_total_amount, 0) AS actual_total_amount,
    g.last_transaction_date AS gold_last_date,
    tm.actual_last_transaction_date AS actual_last_date
FROM customer_360 g
LEFT JOIN transaction_metrics tm
    ON g.customer_id = tm.customer_id
ORDER BY g.customer_id;


-- Q6. Check for accounts referencing unknown customers
SELECT
    a.account_id,
    a.customer_id
FROM accounts a
LEFT JOIN customers c
    ON a.customer_id = c.customer_id
WHERE c.customer_id IS NULL;


-- Q7. Check for transactions referencing unknown accounts
SELECT
    t.transaction_id,
    t.account_id
FROM transactions t
LEFT JOIN accounts a
    ON t.account_id = a.account_id
WHERE a.account_id IS NULL;


-- Q8. Review recorded data-quality exceptions
SELECT
    source_table,
    reason,
    COUNT(*) AS exception_count
FROM exception_records
GROUP BY source_table, reason
ORDER BY source_table, reason;