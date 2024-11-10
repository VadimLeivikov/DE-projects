{{ config(materialized='table') }}


WITH max_order_num AS (
    SELECT 2 * FLOOR(AVG(cnt)) AS max_order_limit
    FROM (
        SELECT order_date::date, COUNT(order_id) AS cnt
        FROM nds.orders
        GROUP BY order_date::date
    ) t
)
SELECT
    d."Date" AS bad_date,
    max_order_limit,
    COUNT(d.invoice_id)
FROM stage.kaggle_sales_ready d
JOIN nds.branches b ON b."name" = d.branch
LEFT JOIN max_order_num ON 1 = 1
WHERE NOT EXISTS (SELECT 1 FROM dbt.orders o WHERE o.invoice_id = d.invoice_id)
GROUP BY
    d."Date",
    max_order_limit
HAVING COUNT(d.invoice_id) >= max_order_limit
