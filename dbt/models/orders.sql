{{ config(materialized='incremental', unique_key='invoice_id') }}

SELECT 
    invoice_id,
    CEIL(random() * 997) AS product_id,
    quantity,
    CEIL(random() * 10000) AS customer_id,
    b.id AS branch_id,
    ("Date" || ' ' || "Time") AS order_date
FROM stage.kaggle_sales_ready d
JOIN nds.branches b ON b."name" = d.branch
WHERE NOT EXISTS (
    SELECT 1 
    FROM dbt.orders o 
    WHERE o.invoice_id = d.invoice_id
)
AND NOT EXISTS (
    SELECT 1 
    FROM {{ ref('bad_dates') }} bd 
    WHERE d."Date" = bd.bad_date
)

{% if is_incremental() %}
    -- Conditions for incremental loading
    AND d."Date" > (SELECT MAX(LEFT(order_date, 10)::DATE) FROM dbt.orders)
{% endif %}
ORDER BY 5, 6

