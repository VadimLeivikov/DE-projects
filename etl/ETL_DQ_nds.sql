-- Simulating the situation where the cases identified in the previous DQ check have been resolved, and the tables are clean:
TRUNCATE TABLE dq.nds_orders;
TRUNCATE TABLE dq.nds_invoice_customers;

--DQ ORDERS:

-- Looking for abnormal outliers in the number of orders per day:
WITH max_order_num AS (SELECT 3 *FLOOR(AVG(cnt)) AS max_order_limit
FROM (
	SELECT order_date::date, COUNT(order_id) AS cnt
	FROM nds.orders
	GROUP BY order_date::date
	)t
					)
INSERT INTO dq.nds_orders (order_date, num_orders, issue_id)
SELECT to_timestamp("Date"|| ' ' || "Time", 'YYYY-MM-DD HH24:MI:SS') AS order_date, 
		COUNT(invoice_id) AS num_orders,
		7 AS issue_id  --'number of orders is more than 3*daily'
FROM stage.kaggle_sales_ready d
JOIN nds.branches b 
	ON b."name" = d.branch
LEFT JOIN max_order_num
	ON 1 = 1 
WHERE NOT EXISTS (SELECT 1 FROM nds.orders o WHERE o.invoice_id = d.invoice_id)
GROUP BY order_date,max_order_limit
HAVING COUNT(invoice_id) >= max_order_limit;


UPDATE stage.kaggle_sales_ready st
SET check_status = 2 -- DELETE 
WHERE EXISTS (SELECT 1 FROM dq.nds_orders dq WHERE dq.order_date::date = st."Date"::date);

DELETE FROM stage.kaggle_sales_ready
WHERE check_status = 2;

UPDATE stage.kaggle_sales_ready
SET check_status = 1; -- CHECKED

	

-- "1 invoice = 1 customer" check:

INSERT INTO dq.nds_invoice_customers
SELECT invoice_id, 
		order_date, 
		row_num AS num_customers,
		8 AS issue_id --'there are more than 1 customer for 1 invoice_id'
FROM (
	SELECT invoice_id,
		quantity,
		b.id AS branch_id,
		to_timestamp("Date"|| ' ' || "Time", 'YYYY-MM-DD HH24:MI:SS') AS order_date,
		ROW_NUMBER() OVER (PARTITION BY invoice_id ORDER BY invoice_id) AS row_num
	FROM stage.kaggle_sales_ready d
	JOIN nds.branches b 
		ON b."name" = d.branch 
	WHERE check_status = 1
	) t
WHERE row_num > 1
ORDER BY invoice_id, order_date;	

UPDATE stage.kaggle_sales_ready st
SET check_status = 2 -- DELETE 
WHERE EXISTS (SELECT 1 FROM dq.nds_invoice_customers dq WHERE dq.invoice_id  = st.invoice_id);

DELETE FROM stage.kaggle_sales_ready
WHERE check_status = 2;

UPDATE stage.kaggle_sales_ready
SET check_status = 1; -- CHECKED


-- transfering cleaned data to "nds" layer:
INSERT INTO nds.orders (invoice_id,
					product_id,
					quantity,
					customer_id,
					branch_id,
					order_date
					)
SELECT invoice_id,
		CEIL(random() * 997) AS product_id,
		quantity,
		CEIL(random() * 10000) AS customer_id,
		b.id AS branch_id,
		to_timestamp("Date"|| ' ' || "Time", 'YYYY-MM-DD HH24:MI:SS') AS order_date
FROM stage.kaggle_sales_ready d
JOIN nds.branches b 
	ON b."name" = d.branch 
WHERE NOT EXISTS (SELECT 1 FROM nds.orders o WHERE o.invoice_id = d.invoice_id)
		AND check_status = 1
ORDER BY 5, 6;


INSERT INTO nds.payments 
SELECT o.order_id,
		tax_5,
		total,
		pt.id AS payment_type_id,
		cogs,
		gross_margin_percentage AS gmp,
		gross_income,
		rating
FROM stage.kaggle_sales_ready d 
JOIN nds.orders o 	
	ON o.invoice_id  = d.invoice_id 
JOIN nds.payment_types pt 
	ON pt."type" = d.payment_type
WHERE d.check_status = 1
		AND NOT EXISTS (SELECT 1 FROM nds.payments p WHERE p.order_id = o.order_id);






















