
CREATE TEMP TABLE temp_results (
    id SERIAL,
	avg_orders_num INT,
    avg_total INT,
    max_date DATE,
    min_date DATE
		);

DO $$
BEGIN

-- Calculating the average number of orders at the time of data loading:
INSERT INTO temp_results (avg_orders_num)
SELECT CEIL(AVG(order_count)) 
 FROM (
	SELECT date_key, COUNT(order_id) AS order_count
	FROM fact.sales_items
	GROUP BY date_key
	) t;

 -- Calculating the average order amount
UPDATE temp_results
SET avg_total = (SELECT CEIL(AVG(total_agg)) 
				    FROM (
					SELECT date_key, SUM(total) AS total_agg
					FROM fact.sales_items
					GROUP BY date_key
						) t
				);
			
UPDATE temp_results
SET max_date = (SELECT MAX(o.order_date::date)
				FROM nds.orders o 
				WHERE NOT EXISTS (SELECT 1 FROM fact.sales_items f WHERE f.transaction_date::date = o.order_date::date
														AND f.order_id = o.order_id
														)
												);
											
UPDATE temp_results
SET min_date = (SELECT MIN(o.order_date::date)
				FROM nds.orders o 
				WHERE NOT EXISTS (SELECT 1 FROM fact.sales_items f WHERE f.transaction_date::date = o.order_date::date
														AND f.order_id = o.order_id
														)
												);
			
END;
$$;

		
-- data transfer from "nds" to "dds":
INSERT INTO fact.sales_items
WITH cte AS (SELECT o.order_id, p.unit_price * o.quantity AS cogs
			FROM nds.orders o
			LEFT JOIN nds.products p
				ON p.id = o.product_id 
				)				
SELECT d.id AS date_key,
		o.order_date AS transaction_date,
		o.order_id,
		c2.id AS customer_id, 
		p.payment_type_id,
		b.id AS branch_id,
		c.id AS city_id,
		product_id,
		o.quantity,
		cte.cogs,
		(cte.cogs * 0.05) AS tax_5,
		(cte.cogs * 1.05) AS total,
		4.761905::REAL AS gmp,
		(cte.cogs * 0.05) AS gross_income,
		p.rating
FROM nds.payments p
LEFT JOIN nds.orders o 
	ON o.order_id  = p.order_id 
LEFT JOIN nds.customers c1
	ON c1.id = o.customer_id 
JOIN dim.customers c2 
	ON c2.customer_id = c1.id 
LEFT JOIN dim.dates d
	ON d."date" = o.order_date::date
LEFT JOIN dim.branches b 
	ON b.id = o.branch_id 
JOIN dim.branches_cities bs 
	ON bs.branch_id = b.id 
JOIN nds.cities c 
	ON c.id = bs.city_id 
JOIN cte 
	ON cte.order_id = o.order_id
WHERE NOT EXISTS (SELECT 1 FROM fact.sales_items f WHERE f.date_key = d.id
														AND f.order_id = o.order_id
														); 
												
INSERT INTO dim.products
SELECT p.id AS product_id,
		p."name"  AS product_name,
		p.unit_price,
		pl.line AS product_line
FROM nds.products p
JOIN nds.product_lines pl
	ON pl.id  = p.line_id
WHERE NOT EXISTS (SELECT 1 FROM dim.products p2 WHERE p2.id = p.id);


INSERT INTO dim.payment_types
SELECT id AS payment_type_id,
		"type"  AS payment_type
FROM nds.payment_types pt
WHERE NOT EXISTS (SELECT 1 FROM dim.payment_types pt2 WHERE pt2.id = pt.id);


INSERT INTO dim.functional_structure
SELECT bs.branch_id,
		b."name" AS branch,
		bs.city_id,
		c."name" AS city
FROM nds.branches_cities bs
LEFT JOIN nds.branches b
	ON b.id = bs.branch_id 
LEFT JOIN nds.cities c
	ON c.id = bs.city_id
WHERE NOT EXISTS (SELECT 1 FROM dim.functional_structure f WHERE f.branch_id = bs.branch_id AND f.city_id = bs.city_id);


INSERT INTO dim.customers (
	customer_id,
	customer_type,
	gender,
	fio,
	login,
	active_from)
WITH customer_entry AS (
	SELECT DISTINCT customer_id, first_order_date
	FROM (
		SELECT customer_id, order_date, FIRST_VALUE (order_date) OVER (PARTITION BY customer_id) AS first_order_date
		FROM nds.orders
		) t
				)
SELECT c.id AS nds_customer_id,
		ct."type" AS customer_type,
		c.gender,
		c.fio,
		c.login,
		COALESCE(to_char(ce.first_order_date, 'YYYYMMDD')::int, 20190101) AS active_from
FROM nds.customers c
JOIN nds.customer_types ct
	ON ct.id = c.customer_type_id
LEFT JOIN customer_entry ce
	ON ce.customer_id = c.id 
WHERE NOT EXISTS (SELECT 1 FROM dim.customers c2 WHERE c2.customer_id = c.id);
												
													
-- DQ checks in this layer are performed after loading:

DO $$
BEGIN

-- Calculating the average number of orders considering the inserted data:
INSERT INTO temp_results (avg_orders_num)
SELECT CEIL(AVG(order_count)) 
 FROM (
	SELECT date_key, COUNT(order_id) AS order_count
	FROM fact.sales_items
	GROUP BY date_key
	) t;

 -- Calculating the average order amount considering the inserted data:
UPDATE temp_results
SET avg_total = (SELECT CEIL(AVG(total_agg)) 
				    FROM (
					SELECT date_key, SUM(total) AS total_agg
					FROM fact.sales_items
					GROUP BY date_key
						) t
				);
			
UPDATE temp_results
SET max_date = (SELECT MAX(o.order_date::date)
				FROM nds.orders o 
				WHERE NOT EXISTS (SELECT 1 FROM fact.sales_items f WHERE f.transaction_date::date = o.order_date::date
														AND f.order_id = o.order_id
														)
												);
											
UPDATE temp_results
SET min_date = (SELECT MIN(o.order_date::date)
				FROM nds.orders o 
				WHERE NOT EXISTS (SELECT 1 FROM fact.sales_items f WHERE f.transaction_date::date = o.order_date::date
														AND f.order_id = o.order_id
														)
												);
			
END;
$$;

INSERT INTO dq.dds_trends
SELECT current_date, 
		avg_orders_num,
		(-avg_orders_num + LEAD(avg_orders_num) OVER ()) AS delta_orders_num,
		avg_total,
		(-avg_total + LEAD(avg_total) OVER ()) AS delta_avg_total
FROM temp_results
LIMIT 1;

-- It is possible to monitor deviations from the trend line:
/*
SELECT * FROM dq.dds_trends;	
DROP TABLE temp_results;
*/						