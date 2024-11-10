
DROP TABLE IF EXISTS dim.dates;
CREATE TABLE dim.dates
AS
WITH cte_dates AS (
    SELECT dd::date AS dt
    FROM generate_series
            ('2019-01-01'::timestamp
            ,'2024-12-31'::timestamp
            ,'1 day'::interval) dd
)
SELECT
    to_char(dt, 'YYYYMMDD')::int AS id,
    dt AS date,
    to_char(dt, 'YYYY-MM-DD') AS ansi_date,
    date_part('isodow', dt)::int AS day,
    date_part('week', dt)::int AS week_number,
    date_part('month', dt)::int AS month,
    date_part('isoyear', dt)::int AS year,
    (date_part('isodow', dt)::smallint BETWEEN 1 AND 5)::int AS week_day,
    CASE 
    	WHEN dt IN (SELECT "date"::date FROM stage.days_off WHERE "type" = 'holiday' )
    	THEN TRUE 
    END AS holiday,
    CASE 
    	WHEN dt IN (SELECT "date"::date FROM stage.days_off WHERE "type" = 'preholiday' )
    	THEN TRUE 
    END AS preholiday,
    CASE 
    	WHEN dt IN (SELECT "date"::date FROM stage.days_off WHERE "type" = 'nowork' )
    	THEN TRUE 
    END AS nowork
FROM cte_dates
ORDER BY dt;
ALTER TABLE dim.dates ADD PRIMARY KEY (id);
CLUSTER dim.dates USING dates_pkey;
CREATE INDEX dates_dt_idx ON dim.dates("date");
CREATE INDEX month_dt_idx ON dim.dates("month");
CREATE INDEX year_dt_idx ON dim.dates("year");


DROP TABLE IF EXISTS fact.sales_items;
CREATE TABLE fact.sales_items (
	date_key INT NULL REFERENCES dim.dates(id),
	transaction_date TIMESTAMP NULL,
	order_id INT NULL REFERENCES nds.orders(order_id),
	customer_key INT NULL REFERENCES dim.customers(id),
	payment_type_id INT NULL REFERENCES dim.payment_types(id),
	branch_id INT NULL,
	city_id INT NULL,
	product_key INT NULL REFERENCES dim.products(id),
	quantity INT NULL,
	cogs DECIMAL(18, 2) NULL,
	tax_5 DECIMAL(10, 2) NULL,
	total DECIMAL(18, 2),
	gmp REAL NULL,
	gross_income DECIMAL(10, 2) NULL,
	rating DECIMAL(4, 1) NULL,
	FOREIGN KEY (branch_id, city_id) REFERENCES dim.functional_structure(branch_id, city_id)
);
CREATE INDEX fact_idx_date_ ON fact.sales_items (date_key, order_id) INCLUDE (cogs, tax_5, total, gross_income);
CREATE INDEX fact_idx_trans_date ON fact.sales_items (transaction_date);
CREATE INDEX fact_idx_date_order_id ON fact.sales_items (order_id);
CREATE INDEX fact_idx_date_cust_key ON fact.sales_items (customer_key);
CREATE INDEX fact_idx_date_ptype_id ON fact.sales_items (payment_type_id);
CREATE INDEX fact_idx_date_prod_key ON fact.sales_items (product_key);
CREATE INDEX fact_idx_date_rating ON fact.sales_items (rating);

/*
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
	ON cte.order_id = o.order_id; 
*/



DROP TABLE IF EXISTS dim.branches;
CREATE TABLE dim.branches (
	id INT PRIMARY KEY NOT NULL,
	name VARCHAR(100) NOT NULL
);
CREATE INDEX dim_idx_branches ON dim.branches (id);

/*
INSERT INTO dim.branches
SELECT *
FROM nds.branches;
*/


DROP TABLE IF EXISTS dim.cities;
CREATE TABLE dim.cities (
	id INT PRIMARY KEY NOT NULL,
	name  VARCHAR(100) NOT NULL
);
CREATE INDEX dim_idx_cities ON dim.cities (id);

/*
INSERT INTO dim.cities
SELECT *
FROM nds.cities;
*/


DROP TABLE IF EXISTS dim.branches_cities;
CREATE TABLE dim.branches_cities (
	branch_id INT,
	city_id INT
);

/*
INSERT INTO dim.branches_cities
SELECT *
FROM nds.branches_cities;
*/

DROP TABLE IF EXISTS dim.functional_structure;
CREATE TABLE dim.functional_structure (
	branch_id INT NOT NULL,
	branch VARCHAR(2) NOT NULL,
	city_id INT NOT NULL,
	city VARCHAR(100) NOT NULL,
	UNIQUE (city_id),
	PRIMARY KEY (branch_id, city_id)
);
CREATE INDEX dim_idx_fs ON dim.functional_structure (city_id);

/*
INSERT INTO dim.functional_structure
SELECT bs.branch_id,
		b."name" AS branch,
		bs.city_id,
		c."name" AS city
FROM nds.branches_cities bs
LEFT JOIN nds.branches b
	ON b.id = bs.branch_id 
LEFT JOIN nds.cities c
	ON c.id = bs.city_id;
*/

DROP TABLE IF EXISTS dim.products;
CREATE TABLE dim.products (
	id INT PRIMARY KEY NOT NULL,
	product_name VARCHAR(100) NOT NULL,
	unit_price DECIMAL(10, 2) NOT NULL,
	product_line VARCHAR(100) NULL
);
CREATE INDEX dim_idx_products ON dim.products (id);

/*
INSERT INTO dim.products
SELECT p.id AS product_id,
		p."name"  AS product_name,
		p.unit_price,
		pl.line AS product_line
FROM nds.products p
JOIN nds.product_lines pl
	ON pl.id  = p.line_id; 
*/


DROP TABLE IF EXISTS dim.payment_types;
CREATE TABLE dim.payment_types (
	id INT PRIMARY KEY NOT NULL,
	payment_type VARCHAR(30) NOT NULL
);
CREATE INDEX dim_idx_payment_types ON dim.payment_types (id);

/*
INSERT INTO dim.payment_types
SELECT id AS payment_type_id,
		"type"  AS payment_type
FROM nds.payment_types;
*/


DROP TABLE IF EXISTS dim.customers;
CREATE TABLE dim.customers (
	id SERIAL PRIMARY KEY NOT NULL,
	customer_id INT NOT NULL,
	customer_type VARCHAR(100) NULL,
	gender VARCHAR(6) NULL,
	fio VARCHAR(200) NULL,
	login VARCHAR(50) NOT NULL,
	active_from INT NOT NULL DEFAULT (20190101),
	active_to INT NOT NULL DEFAULT (20990101)
);
CREATE INDEX dim_idx_customers_main ON dim.customers (id) INCLUDE (gender, fio, login);
CREATE INDEX dim_idx_customers ON dim.customers (customer_id);
CREATE INDEX dim_idx_active ON dim.customers (active_from, active_to);

/*
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
	ON ce.customer_id = c.id ;
*/

