CREATE DATABASE SupermarketSales;
ALTER SCHEMA public RENAME TO stage;

--Stage:
DROP TABLE IF EXISTS stage.kaggle_sales;
CREATE TABLE stage.kaggle_sales (
	invoice_id varchar(16) NULL,
	branch varchar(1) NULL,
	city varchar(16) NULL,
	customer_type varchar(8) NULL,
	gender varchar(8) NULL,
	product_line varchar(32) NULL,
	unit_price real NULL,
	quantity integer NULL,
	tax_5 real NULL,
	total real NULL,
	date_ varchar(16) NULL,
	time_ varchar(8) NULL,
	payment varchar(16) NULL,
	cogs real NULL,
	gross_margin_percentage real NULL,
	gross_income real NULL,
	rating real NULL
);


DROP TABLE IF EXISTS stage.kaggle_sales_ready;
SET DateStyle TO European;
SET DateStyle TO 'DMY';

CREATE TABLE stage.kaggle_sales_ready (
	invoice_id varchar(16) NULL,
	branch varchar(1) NULL,
	city varchar(16) NULL,
	customer_type varchar(8) NULL,
	gender varchar(8) NULL,
	product_line varchar(32) NULL,
	unit_price numeric(10, 2) NULL,
	quantity int4 NULL,
	tax_5 numeric(10, 2) NULL,
	total numeric(18, 2) NULL,
	"Date" date NULL,
	"Time" varchar(20) NULL,
	payment_type varchar(16) NULL,
	cogs numeric(18, 2) NULL,
	gross_margin_percentage float4 NULL,
	gross_income numeric(10, 2) NULL,
	rating numeric(4, 1) NULL,
	check_status int4 NULL,
	product_id int4 NULL
);

/*
INSERT INTO stage.kaggle_sales_ready	
SELECT invoice_id,
		branch,
		city ,
		customer_type ,
		gender ,
		product_line ,
		unit_price::decimal(10, 2)  AS unit_price ,
		quantity,
		tax_5:: decimal(10, 2) AS tax_5,
		total::decimal(18, 2),
		to_date(date_, 'MM/DD/YYYY')::date AS date_,
		time_::varchar(5),
		payment AS payment_type,
		cogs::decimal(18, 2),
		gross_margin_percentage,
		gross_income::decimal(10, 2) AS gross_income,
		rating::decimal(4, 1)
FROM stage.kaggle_sales;
 */

-- additional table for holidays and non-working days
DROP TABLE IF EXISTS stage.days_off;
CREATE TABLE stage.days_off (
	"date" VARCHAR(10),
	"type" VARCHAR(20)
);



-- nds:
DROP TABLE IF EXISTS nds.cities;
CREATE TABLE nds.cities (
	id SERIAL PRIMARY KEY,
	name VARCHAR(100)
);

/*
INSERT INTO nds.cities (name)
SELECT DISTINCT city
FROM stage.kaggle_sales_ready;
SELECT * FROM nds.cities;
*/


DROP TABLE IF EXISTS nds.branches;
CREATE TABLE nds.branches(
	id SERIAL PRIMARY KEY,
	name VARCHAR(2)
);

/*
INSERT INTO nds.branches (name)
SELECT DISTINCT branch
FROM stage.kaggle_sales_ready d;
SELECT * FROM nds.branches;
*/


DROP TABLE IF EXISTS nds.branches_cities;
CREATE TABLE nds.branches_cities (
	branch_id INT, city_id INT, 
	FOREIGN KEY (branch_id) REFERENCES branches(id),
	FOREIGN KEY (city_id) REFERENCES cities(id)
);

/*
INSERT INTO nds.branches_cities (branch_id, city_id)
SELECT b.id AS branch_id, c.id AS city_id
FROM (
	SELECT DISTINCT branch, city
	FROM stage.kaggle_sales_ready d
	) t
JOIN nds.cities c 
	ON c.name = t.city
JOIN nds.branches b
	ON b."name"  = t.branch;
SELECT * FROM nds.branches_cities;
*/


DROP TABLE IF EXISTS nds.payment_types;
CREATE TABLE payment_types (
	id SERIAL PRIMARY KEY,
	"type" VARCHAR(30)
);

/*
INSERT INTO payment_types ("type")
SELECT DISTINCT payment_type
FROM stage.kaggle_sales_ready;
SELECT * FROM nds.payment_types;
*/



DROP TABLE IF EXISTS nds.product_lines;
CREATE TABLE product_lines (
	id SERIAL PRIMARY KEY,
	"line" VARCHAR(100)
);

/*
INSERT INTO product_lines ("line")
SELECT DISTINCT product_line
FROM stage.kaggle_sales_ready;
SELECT * FROM nds.product_lines;
*/


DROP TABLE IF EXISTS nds.products;
CREATE TABLE  nds.products (
	id SERIAL PRIMARY KEY,
	"name" VARCHAR(100),
	unit_price DECIMAL(10, 2),
	line_id INT,
	FOREIGN KEY (line_id) REFERENCES nds.product_lines (id)
	);
CREATE INDEX n_idx ON nds.products (line_id);

/*
DO $$ 
DECLARE 
	prod_line RECORD;
	counter_for integer := 0;
	counter_while integer := 0;

BEGIN
	FOR prod_line IN
       SELECT "line" FROM nds.product_lines
    LOOP
		DECLARE
    	counter_while integer := 0;
	
    	BEGIN 
		WHILE counter_while <= (SELECT COUNT(DISTINCT unit_price) FROM stage.kaggle_sales_ready WHERE product_line = prod_line."line") LOOP
		
			INSERT INTO products ("name", unit_price, line_id)
			SELECT CONCAT('product_', counter_for) AS product_name, unit_price, pl.id AS product_line_id
			FROM stage.kaggle_sales_ready d
			JOIN nds.product_lines pl 
				ON pl.line = d.product_line 
			WHERE d.product_line  = prod_line."line"
			OFFSET counter_while
			LIMIT 1;
		
			counter_while := counter_while + 1;
			counter_for := counter_for + 1;
		END LOOP; 
		
		END;
	END LOOP;
END
$$;
*/


DROP TABLE IF EXISTS nds.customer_types;
CREATE TABLE nds.customer_types (
	id SERIAL PRIMARY KEY,
	"type" VARCHAR(100)
);

/*
INSERT INTO nds.customer_types ("type")
SELECT DISTINCT customer_type
FROM stage.kaggle_sales_ready;
SELECT * FROM nds.customer_types;
*/


DROP TABLE IF EXISTS nds.customers CASCADE;
CREATE TABLE customers (
	id SERIAL PRIMARY KEY,
	gender VARCHAR(6),
	customer_type_id INT,
	fio VARCHAR(200),
	login VARCHAR(50),
	FOREIGN KEY (customer_type_id) REFERENCES customer_types(id)
	);
CREATE INDEX customers_idx ON nds.customers (customer_type_id);
					
-- loading to the customers table has been proceeded from the .csv file. 


DROP TABLE IF EXISTS nds.orders;
CREATE TABLE nds.orders (
	order_id SERIAL PRIMARY KEY,
	invoice_id VARCHAR(16),
	product_id INT,
	quantity INT,
	customer_id INT,
	branch_id INT,
	order_date TIMESTAMP,
	FOREIGN KEY (product_id) REFERENCES products(id),
	FOREIGN KEY (customer_id) REFERENCES customers(id),
	FOREIGN KEY (branch_id) REFERENCES branches(id)
);

CREATE INDEX orders_idx_invoice_id ON nds.orders(invoice_id);
CREATE INDEX orders_idx_products ON nds.orders(product_id);
CREATE INDEX orders_idx_customers ON nds.orders(customer_id);
CREATE INDEX orders_idx_branches ON nds.orders(branch_id);

/*
TRUNCATE TABLE nds.orders CASCADE;
INSERT INTO nds.orders (invoice_id,
					product_id,
					quantity,
					customer_id,
					branch_id,
					order_date
					)
SELECT invoice_id,
		product_id,
		quantity,
		CEIL(random() * 10000) AS customer_id,
		b.id AS branch_id,
		to_timestamp("Date"|| ' ' || "Time", 'YYYY-MM-DD HH24:MI:SS') AS order_date
FROM stage.kaggle_sales_ready d
JOIN nds.branches b 
	ON b."name" = d.branch 
ORDER BY 5, 6;
SELECT * FROM nds.orders;
SELECT COUNT(*) FROM nds.orders;
*/


DROP TABLE IF EXISTS nds.payments;
CREATE TABLE nds.payments (
	order_id INT,
	tax_5 DECIMAL(10, 2),
	total DECIMAL(18, 2),
	payment_type_id INT,
	cogs DECIMAL(18, 2),
	gmp REAL,
	gross_income DECIMAL(10, 2),
	rating DECIMAL(4, 1),
	FOREIGN KEY (order_id) REFERENCES orders(order_id),
	FOREIGN KEY (payment_type_id) REFERENCES payment_types(id)
);

CREATE INDEX payments_idx_branches ON nds.payments(order_id) INCLUDE (tax_5, total, cogs, gross_income);
CREATE INDEX payments_idx_payment_types ON nds.payments(payment_type_id);
	
/*
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
	ON pt."type" = d.payment_type;
*/



DROP PROCEDURE stage.load_raw_data();
CREATE PROCEDURE stage.load_raw_data() AS $$

BEGIN 
	
	TRUNCATE TABLE stage.kaggle_sales;
	COPY stage.kaggle_sales FROM 'new_daily_data.csv' DELIMITER ',' CSV header;
INSERT INTO stage.kaggle_sales_ready	
	SELECT invoice_id,
			branch,
			city ,
			customer_type ,
			gender ,
			product_line ,
			unit_price::decimal(10, 2)  AS unit_price ,
			quantity,
			tax_5:: decimal(10, 2) AS tax_5,
			total::decimal(18, 2),
			to_date(date_, 'MM/DD/YYYY')::date AS date_,
			time_,
			payment AS payment_type,
			cogs::decimal(18, 2),
			gross_margin_percentage,
			gross_income::decimal(10, 2) AS gross_income,
			rating::decimal(4, 1)
	FROM stage.kaggle_sales;

END;
$$ LANGUAGE plpgsql;

-- SP for loading the raw data from .csv-file
CREATE OR REPLACE PROCEDURE stage.load_raw_data()
 LANGUAGE plpgsql
AS $procedure$

BEGIN 
	
	TRUNCATE TABLE stage.kaggle_sales;
	TRUNCATE TABLE stage.kaggle_sales_ready;
	COPY stage.kaggle_sales FROM 'new_daily_data.csv' DELIMITER ',' CSV header;
INSERT INTO stage.kaggle_sales_ready	
	SELECT invoice_id,
			branch,
			city ,
			customer_type ,
			gender ,
			product_line ,
			unit_price::decimal(10, 2)  AS unit_price ,
			quantity,
			tax_5:: decimal(10, 2) AS tax_5,
			total::decimal(18, 2),
			to_date(date_, 'MM/DD/YYYY')::date AS date_,
			time_,
			payment AS payment_type,
			cogs::decimal(18, 2),
			gross_margin_percentage,
			gross_income::decimal(10, 2) AS gross_income,
			rating::decimal(4, 1)
	FROM stage.kaggle_sales;

END;
$procedure$
;

-- CALL stage.load_raw_data();
