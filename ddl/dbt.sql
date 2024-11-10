
ALTER TABLE dbt.orders OWNER TO dbt_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE dbt.orders TO dbt_user;

DROP TABLE IF EXISTS dbt.bad_dates;
CREATE TABLE dbt.bad_dates (
	bad_date DATE NULL,
	max_order_limit NUMERIC NULL,
	fact_orders_num INT8 NULL
);


DROP TABLE  IF EXISTS dbt.orders;
CREATE TABLE dbt.orders (
	invoice_id VARCHAR(16) NULL,
	product_id FLOAT8 NULL,
	quantity INT4 NULL,
	customer_id FLOAT8 NULL,
	branch_id INT4 NULL,
	order_date TEXT NULL
);


DROP TABLE  IF EXISTS dbt.orders_snapshot;
CREATE TABLE dbt.orders_snapshot (
	invoice_id VARCHAR(16) NULL,
	product_id FLOAT8 NULL,
	quantity INT4 NULL,
	customer_id FLOAT8 NULL,
	branch_id INT4 NULL,
	order_date TEXT NULL,
	dbt_scd_id TEXT NULL,
	dbt_updated_at TIMESTAMP NULL,
	dbt_valid_from TIMESTAMP NULL,
	dbt_valid_to TIMESTAMP NULL
);


