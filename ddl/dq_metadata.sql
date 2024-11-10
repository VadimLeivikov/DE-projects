CREATE SCHEMA dq;
CREATE SCHEMA metadata;

-- DQ:
DROP TABLE IF EXISTS dq.issues;
CREATE TABLE dq.issues(
	id SERIAL PRIMARY KEY,
	"desc" VARCHAR(200)
);

INSERT INTO dq.issues ("desc") VALUES ('incorrect invoice'),
('incorrect unit_price'),
('incorrect quantity'),
('incorrect tax_5'),
('incorrect raiting'),
('incorrect time'),
('number of orders is more than 3*daily'),
('there are more than 1 customer for 1 invoice_id'),
('cogs was not calculated accurately');


DROP TABLE IF EXISTS dq.stage_kaggle_sales;
CREATE TABLE dq.stage_kaggle_sales (
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
	issue_id INT NULL,
	CONSTRAINT fk_issues FOREIGN KEY (issue_id) REFERENCES dq.issues(id)
);

		
DROP TABLE IF EXISTS dq.nds_orders;
CREATE TABLE dq.nds_orders (
	order_date DATE NOT NULL,
	num_orders INT NULL,
	cogs_from_nds DECIMAL(18, 2) NULL,
	cogs_check_sum DECIMAL(18, 2) NULL,
	issue_id INT NULL,
	CONSTRAINT fk_issues FOREIGN KEY (issue_id) REFERENCES dq.issues(id)
);

	
DROP TABLE IF EXISTS dq.nds_invoice_customers;
CREATE TABLE dq.nds_invoice_customers (
	invoice_id varchar(16) NULL,
	order_date timestamptz NULL,
	num_customers int8 NULL,
	issue_id INT NULL,
	CONSTRAINT fk_issues FOREIGN KEY (issue_id) REFERENCES dq.issues(id);


DROP TABLE IF EXISTS dq.dds_trends;
CREATE TABLE dq.dds_trends (check_date DATE,
							avg_num_orders INT NOT NULL,
							delta_avg_num_orders INT NOT NULL,
							avg_total INT,
							delta_avg_total INT)

							
-- metadata:							
DROP TABLE IF EXISTS metadata.etl_status;  
CREATE TABLE metadata.etl_status (
    id SERIAL PRIMARY KEY,
    dag_id TEXT,
    task_id TEXT,
    execution_date TIMESTAMP,
    status TEXT,
    rows_processed INT,
    target_table TEXT,
    last_row_count INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX md_idx_date_load ON metadata.etl_status(CAST(execution_date AS date));
CREATE INDEX md_idx_date_target ON metadata.etl_status(target_table);


