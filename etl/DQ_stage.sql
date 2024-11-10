-- Enhancement of the source dataset - adding the product_id attribute and updating unit_price and related attributes (for passing DQ checks):
UPDATE stage.kaggle_sales_ready 
SET product_id = CASE WHEN ABS(FLOOR(RANDOM() * 1000)-3) = 0 THEN 1
					ELSE ABS(FLOOR(RANDOM() * 1000)-3)
				END
WHERE check_status IS NULL;

UPDATE stage.kaggle_sales_ready st
SET unit_price = p.unit_price 
FROM nds.products p
WHERE p.id = st.product_id;


UPDATE stage.kaggle_sales_ready st
SET tax_5 = 0.05 * quantity * unit_price,
	cogs = quantity * unit_price;

UPDATE stage.kaggle_sales_ready st
SET total = cogs + tax_5,
	gross_income  = tax_5;
	

-- DQ:
INSERT INTO dq.stage_kaggle_sales
SELECT invoice_id,branch,city,customer_type,gender,product_line,unit_price,quantity,
		tax_5,total,"Date","Time",payment_type,cogs,gross_margin_percentage,gross_income,rating,
		1 AS issue_id --'incorrect invoice'
FROM stage.kaggle_sales_ready
WHERE invoice_id NOT SIMILAR TO '\d{3}-\d{2}-\d{4}'
	AND check_status IS NULL;


INSERT INTO dq.stage_kaggle_sales
SELECT invoice_id,branch,city,customer_type,gender,product_line,unit_price,quantity,
		tax_5,total,"Date","Time",payment_type,cogs,gross_margin_percentage,gross_income,rating,
		2 AS issue_id --'incorrect unit_price'
FROM stage.kaggle_sales_ready
WHERE unit_price <= 0
	AND check_status IS NULL;

	
	
INSERT INTO dq.stage_kaggle_sales
SELECT invoice_id,branch,city,customer_type,gender,product_line,unit_price,quantity,
		tax_5,total,"Date","Time",payment_type,cogs,gross_margin_percentage,gross_income,rating,
		3 AS issue_id --'incorrect quantity'
FROM stage.kaggle_sales_ready
WHERE quantity <= 0
	AND check_status IS NULL;

INSERT INTO dq.stage_kaggle_sales
SELECT invoice_id,branch,city,customer_type,gender,product_line,unit_price,quantity,
		tax_5,total,"Date","Time",payment_type,cogs,gross_margin_percentage,gross_income,rating,
		4 AS issue_id --'incorrect tax_5'
FROM stage.kaggle_sales_ready
WHERE tax_5 <= 0
	AND check_status IS NULL;


INSERT INTO dq.stage_kaggle_sales
SELECT invoice_id,branch,city,customer_type,gender,product_line,unit_price,quantity,
		tax_5,total,"Date","Time",payment_type,cogs,gross_margin_percentage,gross_income,rating,
		5 AS issue_id --'incorrect raiting'
FROM stage.kaggle_sales_ready
WHERE rating NOT BETWEEN 1 AND 10
	AND check_status IS NULL;



DO $$
DECLARE
    rec RECORD; 
BEGIN
	FOR rec IN SELECT * FROM stage.kaggle_sales_ready WHERE check_status IS NULL LOOP
		BEGIN
		    PERFORM CAST(rec."Time" AS time); 
	
		EXCEPTION
		    WHEN OTHERS THEN
		       INSERT INTO dq.stage_kaggle_sales 
		       VALUES (rec.invoice_id,
				rec.branch,
				rec.city,
				rec.customer_type,
				rec.gender,
				rec.product_line,
				rec.unit_price,
				rec.quantity,
				rec.tax_5,
				rec.total,
				rec."Date",
				rec."Time",
				rec.payment_type,
				rec.cogs,
				rec.gross_margin_percentage,
				rec.gross_income,
				rec.rating,
				6 ); --'incorrect time'

		END;
	END LOOP;
END;
$$;


UPDATE stage.kaggle_sales_ready st
SET check_status = 2 -- DELETE
FROM dq.stage_kaggle_sales dq
WHERE dq.invoice_id = st.invoice_id
  AND dq.issue_id = 6; --'incorrect time'



UPDATE stage.kaggle_sales_ready
SET check_status = 2 -- DELETE 
WHERE invoice_id NOT SIMILAR TO '\d{3}-\d{2}-\d{4}' OR
		unit_price <= 0 OR
		quantity <= 0 OR
		tax_5 <= 0 OR
		rating NOT BETWEEN 1 AND 10;
	
	
DELETE FROM stage.kaggle_sales_ready
WHERE check_status = 2;

UPDATE stage.kaggle_sales_ready
SET check_status = 1; -- CHECKED




	
	