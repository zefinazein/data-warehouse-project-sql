/*
=====================================================================
DDL SCRIPT: Create Gold Views in 'gold' schema

Note:
	Creates views for the 'gold' layer in the DataWarehouse.
	This contains the final dimension and fact tables (star schema)
	and each views performs transformation and combines data from 
	silver layer to produce a business-ready dataset
=====================================================================
*/

------------------------------------------
-- CREATE DIMENSION: gold.dim_customers
------------------------------------------

IF OBJECT_ID ('gold.dim_customers', 'V') IS NOT NULL 
	DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER () OVER (ORDER BY cst_id) as customer_key,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
		la.cntry as country,
	ci.cst_marital_status as marital_status,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
		 ELSE COALESCE(ca.gen, 'n/a')
	END as gender,
	ci.cst_create_date as create_date,
	ca.bdate as birthdate
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON		ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON		ci.cst_key = la.cid;
GO

------------------------------------------
-- CREATE DIMENSION: gold.dim_products
------------------------------------------

IF OBJECT_ID ('gold.dim_products', 'V') IS NOT NULL
   DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT
	ROW_NUMBER () OVER (ORDER BY pr.prd_start_dt, pr.prd_key) AS product_key,
	pr.prd_id AS product_id,
	pr.prd_key AS product_number,
	pr.prd_nm AS product_name,
	pr.cat_id AS category_id,
	px.cat AS category,
	px.subcat AS subcategory,
	px.maintenance,
	pr.prd_cost AS cost,
	pr.prd_line AS product_line,
	pr.prd_start_dt AS start_date
FROM silver.crm_prd_info pr
LEFT JOIN silver.erp_px_cat_g1v2 px
ON pr.cat_id = px.id
WHERE prd_end_dt IS NULL; --Filter out all historical data
GO
------------------------------------------
-- CREATE DIMENSION: gold.fact_sales
------------------------------------------

IF OBJECT_ID ('gold.fact_sales', 'V') IS NOT NULL
   DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
	sd.sls_ord_num AS order_number,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id;
GO
