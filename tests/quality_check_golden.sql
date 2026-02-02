/*
=======================================================
QUALITY CHECK

The purpose of this script is to check the data quality 
including consistency, accuracy, and standardization 
in the golden schema.

Usage Notes:
- Run these checks after loading the silver layer data.
========================================================
*/


/*
	QUALITY CHECK: dim_customers
*/

-- Check if there's any duplicate
-- Expectation: No Result
SELECT 
	cst_id, 
	COUNT(*) 
FROM
	(SELECT
		ci.cst_id,
		ci.cst_key,
		ci.cst_firstname,
		ci.cst_lastname,
		ci.cst_marital_status,
		ci.cst_gndr,
		ci.cst_create_date,
		ca.bdate,
		ca.gen,
		la.cntry
	FROM silver.crm_cust_info ci
	LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_a101 la
	ON ci.cst_key = la.cid)t
GROUP BY cst_id
HAVING COUNT(*) > 1;

/*
	QUALITY CHECK: dim_products
*/

-- Check if there's any duplicate
-- Expectation: No Result

SELECT
	prd_key,
	COUNT(*)
FROM
	(SELECT
		pn.prd_id,
		pn.cat_id,
		pn.prd_key,
		pn.prd_nm,
		pn.prd_cost,
		pn.prd_line,
		pn.prd_start_dt,
		pc.cat,
		pc.subcat,
		pc.maintenance
	FROM silver.crm_prd_info pn
	LEFT JOIN silver.erp_px_cat_g1v2 pc
	ON pn.cat_id = pc.id
	WHERE prd_end_dt IS NULL)t
GROUP BY prd_key
HAVING COUNT(*) > 1;

/*
	QUALITY CHECK: fact_sales
*/

-- Foreign Key Integrity
-- Expectation: No Result

SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON f.customer_key = c.customer_key
LEFT JOIN gold.dim_products p
ON f.product_key = p.product_key
WHERE p.product_key IS NULL;
-- WHERE c.customer_key IS NULL;
