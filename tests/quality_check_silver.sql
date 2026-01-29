/*
=======================================================
QUALITY CHECK

The purpose of this script is to check the data quality 
including consistency, accuracy, and standardication 
in the silver schema.

Usage Notes:
- Run these checks after loading the silver layer data.
========================================================
*/


/*
	CLEAN & LOAD TABLE: crm_cust_info
*/
SELECT TOP 1000 * FROM bronze.crm_cust_info;

-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No Result
SELECT
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 or cst_id IS NULL;

-- Check for Unwanted Space
-- Expectation: No Result
SELECT cst_key
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);

-- Data Standardization & Consistency
-- Expectation: Female, Male, and n/a
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

SELECT * FROM silver.crm_cust_info;

/*
	CLEAN & LOAD TABLE: crm_prd_info
*/
SELECT TOP 1000 * FROM bronze.crm_prd_info;

-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No Result
SELECT
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 or prd_id IS NULL;

-- Check for Unwanted Space
-- Expectation: No Result
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for NULLs or Negative Numbers
-- Expectation: No Result
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Data for Invalid Date Orders
-- Expectation: No Result
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

SELECT * FROM silver.crm_prd_info;

/*
	CLEAN & LOAD TABLE: crm_sales_details
*/

-- Check for Invalid Dates
-- Expectation: No Result
SELECT
	NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM silver.crm_sales_details
WHERE sls_due_dt <= 0
OR LEN(sls_due_dt) ! = 8
OR sls_due_dt > 20500101
OR sls_due_dt < 19000101;

-- Check for Invalid Date Orders
-- Expectation: No Result
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- Check Data Consistency between Sales, Quantity, Price
-- Sales = Quantity * Price
-- Values must not be negative, zero, or NULL
-- Expectation: No Result

SELECT DISTINCT
	sls_sales,
	sls_quantity,
	sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0;

SELECT * FROM silver.crm_sales_details;

/*
	CLEAN & LOAD TABLE: erp_cust_az12
*/

-- Identify Out of Range Dates
-- Expectation: No Result
SELECT DISTINCT
	bdate
FROM silver.erp_cust_az12
WHERE bdate > GETDATE();

-- Data Standardization & Consistency
-- Expectation: Female, Male, and n/a
SELECT DISTINCT
	gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	 ELSE 'n/a'
END AS gen
FROM silver.erp_cust_az12;

SELECT * FROM silver.erp_cust_az12;

/*
	CLEAN & LOAD TABLE: erp_loc_a101
*/

-- Data Standardization & Consistency
SELECT DISTINCT
	cntry
FROM silver.erp_loc_a101
ORDER BY cntry;

SELECT * FROM silver.erp_loc_a101;

/*
	CLEAN & LOAD TABLE: erp_px_cat_g1v2
*/

-- Check for Unwanted Spaces
-- Expectation: No Result
SELECT * FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);

-- Data Standardization & Consistency
SELECT DISTINCT
	maintenance
FROM silver.erp_px_cat_g1v2;

SELECT * FROM silver.erp_px_cat_g1v2
