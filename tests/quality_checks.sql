/*
===============================================================================
Quality Checks (Automated, for CI)
===============================================================================
Automated version of tests/quality_check_silver.sql and
tests/quality_check_golden.sql, adapted to run automatically via
GitHub Actions

quality_check_silver.sql and quality_check_golden.sql are still used
for manual review in SSMS during development. This file is CI only.
===============================================================================
*/

SET NOCOUNT ON;

-- ============================================================
-- SILVER: crm_cust_info
-- ============================================================
IF EXISTS (
    SELECT cst_id FROM silver.crm_cust_info
    GROUP BY cst_id HAVING COUNT(*) > 1 OR cst_id IS NULL
)
    THROW 50001, 'FAILED: NULL or duplicate cst_id found in silver.crm_cust_info.', 1;

IF EXISTS (SELECT 1 FROM silver.crm_cust_info WHERE cst_key != TRIM(cst_key))
    THROW 50001, 'FAILED: unwanted whitespace found in cst_key.', 1;

IF EXISTS (
    SELECT 1 FROM silver.crm_cust_info
    WHERE cst_gndr NOT IN ('Female', 'Male', 'n/a')
)
    THROW 50001, 'FAILED: cst_gndr has a value outside Female/Male/n-a.', 1;

PRINT 'PASSED: silver.crm_cust_info';

-- ============================================================
-- SILVER: crm_prd_info
-- ============================================================
IF EXISTS (
    SELECT prd_id FROM silver.crm_prd_info
    GROUP BY prd_id HAVING COUNT(*) > 1 OR prd_id IS NULL
)
    THROW 50002, 'FAILED: NULL or duplicate prd_id found in silver.crm_prd_info.', 1;

IF EXISTS (SELECT 1 FROM silver.crm_prd_info WHERE prd_nm != TRIM(prd_nm))
    THROW 50002, 'FAILED: unwanted whitespace found in prd_nm.', 1;

IF EXISTS (SELECT 1 FROM silver.crm_prd_info WHERE prd_cost < 0 OR prd_cost IS NULL)
    THROW 50002, 'FAILED: prd_cost is negative or NULL.', 1;

IF EXISTS (SELECT 1 FROM silver.crm_prd_info WHERE prd_end_dt < prd_start_dt)
    THROW 50002, 'FAILED: prd_end_dt occurs before prd_start_dt.', 1;

PRINT 'PASSED: silver.crm_prd_info';

-- ============================================================
-- SILVER: crm_sales_details
-- ============================================================
IF EXISTS (
    SELECT 1 FROM silver.crm_sales_details
    WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt
)
    THROW 50003, 'FAILED: sls_order_dt occurs after sls_ship_dt/sls_due_dt.', 1;

IF EXISTS (
    SELECT 1 FROM silver.crm_sales_details
    WHERE sls_sales != sls_quantity * sls_price
       OR sls_sales IS NULL OR sls_quantity IS NULL
       OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
)
    THROW 50003, 'FAILED: sls_sales is inconsistent with sls_quantity * sls_price, or a value is <= 0/NULL.', 1;

PRINT 'PASSED: silver.crm_sales_details';

-- ============================================================
-- SILVER: erp_cust_az12
-- ============================================================
IF EXISTS (SELECT 1 FROM silver.erp_cust_az12 WHERE bdate > GETDATE())
    THROW 50004, 'FAILED: future bdate found in silver.erp_cust_az12.', 1;

IF EXISTS (SELECT 1 FROM silver.erp_cust_az12 WHERE gen NOT IN ('Female', 'Male', 'n/a'))
    THROW 50004, 'FAILED: gen has a value outside Female/Male/n-a.', 1;

PRINT 'PASSED: silver.erp_cust_az12';

-- ============================================================
-- SILVER: erp_px_cat_g1v2
-- ============================================================
IF EXISTS (
    SELECT 1 FROM silver.erp_px_cat_g1v2
    WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)
)
    THROW 50005, 'FAILED: unwanted whitespace found in cat/subcat/maintenance.', 1;

PRINT 'PASSED: silver.erp_px_cat_g1v2';

-- ============================================================
-- GOLD: dim_customers - no duplicate
-- ============================================================
IF EXISTS (
    SELECT cst_id, COUNT(*)
    FROM (
        SELECT
            ci.cst_id,
            ci.cst_key,
            ca.bdate,
            ca.gen,
            la.cntry
        FROM silver.crm_cust_info ci
        LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
        LEFT JOIN silver.erp_loc_a101 la ON ci.cst_key = la.cid
    ) t
    GROUP BY cst_id
    HAVING COUNT(*) > 1
)
    THROW 50006, 'FAILED: duplicate cst_id found in gold.dim_customers (source query).', 1;

PRINT 'PASSED: gold.dim_customers no duplicate';

-- ============================================================
-- GOLD: dim_products - no duplicate
-- ============================================================
IF EXISTS (
    SELECT prd_key, COUNT(*)
    FROM (
        SELECT pn.prd_key, pn.prd_id
        FROM silver.crm_prd_info pn
        LEFT JOIN silver.erp_px_cat_g1v2 pc ON pn.cat_id = pc.id
        WHERE pn.prd_end_dt IS NULL
    ) t
    GROUP BY prd_key
    HAVING COUNT(*) > 1
)
    THROW 50007, 'FAILED: duplicate prd_key found in gold.dim_products (source query).', 1;

PRINT 'PASSED: gold.dim_products no duplicate';

-- ============================================================
-- GOLD: fact_sales - referential integrity
-- ============================================================
IF EXISTS (
    SELECT 1
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_customers c ON f.customer_key = c.customer_key
    LEFT JOIN gold.dim_products p ON f.product_key = p.product_key
    WHERE p.product_key IS NULL OR c.customer_key IS NULL
)
    THROW 50008, 'FAILED: orphan row found in gold.fact_sales (FK not found in dim_customers/dim_products).', 1;

PRINT 'PASSED: gold.fact_sales referential integrity';

PRINT '================================================';
PRINT 'ALL QUALITY CHECKS PASSED (silver + gold)';
PRINT '================================================';
