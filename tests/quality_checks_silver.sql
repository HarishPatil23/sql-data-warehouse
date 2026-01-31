/*
===============================================================================
FILE NAME  : quality_checks_silver.sql
LAYER      : Silver (Data Quality Validation)
DATABASE   : silver (MySQL)
===============================================================================

PURPOSE:
    This script performs data quality checks on the Silver layer to ensure:
    - Primary key uniqueness
    - Standardization correctness
    - Data consistency and validity
    - Referential integrity readiness

USAGE NOTES:
    - Run AFTER sp_load_silver execution
    - All queries are diagnostic (read-only)
    - Any returned rows indicate issues to investigate

===============================================================================
*/

-- ====================================================================
-- SILVER.CRM_CUST_INFO
-- ====================================================================

-- Primary Key: NULLs or Duplicates
-- Expectation: No rows
SELECT
    cst_id,
    COUNT(*) AS cnt
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING cst_id IS NULL OR COUNT(*) > 1;

-- Unwanted Spaces
-- Expectation: No rows
SELECT cst_key
FROM silver.crm_cust_info
WHERE cst_key <> TRIM(cst_key);

-- Standardization Check
SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

-- ====================================================================
-- SILVER.CRM_PRD_INFO
-- ====================================================================

-- Primary Key: NULLs or Duplicates
-- Expectation: No rows
SELECT
    prd_id,
    COUNT(*) AS cnt
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING prd_id IS NULL OR COUNT(*) > 1;

-- Unwanted Spaces
-- Expectation: No rows
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm <> TRIM(prd_nm);

-- Invalid Costs
-- Expectation: No rows
SELECT *
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Standardization Check
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- Invalid Date Ranges
-- Expectation: No rows
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt IS NOT NULL
  AND prd_end_dt < prd_start_dt;

-- ====================================================================
-- SILVER.CRM_SALES_DETAILS
-- ====================================================================

-- Invalid Date Order
-- Expectation: No rows
SELECT *
FROM silver.crm_sales_details
WHERE (sls_ship_dt IS NOT NULL AND sls_order_dt > sls_ship_dt)
   OR (sls_due_dt  IS NOT NULL AND sls_order_dt > sls_due_dt);

-- Sales Consistency Check
-- Expectation: No rows
SELECT *
FROM silver.crm_sales_details
WHERE sls_sales <> sls_quantity * sls_price
   OR sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0;

-- Orphan Product Keys (FK readiness)
-- Expectation: No rows
SELECT DISTINCT s.sls_prd_key
FROM silver.crm_sales_details s
LEFT JOIN silver.crm_prd_info p
       ON s.sls_prd_key = p.prd_key
WHERE p.prd_key IS NULL;

-- ====================================================================
-- SILVER.ERP_CUST_AZ12
-- ====================================================================

-- Identify Out-of-Range Dates
-- Expectation: Birthdates between 1924-01-01 and Today
SELECT *
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01'
   OR bdate > CURDATE();

-- Gender Standardization
SELECT DISTINCT gen
FROM silver.erp_cust_az12;

-- Primary Key NULLs
-- Expectation: No rows
SELECT *
FROM silver.erp_cust_az12
WHERE cid IS NULL;

-- ====================================================================
-- SILVER.ERP_LOC_A101
-- ====================================================================

-- Country Standardization
SELECT DISTINCT cntry
FROM silver.erp_loc_a101
ORDER BY cntry;

-- Primary Key NULLs
-- Expectation: No rows
SELECT *
FROM silver.erp_loc_a101
WHERE cid IS NULL;

-- ====================================================================
-- SILVER.ERP_PX_CAT_G1V2
-- ====================================================================

-- Unwanted Spaces
-- Expectation: No rows
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE cat <> TRIM(cat)
   OR subcat <> TRIM(subcat)
   OR maintenance <> TRIM(maintenance);

-- Maintenance Standardization
SELECT DISTINCT maintenance
FROM silver.erp_px_cat_g1v2;
