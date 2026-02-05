/*
===============================================================================
FILE NAME  : quality_checks_gold.sql
LAYER      : Gold (Business & Analytics Ready)
DATABASE   : gold (MySQL)
===============================================================================

PURPOSE:
    Perform quality checks to validate integrity, consistency, and correctness
    of the Gold layer dimensional model.

VALIDATIONS INCLUDED:
    - Uniqueness of surrogate keys in dimension views
    - Referential integrity between fact and dimension views
    - Detection of orphaned fact records

USAGE NOTES:
    - Run after Gold views are created
    - All queries are read-only
    - Expected result for most checks: NO ROWS
===============================================================================
*/

-- ====================================================================
-- Checking gold.dim_customers
-- ====================================================================
-- Check for uniqueness of customer_key
-- Expectation: No results
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- Optional sanity check: Null surrogate keys
-- Expectation: No results
SELECT *
FROM gold.dim_customers
WHERE customer_key IS NULL;

-- ====================================================================
-- Checking gold.dim_products
-- ====================================================================
-- Check for uniqueness of product_key
-- Expectation: No results
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- Optional sanity check: Null surrogate keys
-- Expectation: No results
SELECT *
FROM gold.dim_products
WHERE product_key IS NULL;

-- ====================================================================
-- Checking gold.fact_sales
-- ====================================================================
-- Referential integrity check:
-- Every fact record must map to valid dimension records
-- Expectation: No results
SELECT 
    f.*
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
    ON p.product_key = f.product_key
WHERE c.customer_key IS NULL
   OR p.product_key IS NULL;
