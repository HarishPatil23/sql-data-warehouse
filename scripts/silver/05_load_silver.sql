/*
===============================================================================
FILE NAME  : 05_load_silver.sql
LAYER      : Silver (Cleaned & Standardized)
DATABASE   : silver (MySQL)
===============================================================================

PURPOSE:
    This script loads cleaned, standardized, and conformed data
    from the Bronze layer into the Silver layer.

SILVER LAYER RULES:
    - Source data ONLY from Bronze
    - Apply data type corrections
    - Standardize values
    - Handle nulls & invalid records
    - Remove duplicates where required
    - NO business aggregations
    - NO analytics logic

EXECUTION NOTES:
    - Safe to re-run (TRUNCATE + INSERT)
    - Requires Bronze layer to be loaded first
    - Uses fully qualified table names for cross-layer access

===============================================================================
*/

DROP PROCEDURE IF EXISTS sp_load_silver;
DELIMITER $$

CREATE PROCEDURE sp_load_silver()
BEGIN
    /* -----------------------------
       Batch-level timing
       ----------------------------- */
    DECLARE batch_start_time DATETIME;
    DECLARE batch_end_time DATETIME;

    /* -----------------------------
       Fail fast if any step errors
       ----------------------------- */
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SELECT 'ERROR OCCURRED DURING SILVER LOAD';
    END;

    SET batch_start_time = NOW();

    /* ============================================================
       CRM - CUSTOMER INFORMATION
       - Deduplication
       - Standardization of gender & marital status
       ============================================================ */
    TRUNCATE TABLE silver.crm_cust_info;

    INSERT INTO silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        CASE
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END,
        CASE
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END,
        cst_create_date
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY cst_id
                   ORDER BY cst_create_date DESC
               ) AS flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) t
    WHERE flag_last = 1;

    /* ============================================================
       CRM - PRODUCT INFORMATION
       - Key derivation (category & product)
       - Date versioning for product end date
       ============================================================ */
    TRUNCATE TABLE silver.crm_prd_info;

    INSERT INTO silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
        SUBSTRING(prd_key, 7) AS prd_key,
        prd_nm,
        IFNULL(prd_cost, 0),
        CASE
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'n/a'
        END,
        prd_start_dt,
        DATE_SUB(
            LEAD(prd_start_dt) OVER (
                PARTITION BY prd_key
                ORDER BY prd_start_dt
            ),
            INTERVAL 1 DAY
        ) AS prd_end_dt
    FROM bronze.crm_prd_info;

    /* ============================================================
       CRM - SALES DETAILS
       - Date correction
       - Sales & price validation
       ============================================================ */
    TRUNCATE TABLE silver.crm_sales_details;

    INSERT INTO silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE WHEN sls_order_dt = 0 OR CHAR_LENGTH(sls_order_dt) <> 8
             THEN NULL ELSE STR_TO_DATE(sls_order_dt, '%Y%m%d') END,
        CASE WHEN sls_ship_dt = 0 OR CHAR_LENGTH(sls_ship_dt) <> 8
             THEN NULL ELSE STR_TO_DATE(sls_ship_dt, '%Y%m%d') END,
        CASE WHEN sls_due_dt = 0 OR CHAR_LENGTH(sls_due_dt) <> 8
             THEN NULL ELSE STR_TO_DATE(sls_due_dt, '%Y%m%d') END,
        CASE
            WHEN sls_sales IS NULL
              OR sls_sales <= 0
              OR sls_sales <> sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END,
        sls_quantity,
        CASE
            WHEN sls_price IS NULL OR sls_price <= 0
            THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END
    FROM bronze.crm_sales_details;

    /* ============================================================
       ERP - CUSTOMER
       - Remove 'NAS' prefix from IDs
       - Nullify future birthdates
       - Standardize gender values
       ============================================================ */
    TRUNCATE TABLE silver.erp_cust_az12;

    INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
    SELECT
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4) ELSE cid END,
        CASE WHEN bdate > CURDATE() THEN NULL ELSE bdate END,
        CASE
            WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
            ELSE 'n/a'
        END
    FROM bronze.erp_cust_az12;

    /* ============================================================
       ERP - LOCATION
       - Remove unwanted characters in country
       - Standardize country names
       ============================================================ */
    TRUNCATE TABLE silver.erp_loc_a101;

    INSERT INTO silver.erp_loc_a101 (cid, cntry)
    SELECT
        REPLACE(cid,'-',''),
        CASE
            WHEN cntry_clean IS NULL OR cntry_clean = '' THEN 'n/a'
            WHEN cntry_clean IN ('US','USA') THEN 'United States'
            WHEN cntry_clean = 'DE' THEN 'Germany'
            ELSE cntry_clean
        END
    FROM (
        SELECT
            cid,
            TRIM(
                REPLACE(
                    REPLACE(
                        REPLACE(cntry, CHAR(9), ''),
                        CHAR(10), ''
                    ),
                    CHAR(13), ''
                )
            ) AS cntry_clean
        FROM bronze.erp_loc_a101
    ) t;

    /* ============================================================
       ERP - PRODUCT CATEGORY
       - Standardize maintenance flag (Yes/No/n/a)
       ============================================================ */
    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
    SELECT
        id,
        cat,
        subcat,
        CASE
            WHEN maintenance_clean IN ('YES','Y') THEN 'Yes'
            WHEN maintenance_clean IN ('NO','N') THEN 'No'
            ELSE 'n/a'
        END
    FROM (
        SELECT
            id,
            cat,
            subcat,
            UPPER(TRIM(
                REPLACE(
                    REPLACE(
                        REPLACE(maintenance, CHAR(9), ''),
                        CHAR(10), ''
                    ),
                    CHAR(13), ''
                )
            )) AS maintenance_clean
        FROM bronze.erp_px_cat_g1v2
    ) t;

    /* -----------------------------
       Batch completion
       ----------------------------- */
    SET batch_end_time = NOW();

    SELECT CONCAT(
        'Silver load completed in ',
        TIMESTAMPDIFF(SECOND, batch_start_time, batch_end_time),
        ' seconds'
    ) AS load_duration;

END$$
DELIMITER ;
