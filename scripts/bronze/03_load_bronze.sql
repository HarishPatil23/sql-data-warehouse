/*
===============================================================================
File Name   : 03_load_bronze.sql
Layer       : Bronze
Database    : bronze (MySQL)
===============================================================================
Purpose:
    This script loads raw data from external CSV files into Bronze layer tables.
    The Bronze layer represents the raw ingestion zone with minimal transformation.

Key Characteristics:
    - Truncates target tables before loading
    - Uses LOAD DATA INFILE for high-performance ingestion
    - Handles dirty data (blank values, \r, \n characters)
    - No joins, no business rules
    - No deduplication
    - No stored procedures (MySQL limitation)

Execution:
    Run this script manually or via orchestration tool.
    Ensure correct database is selected before execution.

===============================================================================
*/

-- ============================================================================
-- Set Database Context
-- ============================================================================
USE bronze;

-- ============================================================================
-- CRM SOURCE TABLES
-- ============================================================================

/* ------------------------------------------------
   CRM - CUSTOMER INFORMATION
------------------------------------------------ */
TRUNCATE TABLE crm_cust_info;

LOAD DATA INFILE
'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/source_crm/cust_info.csv'
INTO TABLE crm_cust_info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    @cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    @cst_create_date
)
SET
    cst_id = NULLIF(
        REPLACE(REPLACE(TRIM(@cst_id), '\r', ''), '\n', ''),
        ''
    ),
    cst_create_date = STR_TO_DATE(
        NULLIF(
            REPLACE(REPLACE(TRIM(@cst_create_date), '\r', ''), '\n', ''),
            ''
        ),
        '%Y-%m-%d'
    );


/* ------------------------------------------------
   CRM - PRODUCT INFORMATION
------------------------------------------------ */
TRUNCATE TABLE crm_prd_info;

LOAD DATA INFILE
'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/source_crm/prd_info.csv'
INTO TABLE crm_prd_info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    prd_id,
    prd_key,
    prd_nm,
    @prd_cost,
    @prd_line,
    @prd_start_dt,
    @prd_end_dt
)
SET
    prd_cost = NULLIF(
        REPLACE(REPLACE(TRIM(@prd_cost), '\r', ''), '\n', ''),
        ''
    ),
    prd_line = NULLIF(
        REPLACE(REPLACE(TRIM(@prd_line), '\r', ''), '\n', ''),
        ''
    ),
    prd_start_dt = STR_TO_DATE(
        NULLIF(REPLACE(REPLACE(TRIM(@prd_start_dt), '\r', ''), '\n', ''), ''),
        '%Y-%m-%d'
    ),
    prd_end_dt = STR_TO_DATE(
        NULLIF(REPLACE(REPLACE(TRIM(@prd_end_dt), '\r', ''), '\n', ''), ''),
        '%Y-%m-%d'
    );


/* ------------------------------------------------
   CRM - SALES DETAILS
------------------------------------------------ */
TRUNCATE TABLE crm_sales_details;

LOAD DATA INFILE
'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/source_crm/sales_details.csv'
INTO TABLE crm_sales_details
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    sls_ord_num,
    sls_prd_key,
    @sls_cust_id,
    @sls_order_dt,
    @sls_ship_dt,
    @sls_due_dt,
    @sls_sales,
    @sls_quantity,
    @sls_price
)
SET
    sls_cust_id = NULLIF(
        REPLACE(REPLACE(TRIM(@sls_cust_id), '\r', ''), '\n', ''),
        ''
    ),

    sls_order_dt = NULLIF(
        REPLACE(REPLACE(TRIM(@sls_order_dt), '\r', ''), '\n', ''),
        ''
    ),

    sls_ship_dt = NULLIF(
        REPLACE(REPLACE(TRIM(@sls_ship_dt), '\r', ''), '\n', ''),
        ''
    ),

    sls_due_dt = NULLIF(
        REPLACE(REPLACE(TRIM(@sls_due_dt), '\r', ''), '\n', ''),
        ''
    ),

    sls_sales = NULLIF(
        REPLACE(REPLACE(TRIM(@sls_sales), '\r', ''), '\n', ''),
        ''
    ),

    sls_quantity = NULLIF(
        REPLACE(REPLACE(TRIM(@sls_quantity), '\r', ''), '\n', ''),
        ''
    ),

    sls_price = NULLIF(
        REPLACE(REPLACE(TRIM(@sls_price), '\r', ''), '\n', ''),
        ''
    );


-- ============================================================================
-- ERP SOURCE TABLES
-- ============================================================================

/* ------------------------------------------------
   ERP - LOCATION DATA
------------------------------------------------ */
TRUNCATE TABLE erp_loc_a101;

LOAD DATA INFILE
'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/source_erp/loc_a101.csv'
INTO TABLE erp_loc_a101
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    cid,
    cntry
);


/* ------------------------------------------------
   ERP - CUSTOMER MASTER (AZ12)
------------------------------------------------ */
TRUNCATE TABLE erp_cust_az12;

LOAD DATA INFILE
'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/source_erp/CUST_AZ12.csv'
INTO TABLE erp_cust_az12
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    @cid,
    @bdate,
    @gen
)
SET
    cid = NULLIF(
        REPLACE(REPLACE(TRIM(@cid), '\r', ''), '\n', ''),
        ''
    ),

    bdate = STR_TO_DATE(
        NULLIF(
            REPLACE(REPLACE(TRIM(@bdate), '\r', ''), '\n', ''),
            ''
        ),
        '%Y-%m-%d'
    ),

    gen = NULLIF(
        REPLACE(REPLACE(TRIM(@gen), '\r', ''), '\n', ''),
        ''
    );


/* ------------------------------------------------
   ERP - PRODUCT CATEGORY
------------------------------------------------ */
TRUNCATE TABLE erp_px_cat_g1v2;

LOAD DATA INFILE
'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/source_erp/px_cat_g1v2.csv'
INTO TABLE erp_px_cat_g1v2
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    id,
    cat,
    subcat,
    maintenance
);

-- ============================================================================
-- End of Bronze Load Script
-- ============================================================================
