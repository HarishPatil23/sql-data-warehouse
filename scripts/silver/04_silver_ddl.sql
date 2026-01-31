/*
===============================================================================
FILE: 04_silver_ddl.sql
LAYER: SILVER (Cleaned & Conformed Data)
PURPOSE:
    This script creates Silver-layer tables in MySQL.
    Silver tables store cleaned, standardized, and type-corrected
    data sourced from the Bronze layer.

EXECUTION NOTES:
    - Tables are dropped and recreated to allow re-runs.
    - This script only defines structure (no data movement).
    - Data loss will occur if tables already exist.

DATABASE:
    silver
===============================================================================
*/

-- ---------------------------------------------------------------------------
-- Set active database
-- ---------------------------------------------------------------------------
USE silver;

-- ---------------------------------------------------------------------------
-- CRM - Customer Information
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS crm_cust_info;

CREATE TABLE crm_cust_info (
    cst_id              INT,
    cst_key             VARCHAR(50),
    cst_firstname       VARCHAR(50),
    cst_lastname        VARCHAR(50),
    cst_marital_status  VARCHAR(50),
    cst_gndr            VARCHAR(50),
    cst_create_date     DATE,
    dwh_create_date     DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------------------------
-- CRM - Product Information
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS crm_prd_info;

CREATE TABLE crm_prd_info (
    prd_id              INT,
    cat_id				VARCHAR(50),
    prd_key             VARCHAR(50),
    prd_nm              VARCHAR(50),
    prd_cost            DECIMAL(10,2),
    prd_line            VARCHAR(50),
    prd_start_dt        DATE,
    prd_end_dt          DATE,
    dwh_create_date     DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------------------------
-- CRM - Sales Details
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS crm_sales_details;

CREATE TABLE crm_sales_details (
    sls_ord_num         VARCHAR(50),
    sls_prd_key         VARCHAR(50),
    sls_cust_id         INT,
    sls_order_dt        DATE,
    sls_ship_dt         DATE,
    sls_due_dt          DATE,
    sls_sales           DECIMAL(10,2),
    sls_quantity        INT,
    sls_price           DECIMAL(10,2),
    dwh_create_date     DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------------------------
-- ERP - Location Data
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS erp_loc_a101;

CREATE TABLE erp_loc_a101 (
    cid                 VARCHAR(50),
    cntry               VARCHAR(50),
    dwh_create_date     DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------------------------
-- ERP - Customer Demographics
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS erp_cust_az12;

CREATE TABLE erp_cust_az12 (
    cid                 VARCHAR(50),
    bdate               DATE,
    gen                 VARCHAR(50),
    dwh_create_date     DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------------------------
-- ERP - Product Category
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS erp_px_cat_g1v2;

CREATE TABLE erp_px_cat_g1v2 (
    id                  VARCHAR(50),
    cat                 VARCHAR(50),
    subcat              VARCHAR(50),
    maintenance         VARCHAR(50),
    dwh_create_date     DATETIME DEFAULT CURRENT_TIMESTAMP
);
