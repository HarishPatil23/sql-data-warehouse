/*
=============================================================
FILE: 01_init_database.sql
PROJECT: MySQL Data Warehouse
PURPOSE:
    This script initializes the Data Warehouse environment
    by creating separate databases for each layer:
    - bronze  : Raw ingestion layer
    - silver  : Cleaned & transformed layer
    - gold    : Analytics & reporting layer

IMPORTANT NOTES:
    - MySQL treats SCHEMA as a synonym for DATABASE.
    - Therefore, each layer is implemented as a separate database.
    - This script is intended to be run ONE TIME only.

WARNING:
    Running this script will DROP the databases listed below
    if they already exist.
    ALL DATA in those databases will be permanently deleted.
    Ensure backups exist before execution.
=============================================================
*/

-- -----------------------------------------------------------
-- Drop existing layer databases (if any)
-- -----------------------------------------------------------
DROP DATABASE IF EXISTS bronze;
DROP DATABASE IF EXISTS silver;
DROP DATABASE IF EXISTS gold;

-- -----------------------------------------------------------
-- Create layer databases
-- -----------------------------------------------------------
CREATE DATABASE bronze;
CREATE DATABASE silver;
CREATE DATABASE gold;
