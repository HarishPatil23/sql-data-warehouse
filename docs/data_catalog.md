#  Gold Layer – Data Catalog (MySQL)

## Overview
The **Gold Layer** represents the business-ready data model used for analytics and reporting.  
It follows a **star schema** design consisting of **dimension** and **fact** objects built from the Silver layer.

- Database: `gold`
- Source: `silver` layer
- Technology: **MySQL**
- Objects: Views (read-only, analytics-ready)

---

## 1. `gold.dim_customers`

**Purpose**  
Stores customer master data enriched with demographic and geographic attributes.

### Columns

| Column Name      |    Data Type    | Description |
|------------------|-----------------|-------------|
| customer_key     | INT             | Surrogate key generated in the Gold layer for each customer. |
| customer_id      | INT             | Business identifier of the customer from CRM. |
| customer_number  | VARCHAR(50)     | Alphanumeric customer reference key. |
| first_name       | VARCHAR(50)     | Customer’s first name. |
| last_name        | VARCHAR(50)     | Customer’s last name. |
| country          | VARCHAR(50)     | Customer country (standardized). |
| marital_status   | VARCHAR(50)     | Marital status (Single, Married, n/a). |
| gender           | VARCHAR(50)     | Gender (Male, Female, n/a). |
| birthdate        | DATE            | Customer date of birth. |
| create_date      | DATE            | Date when the customer was created in the source system. |

---

## 2. `gold.dim_products`

**Purpose**  
Provides current (active) product details with category and maintenance attributes.

### Columns

| Column Name              |    Data Type    | Description |
|--------------------------|-----------------|-------------|
| product_key              | INT             | Surrogate key generated in the Gold layer for each product. |
| product_id               | INT             | Business identifier of the product. |
| product_number           | VARCHAR(50)     | Alphanumeric product code. |
| product_name             | VARCHAR(50)     | Product name or description. |
| category_id              | VARCHAR(50)     | Category identifier derived from product key. |
| category                 | VARCHAR(50)     | High-level product category. |
| subcategory              | VARCHAR(50)     | Detailed product classification. |
| maintenance_required     | VARCHAR(50)     | Indicates maintenance requirement (Yes / No / n/a). |
| cost                     | DECIMAL(10,2)   | Base cost of the product. |
| product_line             | VARCHAR(50)     | Product line (Road, Mountain, Touring, etc.). |
| start_date               | DATE            | Date the product version became active. |

>  Historical product versions are excluded in Gold. Only active products are exposed.

---

## 3. `gold.fact_sales`

**Purpose**  
Stores transactional sales data at the order line level for analytical reporting.

### Columns

| Column Name     |    Data Type    | Description |
|-----------------|-----------------|-------------|
| order_number    | VARCHAR(50)     | Unique sales order number. |
| product_key     | INT             | Foreign key referencing `gold.dim_products`. |
| customer_key    | INT             | Foreign key referencing `gold.dim_customers`. |
| order_date      | DATE            | Date when the order was placed. |
| shipping_date   | DATE            | Date when the order was shipped. |
| due_date        | DATE            | Payment due date for the order. |
| sales_amount    | DECIMAL(10,2)   | Total sales value for the line item. |
| quantity        | INT             | Quantity sold. |
| price           | DECIMAL(10,2)   | Unit price of the product. |

---

## Design Notes
- All Gold objects are **read-only views**
- Surrogate keys are generated using `ROW_NUMBER()`
- Monetary values use `DECIMAL(10,2)`
- Data is fully cleaned and standardized in Silver
- No transformations, aggregations, or business logic in Gold

---

## Usage
These views are intended for:
- BI tools (Power BI, Tableau, Looker, etc.)
- Ad-hoc analytical queries
- Executive and operational reporting

---
