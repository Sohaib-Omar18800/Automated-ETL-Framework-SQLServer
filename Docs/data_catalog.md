# Data Warehouse Documentation: Gold Layer (Sales Analytics)

This folder contains the DDL for the **Gold Layer**, which serves as the final consumption layer for Business Intelligence and Analytics. These views implement a **Star Schema** to ensure high performance and ease of use.

---

## 🏗 Schema Overview

The Gold layer transforms cleansed data from the Silver layer into business-ready dimensions and facts.

- **Fact Tables:** Quantitative data and metrics (e.g., Sales, Quantities).
- **Dimension Tables:** Descriptive attributes used for filtering and grouping (e.g., Customer names, Product categories).

---

## 📈 Fact Table: `gold.fact_sales`
**Purpose:** Stores all sales transactions. Use this for calculating Revenue and Order counts.

| Column | Type | Description | Join Key |
| :--- | :--- | :--- | :--- |
| `customer_sk` | INT | Surrogate Key for Customers | `gold.dim_customers` |
| `product_sk` | INT | Surrogate Key for Products | `gold.dim_product` |
| `order_number` | NVARCHAR | The unique ID of the sales order. | - |
| `order_date` | DATE | Date the order was placed. | - |
| `due_date` | DATE | Date the payment is due. | - |
| `shipping_date`| DATE | Date the product was shipped. | - |
| `quantity` | INT | Number of units sold. | - |
| `price` | DECIMAL(16,2)| Unit price of the product. | - |
| `sales` | DECIMAL(16,2)| Total revenue (`quantity * price`). | - |

---

## 📦 Dimension Table: `gold.dim_product`
**Purpose:** Provides a master list of products. This view doesnot include **historical pricing** products.

| Column | Type | Description |
| :--- | :--- | :--- |
| `product_sk` | INT | **Primary Key.** Unique surrogate key for the warehouse. |
| `product_id` | INT | Technical ID from the source CRM. |
| `product_number`| NVARCHAR | Unique business key (Product SKU). |
| `product_name` | NVARCHAR | Name of the product. |
| `category_name` | NVARCHAR | High-level product category (from ERP). |
| `subcategory` | NVARCHAR | Detailed product subcategory. |
| `maintenance` | NVARCHAR | Maintenance requirements(eg.,'Yes','No'). |
| `cost` | DECIMAL(16,2) | Standard cost of the product. |
| `product_line`| NVARCHAR | Business line classification. |
| `product_start_date` | DATE | Date the product was added to the catalog. |

---

## 👤 Dimension Table: `gold.dim_customers`
**Purpose:** Store customer details combining CRM data with ERP location and demographic details.

| Column | Type | Description |
| :--- | :--- | :--- |
| `customer_sk`| INT | **Primary Key.** Unique surrogate key for the warehouse. |
| `customer_id`| INT | Technical ID from the source CRM. |
| `customer_number`| NVARCHAR | Unique business key for the customer. |
| `first_name`| NVARCHAR  | Customer's first name. |
| `last_name`| NVARCHAR  | Customer's last name. |
| `country` | NVARCHAR | Customer's country (sourced from ERP). |
| `material_status`| NVARCHAR | Marital status. |
| `gender` | NVARCHAR | Gender of the customer (eg., male,female)|
| `birthdate` | DATE | Date of birth for age-demographic analysis. |
| `dwh_create_date`| DATETIME | Record creation timestamp in the warehouse. |

---
