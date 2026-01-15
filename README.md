# <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/python/python-original.svg" alt="python" width="40" height="40"/> Soda-Powered Medallion Data Pipeline

![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![SQL Server](https://img.shields.io/badge/Microsoft%20SQL%20Server-CC2927?style=for-the-badge&logo=microsoft-sql-server&logoColor=white)
![Polars](https://img.shields.io/badge/Polars-%23CD792C?style=for-the-badge&logo=polars&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?style=for-the-badge&logo=duckdb&logoColor=black)
![Soda](https://img.shields.io/badge/Soda.io-00D1FF?style=for-the-badge&logo=soda&logoColor=white)

An advanced, robust ETL pipeline implementing the **Medallion Architecture** (Bronze, Silver, Gold). This project focuses on **Incremental Loading**, **Data Quality Assurance**, and **Self-Healing Infrastructure**.

---

## 🏗️ Architecture Workflow

Here is the high-level data flow from source to consumption:

<div align="center">
  <img src="./Docs/Data%20Architecture.png" alt="Data Architecture Workflow" width="900">
  <p><i>Figure 1: Medallion Architecture with Polars & Soda Integration</i></p>
</div>

---

## 📂 Project Structure

```text
Automated-ETL-Framework-SQLServer/
├── dataset/
│   ├── source_crm
│   └── source_erp
├── docs/
│   ├── Data_Architecture.png # Architecture Diagram
│   ├── DWH_Schema.png        # Schema of DWH
│   ├── Layers,Tables&dim.png # Connection Between Layers
│   ├── Relations_Between_Tables.png # Relations Diagram
│   └── data_catalog.md
├── scripts/
│   ├── bronze
│     └── ddl_bronze_tables.sql
│   ├── silver
│     └── ddl_silver_tables.sql
│   ├── gold
│     └── ddl_gold_tables.sql
├── src/
│   ├── database/
│     ├── start_bronze.py      # Database initialization & Bronze logic
│     ├── start_silver.py      # Schema creation & Silver dependency checks
│     └── start_gold.py        # Transformation & Star Schema in DuckDB
│   ├── engine/
│     └── auto_increment.py    # The core ETL engine (Incremental Load logic)
│   ├── config/
│     ├── create_bronze_table.py           # Soda YAML files for integrity checks
│     ├── create_silver_table.py           # Soda YAML files for integrity checks
│     ├── create_gold_table.py           # Soda YAML files for integrity checks
│     ├── etl_bronze_to_silver.py           # Soda YAML files for integrity checks
│     ├── schemas.py           # Soda YAML files for integrity checks
│     ├── soda_config.py           # Soda YAML files for integrity checks
│     ├── sql_service_var.py           # Soda YAML files for integrity checks
│     └── tables.py           # Soda YAML files for integrity checks
│   ├── test/
│     ├── soda_check/
│     ├── crm_customer_info_test.py
│     ├── crm_product_info_test.py
│     ├── crm_sale_details_test.py
│     ├── erp_customer_az12_test.py
│     ├── erp_loc_a101_test.py
│     ├── erp_px_cat_g1v2_test.py
│     └── print_sample_of_all.py
│   └── main.py                  # CLI Entry point
└── requirements.txt         # Project dependencies
```
## 🛠️ **<u>Data Pipeline Details</u>**
### **1. 🥉 Bronze Layer (Raw Ingestion)**
- Source: Extracts data from multi-source CSV files (CRM & ERP).

- Process: Bulk loading into SQL Server using Polars for high-speed ingestion.

- Metadata: Every record is tagged with a bronze_inserted_at timestamp to enable incremental tracking.

### **2. 🥈 Silver Layer (Cleansing & Transformation)**
- Cleaning: Handles data types standardization and null values handling.

- Incremental Load (The Brain): The auto_increment.py engine compares the source (Bronze) with the target (Silver) using timestamps, ensuring that only new records are processed.

- Validation: Integrated Soda Core checks run at this stage to ensure data schema and integrity before moving forward.

### **3. 🥇 Gold Layer (Analytical Modeling)**
- Storage: Data is moved to DuckDB for specialized analytical performance.

- Modeling: Implements a Star Schema (Fact & Dimension tables).

- Final Product: Creates SQL Views ready for BI tools like Power BI or Tableau.

### **🛡️ Reliability & Self-Healing**
What makes this framework different is the Self-Healing Infrastructure:

- Dependency Awareness: The system detects if you are trying to run a "Gold Load" while the "Silver" or "Bronze" layers are empty.

- Recursive Triggers: It will automatically backtrack, initialize the database, create schemas, and load the missing upstream data before completing your requested operation.

- Fail-Safe Connections: Robust handling of Windows Authentication and dynamic SQL Server connection strings.

### **✅ Data Quality Assurance (Soda.io)**
- We don't just move data; we ensure it's correct. The pipeline executes automated tests:

- Schema Validation: Ensures no breaking changes in source files.

- Uniqueness Checks: Prevents duplicate records in Dimension tables.

- Referential Integrity: Validates that Sales facts correspond to existing Customers and Products.

### **🚀 How to Run**
- Configure SQL Server: Ensure your SQL instance is running.

- Install Requirements: pip install -r requirements.txt

- Launch: Run python src/main.py.

**Interactive CLI: Follow the prompts to select the layer (Bronze/Silver/Gold) or perform a full "All Tables" sync.**

## ✨ Developed by
**Sohaib Omar**

Feel free to reach out for collaborations or questions!

**_LinkedIn_**:
🔗
[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/sohaib-omar-188oo)
