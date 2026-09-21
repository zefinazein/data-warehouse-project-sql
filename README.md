# Data Warehouse SQL Project

Designing, orchestrating, and testing a data warehouse for a bicycle retailer, from raw CRM/ERP data to a business-ready analytics layer.

![SQL Server](https://img.shields.io/badge/SQL%20Server-CC2927?logo=microsoftsqlserver&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-017CEE?logo=apacheairflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)
![GitHub Actions](https://github.com/zefinazein/data-warehouse-project-sql/actions/workflows/test.yml/badge.svg)
![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?logo=powerbi&logoColor=black)

# ⟡ Project Overview ⟡

This project builds a data warehouse from CRM and ERP datasets of a bicycle retail company, using a bronze-silver-gold (medallion) architecture. Beyond the core ETL, this project is orchestrated with Apache Airflow, validated with automated CI/CD testing on every push, and consumed through a Power BI dashboard, forming a complete pipeline from raw data to business insight.

The warehouse enables analysis such as:
- sales performance
- product trends
- regional transactions
- store profitability

# 🗁 Dataset 🗁

The dataset contains CRM and ERP of bicycle retail company including:

### CRM
- **cust_info**: customer information
- **prd_info**: product information
- **sales_details**: sales transaction records

### ERP
- **cust_az12**: additional customer demographic data
- **loc_a101**: location and regional information
- **px_cat_g1v2**: product category data

# 🖾 Data Architecture 🖾

<img width="1456" height="792" alt="image" src="https://github.com/user-attachments/assets/357fa82f-283d-4af8-8193-15f35c19d975" />

# ☆ Data Schema ☆

Type: Star

#### Fact Table
- fact_sales

#### Dimension Tables
- dim_customer
- dim_product

# ❯❯❯❯ Data Flow ❯❯❯❯

<img width="640" height="397" alt="Data Flow Diagram drawio" src="https://github.com/user-attachments/assets/7007b66f-6f36-42e8-b0a7-79857916f0c2" />

# ⛯ Orchestration ⛯

The pipeline is orchestrated with Apache Airflow, running in Docker via Astro CLI. A single DAG (`load_dwh_pipeline`) runs three tasks in sequence:

```
load_bronze  →  load_silver  →  load_gold
```

- **Schedule**: monthly (`0 2 1 * *`, runs at 02:00 on the 1st of every month)
- **Retries**: 2 attempts with a 5-minute delay on failure
- **Connection**: SQL Server via `SQLExecuteQueryOperator`

<!-- Tempel screenshot Airflow Grid/Graph view yang semua task hijau di sini -->

# ☑ Data Quality & CI/CD Testing ☑

Every push to this repository triggers a GitHub Actions workflow that:

1. Spins up a fresh, temporary SQL Server container
2. Runs the full pipeline (init database, DDL, bronze load, silver load, gold views)
3. Runs automated quality checks (`tests/quality_checks.sql`): row counts, NULL/duplicate checks on primary keys, referential integrity between fact and dimension tables, value range checks

This CI process caught real bugs during development, including:
- A missing comma in `ddl_silver.sql` that would have silently broken table creation
- A copy-paste error where `silver.load_silver` had been named `silver.load_bronze`
- A missing `GO` batch separator in `init_database.sql` that caused `CREATE DATABASE` and `USE` to fail when run as a single script

# 🗠 Consumption Layer 🗠


<!-- Tempel screenshot dashboard Power BI di sini setelah selesai -->
<!-- Jelasin singkat 2-3 insight yang bisa dilihat dari dashboard -->

# ☁️ Cloud (Azure) ☁️

Cloud migration to Azure SQL Database and Azure Blob Storage was set up and validated through schema creation, table deployment, and external data source configuration for Blob-based `BULK INSERT`. Full end-to-end execution was blocked by a shared organizational subscription running out of credit, an environment constraint outside the scope of this project rather than a technical limitation of the approach.

# ✎ᝰ. Challenges & Learnings

A few real problems solved along the way:

- **Airflow + Docker on Windows**: worked through a `docker-compose.yaml` misconfiguration (`build: .` overriding the official image), a Microsoft APT repository GPG signature failure (worked around by installing `sqlcmd` as a standalone binary instead of via `apt`), and eventually migrated to Astro CLI for a more reliable local setup
- **SQL Server authentication**: diagnosed and fixed a named-instance Mixed Mode authentication issue that required checking the SQL Server error log directly, since client-side error messages didn't reveal the real cause
- **CI environment differences**: parameterized the bronze load procedure so the same code path works against a local Windows path and a Linux CI runner, and swapped Windows-style backslashes for forward slashes to keep it portable

# 📂 Project Structure 📂

```
data-warehouse-project-sql/
|
|—— datasets/
|   |—— source_crm/
|   |—— source_erp/
|
|—— docs/
|   |—— data_architecture.png
|   |—— data_flow_diagram.png
|   |—— integration_model.png
|
|—— scripts/
|   |—— bronze/
|   |—— silver/
|   |—— gold/
|   |—— init_database.sql
|
|—— tests/
|   |—— quality_check_silver.sql
|   |—— quality_check_golden.sql
|   |—— quality_checks.sql
|
|—— .github/
|   |—— workflows/
|       |—— test.yml
|
|—— LICENSE
|—— README.md
```

# ▶️ How to Run ▶️

1. Run `scripts/init_database.sql` to create the database and schemas
2. Run the DDL scripts in `scripts/bronze/` and `scripts/silver/` to create tables
3. Deploy the stored procedures in `scripts/bronze/` and `scripts/silver/`
4. Run `scripts/gold/ddl_gold.sql` to create the gold layer views
5. Execute `EXEC bronze.load_bronze;` followed by `EXEC silver.load_silver;`
6. (Optional) Set up Airflow via Astro CLI using the DAG in the orchestration repo to automate steps 5–6 on a schedule

# Author

Zefina Zein" />
