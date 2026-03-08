# Data Warehouse SQL Project
Designing and analyzing a data warehouse for a bicycle retailer using CRM and ERP datasets.

# ⟡ Project Overview ⟡
This project builds a data warehouse from CRM and ERP datasets of a bicycle retail company.
The data is extracted, transformed, and loaded (ETL) to create a structured and analysis-ready environment for business insights.

* The warehouse enables analysis such as:
* sales performance
* product trends
* regional transactions
* store profitability

Using a data warehouse allows the data to be organized in a structured schema that supports efficient analytical queries and reporting.

# 🗂️ Dataset 🗂️
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
<img width="1502" height="841" alt="data_architecture" src="https://github.com/user-attachments/assets/e840f596-31b5-4f0a-a90d-646e337f9519" 
/>

# ☆ Data Schema ☆
Type: Star

#### Fact Table
* fact_sales

#### Dimension Tables
* dim_customer
* dim_product

# 📊 Data Flow 📊
<img width="640" height="397" alt="Data Flow Diagram drawio" src="https://github.com/user-attachments/assets/7007b66f-6f36-42e8-b0a7-79857916f0c2" />

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
|
|—— LICENSE
|—— README.md
```

# Author
Zefina Zein








