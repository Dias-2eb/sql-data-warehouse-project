# sql-data-warehouse-project
## Data Warehouse and Analytics Project (PostgreSQL + DBeaver) 
This repository showcases a complete data warehouse and analytics solution, implemented in PostgreSQL with DBeaver as the management and visualization tool.
The project follows the Medallion Architecture (Bronze, Silver, Gold) and demonstrates industry best practices in data engineering and analytics. 



## Original Inspiration: This project was inspired by Data with Baraa, where the architecture and workflows were first demonstrated in SQL Server. Here, the project is recreated and extended using PostgreSQL.

## Data Architecture

The project follows the Bronze → Silver → Gold layered architecture:

Bronze Layer: Raw data ingested directly from CSV files into PostgreSQL.

Silver Layer: Cleaned and standardized tables (e.g., handling nulls, applying business rules, ensuring data type consistency).

Gold Layer: Final star schema (fact + dimension tables) optimized for reporting and analytics.
![Data Architecture](docs/Data_model.png)
## Tools & Technologies

Database: PostgreSQL

Client/IDE: DBeaver

ETL: SQL scripts (COPY, INSERT, UPDATE, CASE, window functions)

Data Modeling: Star schema (Fact + Dimension)

Reporting: SQL queries & dashboards in DBeaver (extendable to BI tools like Power BI or Tableau)

## Project Workflow

### Data Ingestion (Bronze)

Loaded raw CSV files into PostgreSQL using COPY.

Maintained raw structure for auditability.

### Data Transformation (Silver)

Standardized column names and formats.

Handled missing/NULL values.

Normalized categorical and reference data.

### Data Modeling (Gold)

Created Fact tables (e.g., Sales, Transactions).

Built Dimension tables (e.g., Customers, Products, Dates).

Implemented relationships for analytical queries.

Analytics & Reporting

Wrote SQL queries for KPIs (e.g., monthly revenue, customer segmentation, product trends).

Created exploratory dashboards in DBeaver.

### Example Outputs

Star Schema Example:

Fact Table: fact_sales

Dimensions: dim_customers, dim_products, dim_date

Sample Queries:

Monthly revenue trends

Top 10 customers by sales

Product performance over time

## How to Run

Clone this repo.

Import CSV data into PostgreSQL (bronze layer).

Run SQL scripts in order: Bronze → Silver → Gold.

Open queries in DBeaver to explore reports.

## Key Learnings

Designed and implemented a modern data warehouse using PostgreSQL.

Applied ETL best practices (data cleansing, transformation, normalization).

Built a star schema for analytics-ready data.

Generated business insights with SQL queries and visualizations.

## This project demonstrates my ability to build end-to-end data pipelines and design analytics-ready data models using open-source tools.
