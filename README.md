# E-Commerce Data Engineering Pipeline

An end-to-end data engineering project that automates the extraction, transformation, storage, processing, and modeling of e-commerce product data.

## Project Architecture

DummyJSON API
→ Pandas
→ AWS S3
→ PostgreSQL
→ PySpark
→ dbt
→ Apache Airflow

## Technologies Used

- Python
- Requests
- Pandas
- AWS S3 (Boto3)
- PostgreSQL
- SQLAlchemy
- PySpark
- Apache Airflow
- dbt
- SQL

## Pipeline

### 1. API Data Extraction
Product data is extracted from the DummyJSON Products API using Python Requests.

### 2. Data Cleaning with Pandas
The raw API data is transformed using Pandas.

Transformations include:
- Handling missing brand values
- Selecting required columns
- Removing duplicate product IDs
- Calculating final product price after discount

### 3. AWS S3 Storage
The cleaned dataset is uploaded to an AWS S3 bucket using Boto3.

### 4. PostgreSQL Loading
The cleaned data is loaded into PostgreSQL using SQLAlchemy.

### 5. PySpark Transformation
PySpark processes the cleaned dataset and calculates an additional `discount_amount` column.

The transformed dataset is stored in Parquet format.

### 6. dbt Modeling and Testing
dbt creates an analytics model from the PostgreSQL products table.

Data quality tests include:
- Unique product ID
- Non-null product ID

### 7. Apache Airflow Orchestration
Apache Airflow orchestrates the complete pipeline.

Task flow:

extract_api
→ pandas_clean
→ upload_to_s3
→ load_postgres
→ pyspark_transform
→ run_dbt

## Data Pipeline Output

The final dataset contains product information including:

- Product ID
- Product title
- Category
- Original price
- Discount percentage
- Rating
- Stock
- Brand
- Final price
- Discount amount

## SQL Analysis

SQL was used to analyze the PostgreSQL dataset using:

- Filtering and aggregation
- GROUP BY
- Aggregate functions
- CTEs
- ROW_NUMBER()
- RANK()
- PARTITION BY
- Category-level product analysis

## Project Goal

The goal of this project is to demonstrate an end-to-end data engineering workflow that automatically moves product data from an external API through cleaning, cloud storage, database loading, distributed processing, transformation, testing, and workflow orchestration.