# GCP Retail Data Engineering Pipeline

An end-to-end retail data engineering pipeline built with **Python, PySpark, Google BigQuery, and Google Cloud**.

The project demonstrates how raw retail sales data can be ingested, transformed, validated, analyzed, and loaded into BigQuery for analytical use.

---

## Project Overview

This project implements a complete retail data engineering workflow:

```text
Raw Retail CSV
      |
      v
   PySpark
      |
      v
Data Transformation
      |
      v
Data Quality Checks
      |
      v
Business Analytics
      |
      v
BigQuery
      |
      v
SQL Analytics
```

The pipeline processes a sample retail sales dataset containing **15 records** and creates multiple analytical datasets in BigQuery.

---

## Key Features

* PySpark-based ETL pipeline
* Retail sales transformation
* Gross sales calculation
* Discount calculation
* Net sales calculation
* Data quality validation
* Null-value validation
* Quantity validation
* Price validation
* Discount validation
* Duplicate order detection
* Daily sales analytics
* Category-level sales analytics
* Store-level sales analytics
* Payment-method analytics
* Google BigQuery integration
* Automated BigQuery table creation
* Automated batch loading
* BigQuery SQL analytics
* Automated PyTest test suite
* Google Cloud Application Default Credentials authentication

---

## Technology Stack

| Technology            | Version / Purpose |
| --------------------- | ----------------- |
| Python                | 3.12.7            |
| PySpark               | 4.2.0             |
| PyTest                | 9.1.1             |
| Google Cloud BigQuery | 3.43.0            |
| pandas                | 2.3.3             |
| PyArrow               | 25.0.0            |
| Google Cloud SDK      | 579.0.0           |
| BigQuery CLI          | 2.1.36            |
| Git                   | Version control   |

---

## GCP Configuration

### Google Cloud Project

```text
Project ID:
vast-falcon-415411
```

### BigQuery Dataset

```text
retail_analytics
```

The project uses Google Cloud Application Default Credentials (ADC) for authentication.

---

## BigQuery Tables

The pipeline automatically creates and loads five BigQuery tables.

### 1. sales

Contains the transformed retail transaction-level data.

```text
order_id
order_date
customer_id
product_id
category
quantity
unit_price
discount
store_id
payment_method
gross_sales
discount_amount
net_sales
```

Rows loaded:

```text
15
```

---

### 2. daily_sales

Daily sales aggregation.

```text
order_date
order_count
total_quantity
gross_sales
total_discount
net_sales
```

Rows loaded:

```text
8
```

---

### 3. category_sales

Category-level sales aggregation.

```text
category
order_count
total_quantity
net_sales
```

Rows loaded:

```text
4
```

---

### 4. store_sales

Store-level sales aggregation.

```text
store_id
order_count
total_quantity
net_sales
```

Rows loaded:

```text
3
```

---

### 5. payment_method_sales

Payment-method sales aggregation.

```text
payment_method
order_count
net_sales
```

Rows loaded:

```text
4
```

---

## Data Transformation

The pipeline calculates:

### Gross Sales

```text
gross_sales =
quantity × unit_price
```

### Discount Amount

```text
discount_amount =
quantity × unit_price × discount
```

### Net Sales

```text
net_sales =
gross_sales - discount_amount
```

Financial values are converted to appropriate decimal representations for reliable BigQuery `NUMERIC` loading.

---

## Data Quality Checks

The pipeline validates the incoming retail data before loading it into BigQuery.

### Checks implemented

* Null checks
* Invalid quantity checks
* Invalid price checks
* Invalid discount checks
* Duplicate order checks

The current dataset passed all quality checks:

```text
Overall status: PASS

Records checked: 15

Null checks:              PASS
Invalid quantity:        PASS
Invalid price:           PASS
Invalid discount:        PASS
Duplicate orders:        PASS

Total quality errors: 0
```

---

## Analytics

The PySpark analytics layer produces:

### Daily Sales

```text
order_date
order_count
total_quantity
gross_sales
total_discount
net_sales
```

### Category Sales

```text
category
order_count
total_quantity
net_sales
```

### Store Sales

```text
store_id
order_count
total_quantity
net_sales
```

### Payment Method Sales

```text
payment_method
order_count
net_sales
```

---

## BigQuery SQL Analytics

SQL queries are available in:

```text
sql/analytics.sql
```

The SQL layer includes queries for:

* Overall sales summary
* Daily sales performance
* Category performance
* Store performance
* Payment-method performance
* Top orders
* Category sales percentage
* Discount analysis

---

## Project Structure

```text
gcp-retail-data-engineering-pipeline/
|
├── data/
│   └── sales.csv
|
├── sql/
│   └── analytics.sql
|
├── src/
│   ├── __init__.py
│   ├── analytics.py
│   ├── bigquery_loader.py
│   ├── data_quality.py
│   ├── pipeline.py
│   ├── transform.py
│   └── test_bigquery_connection.py
|
├── tests/
│   ├── conftest.py
│   ├── test_data_quality.py
│   └── test_transform.py
|
├── .gitignore
├── README.md
└── requirements.txt
```

---

## Environment Setup

### 1. Clone the repository

```bash
git clone https://github.com/ramakrishna-vulli/gcp-retail-data-engineering-pipeline.git
```

```bash
cd gcp-retail-data-engineering-pipeline
```

---

### 2. Create a virtual environment

Windows:

```bat
python -m venv .venv
```

Activate:

```bat
.venv\Scripts\activate
```

---

### 3. Install dependencies

```bat
python -m pip install -r requirements.txt
```

---

## Google Cloud Authentication

The pipeline uses Application Default Credentials.

Authenticate with:

```bat
gcloud auth application-default login
```

Set the project:

```bat
gcloud config set project vast-falcon-415411
```

Verify:

```bat
gcloud config get-value project
```

Expected:

```text
vast-falcon-415411
```

---

## BigQuery Connection Test

Run:

```bat
python src\test_bigquery_connection.py
```

Expected:

```text
BigQuery connection successful!
Project: vast-falcon-415411
Dataset: retail_analytics
```

---

## Run the Pipeline

The complete ETL and BigQuery workflow can be executed with:

```bat
python src\pipeline.py
```

The pipeline performs:

```text
1. Load source data
2. Transform sales data
3. Run data quality checks
4. Create analytics
5. Load data into BigQuery
```

Successful execution produces:

```text
PIPELINE COMPLETED SUCCESSFULLY
```

---

## Test the Project

Run all automated tests:

```bat
python -m pytest tests -v
```

Current test result:

```text
3 passed
```

Tests cover:

```text
test_quality_checks_pass
test_invalid_discount_fails
test_sales_transformation
```

---

## Verified Pipeline Results

The complete pipeline was successfully executed against BigQuery.

```text
============================================================
PIPELINE COMPLETED SUCCESSFULLY
============================================================

BigQuery tables:

sales: 15 rows
daily_sales: 8 rows
category_sales: 4 rows
store_sales: 3 rows
payment_method_sales: 4 rows

Project: vast-falcon-415411
Dataset: retail_analytics
```

---

## Data Quality Result

```text
Records checked: 15

Overall status: PASS

Null checks: 0
Invalid quantity: 0
Invalid price: 0
Invalid discount: 0
Duplicate orders: 0

Total quality errors: 0
```

---

## Architecture

```text
                    Retail Sales CSV
                           |
                           v
                    +--------------+
                    |    PySpark   |
                    |     ETL      |
                    +------+-------+
                           |
                           v
                    Data Transformation
                           |
                           v
                    Data Quality Checks
                           |
                           v
                       Analytics
                           |
             +-------------+-------------+
             |             |             |
             v             v             v
        Daily Sales   Category Sales  Store Sales
             |             |             |
             +-------------+-------------+
                           |
                           v
                    BigQuery Loader
                           |
                           v
              +--------------------------+
              |       Google BigQuery    |
              |                          |
              |    retail_analytics      |
              |                          |
              |    sales                 |
              |    daily_sales           |
              |    category_sales        |
              |    store_sales            |
              |    payment_method_sales  |
              +--------------------------+
                           |
                           v
                    BigQuery SQL
                       Analytics
```

---

## Engineering Concepts Demonstrated

This project demonstrates practical experience with:

### Data Engineering

* ETL pipelines
* Batch processing
* Data transformation
* Data validation
* Data quality
* Aggregations
* Analytical data modeling

### PySpark

* Spark DataFrames
* Transformations
* Aggregations
* Decimal data types
* Local Spark execution
* PySpark testing

### Google Cloud

* Google Cloud project configuration
* Application Default Credentials
* BigQuery
* BigQuery schemas
* BigQuery batch loading
* BigQuery SQL

### Software Engineering

* Modular Python code
* Automated tests
* Requirements management
* Git version control
* Error handling
* Reusable functions
* Pipeline orchestration

---

## Important Notes

This project uses a small synthetic dataset for portfolio and demonstration purposes.

The pipeline uses:

```text
PySpark → pandas → PyArrow → BigQuery
```

for the BigQuery batch-loading stage.

The dataset is intentionally small, making this approach appropriate for this portfolio project.

For large production datasets, the pipeline could be extended to use distributed cloud-native loading approaches such as:

* Cloud Storage
* BigQuery load jobs
* Dataflow
* Dataproc
* Cloud Composer
* BigQuery Storage Write API

---

## Future Enhancements

Potential production improvements include:

* Google Cloud Storage ingestion
* Cloud Composer / Airflow orchestration
* Incremental processing
* Partitioned BigQuery tables
* BigQuery clustering
* Data lineage
* Cloud Logging
* Cloud Monitoring
* CI/CD using GitHub Actions
* Infrastructure as Code using Terraform
* Service-account based authentication
* Automated deployment
* BigQuery scheduled queries
* Looker Studio dashboard

---

## Portfolio Summary

This project demonstrates an end-to-end **GCP retail data engineering pipeline** using PySpark and BigQuery.

The pipeline successfully:

```text
15 source records
       ↓
PySpark transformation
       ↓
Data quality validation
       ↓
Business analytics
       ↓
BigQuery
       ↓
5 analytical tables
       ↓
BigQuery SQL analytics
```

All automated tests currently pass, and the complete pipeline has been successfully executed against Google BigQuery.
