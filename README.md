# GCP Retail Data Engineering Pipeline

A production-style retail data engineering pipeline built using **Python, PySpark, Google Cloud Storage (GCS), and BigQuery**.

The project demonstrates an end-to-end cloud data engineering workflow:

- Raw data ingestion into Google Cloud Storage
- PySpark-based data transformation
- Data quality validation
- Incremental loading using `order_id`
- BigQuery partitioning
- BigQuery clustering
- Retail sales analytics
- Automated testing with PyTest
- Google Cloud authentication using Application Default Credentials (ADC)

---

## 1. Project Overview

This project simulates a retail data engineering platform on Google Cloud.

Retail sales data is first stored in a **raw layer in Google Cloud Storage**.

PySpark then:

1. Reads the raw sales data
2. Transforms the data
3. Calculates sales metrics
4. Performs data quality validation
5. Identifies new records
6. Loads only new records into BigQuery
7. Refreshes analytical tables

The main production sales table is optimized using:

- **Partitioning by `order_date`**
- **Clustering by `category` and `store_id`**

---

# 2. Architecture

```text
                         Google Cloud
                              |
                              v
                 +------------------------+
                 | Google Cloud Storage   |
                 |       Raw Layer        |
                 |                        |
                 | raw/retail/sales/      |
                 | sales.csv              |
                 +-----------+------------+
                             |
                             v
                 +------------------------+
                 |        PySpark         |
                 |                        |
                 | Read + Transform       |
                 +-----------+------------+
                             |
                             v
                 +------------------------+
                 |     Data Quality       |
                 |                        |
                 | Null checks             |
                 | Quantity validation     |
                 | Price validation        |
                 | Discount validation     |
                 | Duplicate validation    |
                 +-----------+------------+
                             |
                             v
                 +------------------------+
                 |  Incremental Processing|
                 |                        |
                 | Existing order_id check |
                 +-----------+------------+
                             |
                             v
        +------------------------------------------------+
        |                    BigQuery                     |
        |                                                 |
        | sales_partitioned                               |
        |   Partition: order_date                         |
        |   Cluster: category, store_id                   |
        |                                                 |
        | daily_sales                                     |
        | category_sales                                  |
        | store_sales                                     |
        | payment_method_sales                            |
        +------------------------------------------------+
		
3. End-to-End Data Flow

GCS Raw Data
     |
     v
PySpark
     |
     +--> Data Transformation
     |
     +--> Data Quality
     |
     +--> Sales Calculations
     |
     v
Incremental Order ID Check
     |
     v
BigQuery
     |
     +--> sales_partitioned
     |
     +--> daily_sales
     |
     +--> category_sales
     |
     +--> store_sales
     |
     +--> payment_method_sales
	 
4. Technologies
| Technology            | Version / Purpose    |
| --------------------- | -------------------- |
| Python                | 3.12.7               |
| PySpark               | 4.2.0                |
| Py4J                  | 0.10.9.9             |
| Pandas                | 2.3.3                |
| PyArrow               | 25.0.0               |
| Google Cloud BigQuery | 3.43.0               |
| Google Cloud Storage  | GCS Python client    |
| PyTest                | 9.1.1                |
| Google Cloud Platform | Cloud infrastructure |


5. Google Cloud Configuration
GCP Project
vast-falcon-415411
BigQuery Dataset
retail_analytics
GCS Bucket
vast-falcon-415411-retail-raw
GCS Raw Data Path
gs://vast-falcon-415411-retail-raw/raw/retail/sales/sales.csv

6. GCS Raw Data Layer

The raw retail sales data is stored in Google Cloud Storage.

Bucket structure:

vast-falcon-415411-retail-raw/
|
+-- raw/
    |
    +-- retail/
        |
        +-- sales/
            |
            +-- sales.csv

The raw file contains:

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

Current sample dataset:
16 records


7. Source Data

Example source record:
order_id,order_date,customer_id,product_id,category,quantity,unit_price,discount,store_id,payment_method
10016,2026-07-08,C001,P016,Electronics,1,30000,0.05,S002,UPI

For order 10016:

Quantity       = 1
Unit Price     = 30,000
Discount       = 5%

Calculated values:

Gross Sales       = 30,000
Discount Amount   = 1,500
Net Sales         = 28,500

8. PySpark Transformation

The PySpark transformation layer performs the following operations.

Date Conversion
order_date

is converted into a Spark date.

Numeric Conversion

The following columns are converted to numeric types:

quantity
unit_price
discount
Gross Sales
gross_sales =
quantity * unit_price
Discount Amount
discount_amount =
quantity * unit_price * discount
Net Sales
net_sales =
quantity * unit_price * (1 - discount)
9. Data Quality

The pipeline validates the source data before loading it into BigQuery.

Null Checks

The following columns are checked:

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
Quantity Validation

The pipeline checks for invalid quantity values.

Price Validation

The pipeline checks for invalid or non-positive prices.

Discount Validation

The pipeline validates discount values.

Duplicate Validation

The pipeline checks for duplicate:

order_id
10. Data Quality Result

The current dataset successfully passed the quality checks.

Overall status: PASS

Null checks:
order_id: 0
order_date: 0
customer_id: 0
product_id: 0
category: 0
quantity: 0
unit_price: 0
discount: 0
store_id: 0
payment_method: 0

Invalid quantity: 0
Invalid price: 0
Invalid discount: 0
Duplicate orders: 0

Total quality errors: 0
11. Incremental Loading

The pipeline implements incremental loading using:

order_id

Before inserting records into BigQuery, the pipeline retrieves existing order IDs from the production table.

Only records that do not already exist are loaded.

Example

Initial state:

BigQuery sales_partitioned = 15 rows

New source record:

order_id = 10016

Pipeline result:

New sales loaded: 1
Sales table rows: 16

When the pipeline runs again:

New sales loaded: 0
Sales table rows: 16

This prevents duplicate order ingestion.

12. BigQuery Production Table

Production sales table:

vast-falcon-415411.retail_analytics.sales_partitioned

Current row count:

16
13. BigQuery Partitioning

The production sales table is partitioned by:

order_date

Partition type:

DAY

Configuration:

Partition field:
order_date

Partition type:
DAY

Partitioning allows queries filtered by date to process only relevant partitions instead of scanning the entire table.

14. BigQuery Clustering

The production sales table is clustered by:

category
store_id

Configuration:

Cluster fields:
category
store_id

This is useful for retail queries that frequently filter or group data by category and store.

15. BigQuery Sales Schema
order_id: INT64 REQUIRED
order_date: DATE REQUIRED
customer_id: STRING REQUIRED
product_id: STRING REQUIRED
category: STRING REQUIRED
quantity: INT64 REQUIRED
unit_price: NUMERIC REQUIRED
discount: NUMERIC REQUIRED
store_id: STRING REQUIRED
payment_method: STRING REQUIRED
gross_sales: NUMERIC REQUIRED
discount_amount: NUMERIC REQUIRED
net_sales: NUMERIC REQUIRED
16. BigQuery Analytical Tables

The pipeline produces four analytical tables.

Daily Sales
vast-falcon-415411.retail_analytics.daily_sales

Current rows:

8

Columns:

order_date
order_count
total_quantity
gross_sales
total_discount
net_sales
Category Sales
vast-falcon-415411.retail_analytics.category_sales

Current rows:

4

Categories:

Clothing
Electronics
Furniture
Home Appliances

Columns:

category
order_count
total_quantity
net_sales
Store Sales
vast-falcon-415411.retail_analytics.store_sales

Current rows:

3

Columns:

store_id
order_count
total_quantity
net_sales
Payment Method Sales
vast-falcon-415411.retail_analytics.payment_method_sales

Current rows:

4

Columns:

payment_method
order_count
net_sales
17. Current BigQuery Results

Latest successful pipeline execution:

============================================================
PIPELINE COMPLETED SUCCESSFULLY
============================================================

Production sales table: sales_partitioned
New sales loaded: 0
Sales table rows: 16
Daily sales rows: 8
Category sales rows: 4
Store sales rows: 3
Payment method sales rows: 4

Project: vast-falcon-415411
Dataset: retail_analytics
============================================================
18. Project Structure
gcp-retail-data-engineering-pipeline/
|
+-- data/
|   |
|   +-- sales.csv
|
+-- sql/
|   |
|   +-- analytics.sql
|
+-- src/
|   |
|   +-- analytics.py
|   +-- bigquery_loader.py
|   +-- data_quality.py
|   +-- pipeline.py
|   +-- transform.py
|   +-- setup_partitioned_sales.py
|   +-- test_bigquery_connection.py
|
+-- tests/
|   |
|   +-- test_bigquery_incremental.py
|   +-- test_data_quality.py
|   +-- test_transform.py
|
+-- .gitignore
+-- README.md
+-- requirements.txt
19. Source Code Components
transform.py

Responsible for:

Creating the Spark session
Loading source data
Reading data from GCS
Supporting local CSV fallback
Transforming sales data
Calculating gross sales
Calculating discounts
Calculating net sales
data_quality.py

Responsible for:

Null validation
Quantity validation
Price validation
Discount validation
Duplicate validation
Data quality reporting
analytics.py

Responsible for generating:

daily_sales
category_sales
store_sales
payment_method_sales
bigquery_loader.py

Responsible for:

BigQuery client creation
Schema definitions
Production table validation
Incremental order ID detection
New-record loading
Analytical table loading
BigQuery partition/clustering validation
pipeline.py

Orchestrates the complete pipeline:

Load
  |
Transform
  |
Quality Check
  |
Analytics
  |
Incremental Check
  |
BigQuery Load
setup_partitioned_sales.py

Responsible for creating and validating:

sales_partitioned

with:

Partition:
order_date

Clusters:
category
store_id
20. Data Source Configuration

The pipeline supports both GCS and local data sources.

GCS
RETAIL_DATA_SOURCE=GCS

GCS path:

gs://vast-falcon-415411-retail-raw/raw/retail/sales/sales.csv
Local
RETAIL_DATA_SOURCE=LOCAL

Local path:

data/sales.csv

GCS is the default source for the production-style pipeline.

The local CSV remains available as a development/test fallback.

21. Google Cloud Authentication

The project uses Google Cloud Application Default Credentials.

Authenticate using:

gcloud auth application-default login

Verify the configured project:

gcloud config list

Expected project:

vast-falcon-415411
22. BigQuery Connection Test

Run:

python src\test_bigquery_connection.py

Expected result:

BigQuery client: 3.43.0

BigQuery connection successful!
Project: vast-falcon-415411
Dataset: retail_analytics
23. Install Dependencies

Create and activate the virtual environment.

Windows:

python -m venv .venv

Activate:

.venv\Scripts\activate

Install dependencies:

pip install -r requirements.txt
24. Requirements

The project uses:

pyspark==4.2.0
py4j==0.10.9.9
pandas==2.3.3
pyarrow==25.0.0
google-cloud-bigquery==3.43.0
google-cloud-storage
pytest==9.1.1
25. Running the Pipeline

From the project root:

python src\pipeline.py

The pipeline performs:

1. Load source data
2. Transform sales data
3. Run data quality checks
4. Generate analytics
5. Check existing BigQuery order IDs
6. Load only new records
7. Refresh analytics tables
8. Print pipeline summary
26. Running with GCS

GCS is the default data source.

Run:

python src\pipeline.py

The pipeline reads:

gs://vast-falcon-415411-retail-raw/raw/retail/sales/sales.csv

and processes the data using PySpark.

27. Running with Local Data

For local development:

set RETAIL_DATA_SOURCE=LOCAL
python src\pipeline.py

To use GCS again:

set RETAIL_DATA_SOURCE=GCS
python src\pipeline.py
28. Run Individual Components
Transformation
python src\transform.py
Analytics
python src\analytics.py
Data Quality
python src\data_quality.py
BigQuery Loader
python src\bigquery_loader.py
Complete Pipeline
python src\pipeline.py
29. Automated Testing

Run all tests:

python -m pytest tests -v

Latest result:

9 passed
30. Test Coverage
Incremental Loading Tests
test_existing_orders_are_skipped
test_new_order_is_loaded
test_no_duplicate_orders
test_all_existing_orders_return_empty

These verify the incremental processing logic.

BigQuery Configuration Tests
test_partition_configuration
test_clustering_configuration

These verify the intended production table configuration.

Data Quality Tests
test_quality_checks_pass
test_invalid_discount_fails

These verify successful and failing data-quality scenarios.

Transformation Test
test_sales_transformation

This verifies the PySpark sales calculations.

31. Latest Test Result
=========================== test session starts ===========================

platform win32
Python 3.12.7
pytest 9.1.1

collected 9 items

9 passed in 63.58s

============================ 9 passed ==============================
32. SQL Analytics

SQL queries are maintained in:

sql/analytics.sql

Example daily sales query:

SELECT
    order_date,
    SUM(net_sales) AS total_net_sales
FROM
    `vast-falcon-415411.retail_analytics.sales_partitioned`
GROUP BY
    order_date
ORDER BY
    order_date;

Example category sales query:

SELECT
    category,
    SUM(net_sales) AS total_net_sales
FROM
    `vast-falcon-415411.retail_analytics.sales_partitioned`
GROUP BY
    category
ORDER BY
    total_net_sales DESC;

Example store sales query:

SELECT
    store_id,
    SUM(net_sales) AS total_net_sales
FROM
    `vast-falcon-415411.retail_analytics.sales_partitioned`
GROUP BY
    store_id
ORDER BY
    total_net_sales DESC;
33. Production Features

This project demonstrates the following production-oriented data engineering practices.

Cloud Raw Layer

Raw data is stored in:

Google Cloud Storage

before processing.

Distributed Processing

PySpark is used for:

Data transformation
Data processing
Aggregation
Data Quality

The pipeline validates:

Null values
Invalid quantities
Invalid prices
Invalid discounts
Duplicate orders
Incremental Processing

Existing order_id values are checked before inserting records.

BigQuery Partitioning

Production sales data is partitioned by:

order_date
BigQuery Clustering

Production sales data is clustered by:

category
store_id
Automated Testing

PyTest validates:

Transformations
Data quality
Incremental logic
Partition configuration
Clustering configuration
34. Windows Development Notes

This project was developed and tested locally on Windows.

PySpark may display warnings such as:

Did not find winutils.exe

and:

Unable to load native-hadoop library

These are related to the local Windows Spark environment.

They do not prevent the current pipeline from successfully processing the data.

The GCS integration uses the Google Cloud Storage Python client to download the raw CSV before PySpark processes it. This avoids requiring a Hadoop GCS connector for the local Windows development environment.

35. GCS Processing Design

The local development flow uses:

Google Cloud Storage
        |
        v
Google Cloud Storage Python SDK
        |
        v
Temporary local CSV
        |
        v
PySpark

This design was selected to keep local Windows development reliable while still using GCS as the cloud raw-data layer.

36. BigQuery Optimization

The production sales table uses:

Partition:
order_date

Clustering:
category
store_id

This design is intended to improve query efficiency for common retail analytics patterns such as:

Sales by date
Sales by category
Sales by store
Sales by date and category
Sales by date and store
37. Billing and Cost Awareness

This project uses Google Cloud services including:

Google Cloud Storage
BigQuery

GCP resources can incur charges depending on usage and billing configuration.

Recommended practices:

Keep development datasets small
Avoid unnecessary large queries
Monitor BigQuery usage
Monitor Cloud Storage usage
Configure billing budgets and alerts
Delete unused resources when they are no longer required

The current portfolio dataset is intentionally small.

38. Project Development Phases
Phase 1 — Local PySpark Pipeline

Status:

COMPLETE

Implemented:

PySpark transformations
Sales calculations
Data quality validation
Analytics
Automated tests
Phase 2 — BigQuery Integration

Status:

COMPLETE

Implemented:

BigQuery client
BigQuery schemas
BigQuery loading
Analytical tables
ADC authentication
Phase 3 — Incremental BigQuery Processing

Status:

COMPLETE

Implemented:

Existing order ID detection
New-record loading
Duplicate prevention
Partitioned sales table
Clustered sales table
Automated incremental tests
Phase 4 — GCS Raw Data Layer

Status:

COMPLETE

Implemented:

GCS bucket
Raw data folder structure
Raw sales CSV
Google Cloud Storage Python client
GCS-to-PySpark integration
GCS as default source
Local CSV fallback
39. Current End-to-End Architecture
                  Google Cloud Storage
                           |
                           v
             raw/retail/sales/sales.csv
                           |
                           v
              Google Cloud Storage SDK
                           |
                           v
                        PySpark
                           |
              +------------+------------+
              |                         |
              v                         v
       Transformations            Data Quality
              |                         |
              +------------+------------+
                           |
                           v
                  Incremental Processing
                       order_id
                           |
                           v
               BigQuery sales_partitioned
                           |
                +----------+----------+
                |                     |
                v                     v
           Partitioning          Clustering
           order_date          category/store_id
                |
                v
             Analytics
                |
       +--------+--------+---------+
       |        |        |         |
       v        v        v         v
     Daily   Category  Store   Payment
     Sales    Sales    Sales   Method Sales
40. Validation Summary

The complete pipeline has been successfully validated.

Current source records:

16

Current production BigQuery records:

16

Current analytics records:

daily_sales: 8
category_sales: 4
store_sales: 3
payment_method_sales: 4

Incremental loading:

New sales loaded: 0

Data quality:

PASS

Automated tests:

9 passed

Partitioning:

order_date

Clustering:

category
store_id
41. Project Status
============================================================
PROJECT 3 STATUS
============================================================

GCS Raw Layer             : COMPLETE
PySpark Transformation   : COMPLETE
Data Quality             : COMPLETE
BigQuery Integration     : COMPLETE
Incremental Loading      : COMPLETE
Partitioning             : COMPLETE
Clustering               : COMPLETE
Analytics                : COMPLETE
Automated Tests          : 9 PASSED
End-to-End Pipeline      : PASS

============================================================
42. Future Enhancements

Potential next improvements include:

Orchestration
Apache Airflow
Cloud Composer
Cloud Scheduler
Event-driven processing
GCS
Multiple file ingestion
File arrival validation
Archive layer
GCS lifecycle policies
Date-partitioned raw folders
BigQuery
MERGE-based incremental processing
Partition expiration
Advanced clustering
Slowly Changing Dimensions
Cost optimization
Monitoring
Cloud Logging
Cloud Monitoring
Pipeline failure alerts
Data quality dashboards
CI/CD
GitHub Actions
Automated PyTest execution
Code quality checks
Automated deployment
Analytics
Looker Studio dashboard
Retail KPI dashboard
Revenue trends
Category performance
Store performance
Payment method analysis
43. Author

Ramakrishna Vulli

Data Engineer

Core technologies demonstrated:

Python
PySpark
Google Cloud Storage
BigQuery
SQL
Data Quality
ETL / ELT
Incremental Data Processing
Partitioning
Clustering
PyTest
Google Cloud Platform
44. Conclusion

This project demonstrates a complete cloud-based retail data engineering pipeline.

The pipeline:

1. Stores raw data in GCS
2. Reads the raw data using the GCS Python client
3. Processes data using PySpark
4. Performs data quality validation
5. Calculates retail sales metrics
6. Detects previously loaded order IDs
7. Loads only new records into BigQuery
8. Uses partitioning and clustering for the production sales table
9. Generates analytical tables
10. Validates functionality using automated tests

The result is a scalable, testable, and production-oriented GCP retail data engineering architecture.

