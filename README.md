# GCP Retail Data Engineering Pipeline

![CI](https://github.com/ramakrishna-vulli/gcp-retail-data-engineering-pipeline/actions/workflows/ci.yml/badge.svg)

**Python | PySpark | GCS | BigQuery | Data Quality | Incremental ETL | Orchestration**

Production-style retail data engineering pipeline with automated testing and GitHub Actions CI.

---

## Project Overview

A production-style retail data engineering pipeline built using Python, PySpark, Google Cloud Storage (GCS), and BigQuery.

The project demonstrates an end-to-end cloud data engineering workflow including:

- Raw data ingestion into Google Cloud Storage
- PySpark-based data transformation
- Data quality validation
- Retail sales calculations
- Incremental loading using `order_id`
- BigQuery partitioning
- BigQuery clustering
- Retail sales analytics
- Lightweight pipeline orchestration
- Task dependency management
- Retry handling
- Automated testing with PyTest
- GitHub Actions CI
- Google Cloud authentication using Application Default Credentials (ADC)

---

## 1. Architecture

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
                 | Incremental Processing |
                 |                        |
                 | Existing order_id check |
                 +-----------+------------+
                             |
                             v
                 +------------------------+
                 |       BigQuery         |
                 |                        |
                 | sales_partitioned      |
                 |                        |
                 | Partition: order_date  |
                 | Cluster: category,     |
                 |           store_id      |
                 +-----------+------------+
                             |
                             v
                 +------------------------+
                 |      Analytics         |
                 |                        |
                 | daily_sales            |
                 | category_sales         |
                 | store_sales            |
                 | payment_method_sales   |
                 +-----------+------------+
                             |
                             v
                 +------------------------+
                 |    Orchestration       |
                 |                        |
                 | Task execution         |
                 | Dependencies           |
                 | Retry handling         |
                 | Logging                |
                 +------------------------+
```

---

## 2. End-to-End Data Flow

```text
GCS Raw Data
     |
     v
Load Source
     |
     v
PySpark Transformation
     |
     v
Data Quality
     |
     v
Create Analytics
     |
     v
BigQuery Connection
     |
     v
Incremental Order ID Check
     |
     v
Load New Records
     |
     v
Analytics Tables
     |
     v
Pipeline Cleanup
```

---

## 3. Technologies

| Technology | Version / Purpose |
|---|---|
| Python | 3.12.7 |
| PySpark | 4.2.0 |
| Py4J | 0.10.9.9 |
| Pandas | 2.3.3 |
| PyArrow | 25.0.0 |
| Google Cloud BigQuery | 3.43.0 |
| Google Cloud Storage | GCS Python client |
| PyTest | 9.1.1 |
| GitHub Actions | CI |
| Google Cloud Platform | Cloud infrastructure |

---

## 4. Google Cloud Configuration

### GCP Project

```text
vast-falcon-415411
```

### BigQuery Dataset

```text
retail_analytics
```

### GCS Bucket

```text
vast-falcon-415411-retail-raw
```

### GCS Raw Data Path

```text
gs://vast-falcon-415411-retail-raw/raw/retail/sales/sales.csv
```

---

## 5. GCS Raw Data Layer

The raw retail sales data is stored in Google Cloud Storage.

Bucket structure:

```text
vast-falcon-415411-retail-raw/
|
+-- raw/
    |
    +-- retail/
        |
        +-- sales/
            |
            +-- sales.csv
```

The raw file contains:

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
```

Current sample dataset:

```text
16 records
```

---

## 6. Source Data

Example source record:

```csv
order_id,order_date,customer_id,product_id,category,quantity,unit_price,discount,store_id,payment_method
10016,2026-07-08,C001,P016,Electronics,1,30000,0.05,S002,UPI
```

For order `10016`:

```text
Quantity       = 1
Unit Price     = 30,000
Discount       = 5%
```

Calculated values:

```text
Gross Sales       = 30,000
Discount Amount   = 1,500
Net Sales         = 28,500
```

---

## 7. PySpark Transformation

The transformation layer performs the following operations.

### Date Conversion

`order_date` is converted into a Spark date.

### Numeric Conversion

The following columns are converted to numeric types:

```text
quantity
unit_price
discount
```

### Gross Sales

```text
gross_sales = quantity * unit_price
```

### Discount Amount

```text
discount_amount = quantity * unit_price * discount
```

### Net Sales

```text
net_sales = quantity * unit_price * (1 - discount)
```

---

## 8. Data Quality

The pipeline validates the source data before loading it into BigQuery.

### Null Checks

The following columns are checked:

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
```

### Quantity Validation

The pipeline checks for invalid quantity values.

### Price Validation

The pipeline checks for invalid or non-positive prices.

### Discount Validation

The pipeline validates discount values.

### Duplicate Validation

The pipeline checks for duplicate `order_id` values.

---

## 9. Data Quality Result

The current dataset successfully passed the quality checks.

```text
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
```

---

## 10. Incremental Loading

The pipeline implements incremental loading using:

```text
order_id
```

Before inserting records into BigQuery, the pipeline retrieves existing order IDs from the production table.

Only records that do not already exist are loaded.

### Example

Initial state:

```text
BigQuery sales_partitioned = 15 rows
```

New source record:

```text
order_id = 10016
```

Pipeline result:

```text
New sales loaded: 1
Sales table rows: 16
```

When the pipeline runs again:

```text
New sales loaded: 0
Sales table rows: 16
```

This prevents duplicate order ingestion.

---

## 11. BigQuery Production Table

Production sales table:

```text
vast-falcon-415411.retail_analytics.sales_partitioned
```

Current row count:

```text
16
```

---

## 12. BigQuery Partitioning

The production sales table is partitioned by:

```text
order_date
```

Partition type:

```text
DAY
```

Configuration:

```text
Partition field:
order_date

Partition type:
DAY
```

Partitioning allows date-based queries to work against relevant date partitions rather than requiring a scan of the entire table.

---

## 13. BigQuery Clustering

The production sales table is clustered by:

```text
category
store_id
```

This supports common retail analytics queries involving category and store filtering/grouping.

---

## 14. BigQuery Sales Schema

```text
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
```

---

## 15. BigQuery Analytical Tables

The pipeline produces four analytical tables.

### Daily Sales

```text
vast-falcon-415411.retail_analytics.daily_sales
```

Current rows:

```text
8
```

Columns:

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
vast-falcon-415411.retail_analytics.category_sales
```

Current rows:

```text
4
```

Categories:

```text
Clothing
Electronics
Furniture
Home Appliances
```

### Store Sales

```text
vast-falcon-415411.retail_analytics.store_sales
```

Current rows:

```text
3
```

### Payment Method Sales

```text
vast-falcon-415411.retail_analytics.payment_method_sales
```

Current rows:

```text
4
```

---

## 16. Current BigQuery Results

Latest successful pipeline execution:

```text
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
```

---

## 17. Pipeline Orchestration

The project includes a lightweight local orchestration layer.

The orchestration layer separates the pipeline into reusable tasks.

```text
Task 1
Create Spark Session
        |
        v
Task 2
Load Source Data
        |
        v
Task 3
Transform Sales
        |
        v
Task 4
Data Quality
        |
        v
Task 5
Create Analytics
        |
        v
Task 6
Create BigQuery Client
        |
        v
Task 7
Filter New Sales
        |
        v
Task 8
Load BigQuery
        |
        v
Task 9
Cleanup
```

---

## 18. Orchestration Components

### `src/tasks.py`

Contains reusable pipeline task functions.

Tasks include:

```text
task_create_spark
task_load_source
task_transform
task_data_quality
task_create_analytics
task_create_bigquery_client
task_filter_new_sales
task_load_bigquery
task_cleanup
```

A shared `PipelineContext` is used to maintain state while tasks execute within the same Python process.

### `src/orchestrator.py`

Responsible for:

- Task execution
- Task dependency/order
- Retry handling
- Task-level logging
- Pipeline failure handling
- Pipeline summary
- Cleanup

The orchestrator uses a maximum of two retries after the initial attempt.

---

## 19. Orchestration Retry Handling

Configuration:

```text
Maximum retries:
2

Total possible attempts:
3

Retry delay:
5 seconds
```

Example:

```text
Attempt 1
   |
   X Failure
   |
Attempt 2
   |
   X Failure
   |
Attempt 3
   |
   v
Success / Final Failure
```

---

## 20. Orchestrator Execution

Run:

```bat
python -m src.orchestrator
```

Latest successful execution:

```text
======================================================================
PIPELINE COMPLETED SUCCESSFULLY
======================================================================

Source records: 16
Transformed records: 16
New records: 0

======================================================================
```

The Spark cleanup task also completed successfully.

---

## 21. Airflow-Compatible Design

The project also contains:

```text
dags/
└── retail_pipeline_dag.py
```

The DAG is designed to reuse the existing pipeline logic rather than duplicating business logic.

The intended architecture is:

```text
Airflow
   |
   v
retail_pipeline_dag.py
   |
   v
src/pipeline.py
   |
   +--> GCS
   +--> PySpark
   +--> Data Quality
   +--> Analytics
   +--> BigQuery
```

Airflow is not installed in the current Windows development environment.

Docker is also not required for the current local implementation.

The lightweight orchestrator provides the task-oriented architecture while keeping the existing development environment simple.

---

## 22. Project Structure

```text
gcp-retail-data-engineering-pipeline/
|
+-- .github/
|   |
|   +-- workflows/
|       |
|       +-- ci.yml
|
+-- data/
|   |
|   +-- sales.csv
|
+-- dags/
|   |
|   +-- retail_pipeline_dag.py
|
+-- sql/
|   |
|   +-- analytics.sql
|
+-- src/
|   |
|   +-- __init__.py
|   +-- analytics.py
|   +-- bigquery_loader.py
|   +-- data_quality.py
|   +-- orchestrator.py
|   +-- pipeline.py
|   +-- setup_partitioned_sales.py
|   +-- tasks.py
|   +-- test_bigquery_connection.py
|   +-- transform.py
|
+-- tests/
|   |
|   +-- test_bigquery_incremental.py
|   +-- test_data_quality.py
|   +-- test_orchestrator.py
|   +-- test_transform.py
|
+-- .gitignore
+-- README.md
+-- requirements.txt
```

---

## 23. Source Code Components

### `src/transform.py`

Responsible for:

- Creating the Spark session
- Loading source data
- Reading data from GCS
- Supporting local CSV fallback
- Transforming sales data
- Calculating gross sales
- Calculating discounts
- Calculating net sales

### `src/data_quality.py`

Responsible for:

- Null validation
- Quantity validation
- Price validation
- Discount validation
- Duplicate validation
- Data quality reporting

### `src/analytics.py`

Responsible for generating:

```text
daily_sales
category_sales
store_sales
payment_method_sales
```

### `src/bigquery_loader.py`

Responsible for:

- BigQuery client creation
- BigQuery schemas
- Production table validation
- Incremental order ID detection
- New-record loading
- Analytical table loading
- Partition validation
- Clustering validation

### `src/pipeline.py`

The original end-to-end pipeline.

It performs:

```text
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
```

### `src/tasks.py`

Provides reusable functions for each pipeline stage.

This makes the pipeline easier to orchestrate and provides a foundation for future Airflow task mapping.

### `src/orchestrator.py`

Provides lightweight local orchestration with:

```text
Task dependencies
Retry handling
Logging
Failure handling
Cleanup
```

### `src/setup_partitioned_sales.py`

Responsible for creating and validating:

```text
sales_partitioned
```

with:

```text
Partition:
order_date

Clusters:
category
store_id
```

---

## 24. Data Source Configuration

The pipeline supports two source modes.

### GCS

```text
RETAIL_DATA_SOURCE=GCS
```

GCS path:

```text
gs://vast-falcon-415411-retail-raw/raw/retail/sales/sales.csv
```

### Local

```text
RETAIL_DATA_SOURCE=LOCAL
```

Local path:

```text
data/sales.csv
```

GCS is the default source for the production-style pipeline.

The local CSV remains available as a development/test fallback.

---

## 25. Google Cloud Authentication

The project uses Google Cloud Application Default Credentials.

Authenticate using:

```bat
gcloud auth application-default login
```

Verify the configured project:

```bat
gcloud config list
```

Expected project:

```text
vast-falcon-415411
```

---

## 26. BigQuery Connection Test

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

## 27. Install Dependencies

Create a virtual environment:

```bat
python -m venv .venv
```

Activate:

```bat
.venv\Scripts\activate
```

Install dependencies:

```bat
pip install -r requirements.txt
```

---

## 28. Running the Standard Pipeline

From the project root:

```bat
python src\pipeline.py
```

The pipeline performs:

```text
1. Load source data
2. Transform sales data
3. Run data quality checks
4. Generate analytics
5. Check existing BigQuery order IDs
6. Load only new records
7. Refresh analytics tables
8. Print pipeline summary
```

---

## 29. Running the Orchestrated Pipeline

Run:

```bat
python -m src.orchestrator
```

The orchestrator executes:

```text
create_spark_session
        |
load_source_data
        |
transform_sales
        |
data_quality
        |
create_analytics
        |
create_bigquery_client
        |
filter_new_sales
        |
load_bigquery
        |
cleanup
```

---

## 30. Running with GCS

GCS is the default data source.

Run:

```bat
python src\pipeline.py
```

or:

```bat
python -m src.orchestrator
```

The pipeline reads:

```text
gs://vast-falcon-415411-retail-raw/raw/retail/sales/sales.csv
```

and processes the data using PySpark.

---

## 31. Running with Local Data

For local development:

```bat
set RETAIL_DATA_SOURCE=LOCAL
python src\pipeline.py
```

Or:

```bat
set RETAIL_DATA_SOURCE=LOCAL
python -m src.orchestrator
```

To return to GCS:

```bat
set RETAIL_DATA_SOURCE=GCS
```

---

## 32. Running Individual Components

### Transformation

```bat
python src\transform.py
```

### Analytics

```bat
python src\analytics.py
```

### Data Quality

```bat
python src\data_quality.py
```

### BigQuery Loader

```bat
python src\bigquery_loader.py
```

### Partitioning

```bat
python src\setup_partitioned_sales.py
```

### Standard Pipeline

```bat
python src\pipeline.py
```

### Orchestrated Pipeline

```bat
python -m src.orchestrator
```

---

## 33. Automated Testing

Run the complete test suite:

```bat
python -m pytest tests -v
```

Latest result:

```text
13 passed
```

---

## 34. Test Coverage

### Incremental Loading Tests

```text
test_existing_orders_are_skipped
test_new_order_is_loaded
test_no_duplicate_orders
test_all_existing_orders_return_empty
```

### BigQuery Configuration Tests

```text
test_partition_configuration
test_clustering_configuration
```

### Data Quality Tests

```text
test_quality_checks_pass
test_invalid_discount_fails
```

### Transformation Test

```text
test_sales_transformation
```

### Orchestration Tests

```text
test_task_success
test_task_retry
test_task_failure_after_retries
test_pipeline_task_order
```

These validate:

- Successful task execution
- Retry behavior
- Failure handling
- Task dependency/order

---

## 35. Latest Test Result

```text
=========================================================================== test session starts ===========================================================================

platform win32
Python 3.12.7
pytest-9.1.1

collected 13 items

tests/test_bigquery_incremental.py::test_existing_orders_are_skipped PASSED
tests/test_bigquery_incremental.py::test_new_order_is_loaded PASSED
tests/test_bigquery_incremental.py::test_no_duplicate_orders PASSED
tests/test_bigquery_incremental.py::test_all_existing_orders_return_empty PASSED
tests/test_bigquery_incremental.py::test_partition_configuration PASSED
tests/test_bigquery_incremental.py::test_clustering_configuration PASSED
tests/test_data_quality.py::test_quality_checks_pass PASSED
tests/test_data_quality.py::test_invalid_discount_fails PASSED
tests/test_orchestrator.py::test_task_success PASSED
tests/test_orchestrator.py::test_task_retry PASSED
tests/test_orchestrator.py::test_task_failure_after_retries PASSED
tests/test_orchestrator.py::test_pipeline_task_order PASSED
tests/test_transform.py::test_sales_transformation PASSED

====================================================================== 13 passed ======================================================================
```

---

## 36. GitHub Actions CI

The project includes GitHub Actions continuous integration.

Workflow:

```text
.github/
└── workflows/
    └── ci.yml
```

The CI pipeline automatically runs on pushes and pull requests.

Workflow:

```text
Git Push / Pull Request
          |
          v
GitHub Actions
          |
          v
Checkout Repository
          |
          v
Python 3.12
          |
          v
Java 17
          |
          v
Install requirements.txt
          |
          v
Run PyTest
          |
          v
13 Tests
          |
          v
PASS / FAIL
```

The CI workflow is intentionally credential-free. It does not use personal GCP ADC credentials.

Latest GitHub Actions result:

```text
Retail Data Pipeline CI
Run PyTest
SUCCESS
```

---

## 37. SQL Analytics

SQL queries are maintained in:

```text
sql/analytics.sql
```

Example daily sales query:

```sql
SELECT
    order_date,
    SUM(net_sales) AS total_net_sales
FROM
    `vast-falcon-415411.retail_analytics.sales_partitioned`
GROUP BY
    order_date
ORDER BY
    order_date;
```

Example category sales query:

```sql
SELECT
    category,
    SUM(net_sales) AS total_net_sales
FROM
    `vast-falcon-415411.retail_analytics.sales_partitioned`
GROUP BY
    category
ORDER BY
    total_net_sales DESC;
```

Example store sales query:

```sql
SELECT
    store_id,
    SUM(net_sales) AS total_net_sales
FROM
    `vast-falcon-415411.retail_analytics.sales_partitioned`
GROUP BY
    store_id
ORDER BY
    total_net_sales DESC;
```

---

## 38. Production Features

This project demonstrates several production-oriented data engineering practices.

### Cloud Raw Layer

Raw data is stored in Google Cloud Storage.

### Distributed Processing

PySpark is used for data transformation, processing and aggregation.

### Data Quality

The pipeline validates:

```text
Null values
Invalid quantities
Invalid prices
Invalid discounts
Duplicate orders
```

### Incremental Processing

Existing `order_id` values are checked before inserting records.

### BigQuery Partitioning

Production sales data is partitioned by:

```text
order_date
```

### BigQuery Clustering

Production sales data is clustered by:

```text
category
store_id
```

### Orchestration

The pipeline is broken into reusable tasks with:

```text
Task dependencies
Retry handling
Logging
Failure handling
Cleanup
```

### Automated Testing

PyTest validates:

```text
Transformations
Data quality
Incremental logic
Partition configuration
Clustering configuration
Orchestration
```

### Continuous Integration

GitHub Actions automatically runs the test suite after code changes.

---

## 39. Windows Development Notes

This project is developed and tested locally on Windows.

PySpark may display warnings such as:

```text
Did not find winutils.exe
```

and:

```text
Unable to load native-hadoop library
```

These warnings are related to the local Windows Spark environment.

They do not prevent the current pipeline from successfully processing the data.

The GCS integration uses the Google Cloud Storage Python client to download the raw CSV before PySpark processes it.

---

## 40. GCS Processing Design

The local development flow uses:

```text
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
```

This keeps local Windows development simple while still using GCS as the cloud raw-data layer.

---

## 41. BigQuery Optimization

The production sales table uses:

```text
Partition:
order_date

Clustering:
category
store_id
```

This design supports common retail analytics patterns such as:

```text
Sales by date
Sales by category
Sales by store
Sales by date and category
Sales by date and store
```

---

## 42. Billing and Cost Awareness

This project uses Google Cloud services including:

```text
Google Cloud Storage
BigQuery
```

GCP resources can incur charges depending on usage and billing configuration.

Recommended practices:

- Keep development datasets small
- Avoid unnecessary large queries
- Monitor BigQuery usage
- Monitor Cloud Storage usage
- Configure billing budgets and alerts
- Delete unused resources when no longer required

The current portfolio dataset is intentionally small.

---

## 43. Project Development Phases

### Phase 1 — Local PySpark Pipeline

Status:

```text
COMPLETE
```

Implemented:

- PySpark transformations
- Sales calculations
- Data quality validation
- Analytics
- Automated tests

### Phase 2 — BigQuery Integration

Status:

```text
COMPLETE
```

Implemented:

- BigQuery client
- BigQuery schemas
- BigQuery loading
- Analytical tables
- ADC authentication

### Phase 3 — Incremental BigQuery Processing

Status:

```text
COMPLETE
```

Implemented:

- Existing order ID detection
- New-record loading
- Duplicate prevention
- Partitioned sales table
- Clustered sales table
- Incremental tests

### Phase 4 — GCS Raw Data Layer

Status:

```text
COMPLETE
```

Implemented:

- GCS bucket
- Raw data folder structure
- Raw sales CSV
- Google Cloud Storage Python client
- GCS-to-PySpark integration
- GCS as default source
- Local CSV fallback

### Phase 5 — Pipeline Orchestration

Status:

```text
COMPLETE
```

Implemented:

- Reusable pipeline tasks
- Shared pipeline context
- Task dependencies
- Retry handling
- Task-level logging
- Failure handling
- Cleanup
- Lightweight local orchestration
- Airflow-compatible DAG structure
- Orchestration tests

### Phase 6 — GitHub Actions CI

Status:

```text
COMPLETE
```

Implemented:

- GitHub Actions workflow
- Python 3.12 setup
- Java 17 setup
- Dependency installation
- Automated PyTest execution
- CI validation on code changes

---

## 44. Current End-to-End Architecture

```text
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
                    Create Analytics
                             |
                             v
                  Incremental Processing
                       order_id
                             |
                             v
                    BigQuery Load
                             |
                +------------+------------+
                |                         |
                v                         v
          Partitioning              Clustering
          order_date             category/store_id
                |
                v
             Analytics
                |
       +--------+--------+---------+
       |        |        |         |
       v        v        v         v
     Daily   Category  Store   Payment
     Sales    Sales    Sales   Method Sales
                             |
                             v
                      Orchestrator
                             |
                    +--------+--------+
                    |                 |
                    v                 v
                  Retry            Cleanup
                             |
                             v
                       GitHub Actions
                             |
                             v
                         PyTest
                             |
                             v
                         13 PASS
```

---

## 45. Validation Summary

The complete pipeline has been successfully validated.

Current source records:

```text
16
```

Current production BigQuery records:

```text
16
```

Current analytics records:

```text
daily_sales: 8
category_sales: 4
store_sales: 3
payment_method_sales: 4
```

Incremental loading:

```text
New sales loaded: 0
```

Data quality:

```text
PASS
```

Automated tests:

```text
13 passed
```

Partitioning:

```text
order_date
```

Clustering:

```text
category
store_id
```

Orchestration:

```text
SUCCESS
```

GitHub Actions CI:

```text
SUCCESS
```

Git repository:

```text
Working tree clean
```

---

## 46. Project Status

```text
============================================================
PROJECT STATUS
============================================================

GCS Raw Layer             : COMPLETE
PySpark Transformation   : COMPLETE
Data Quality             : COMPLETE
BigQuery Integration     : COMPLETE
Incremental Loading      : COMPLETE
Partitioning             : COMPLETE
Clustering               : COMPLETE
Analytics                : COMPLETE
Pipeline Orchestration   : COMPLETE
Automated Tests          : 13 PASSED
GitHub Actions CI        : PASS
End-to-End Pipeline      : PASS

============================================================
```

---

## 47. Future Enhancements

Potential future improvements include:

### Orchestration

- Apache Airflow
- Cloud Composer
- Cloud Scheduler
- Event-driven processing
- Production scheduling
- Dependency monitoring

### GCS

- Multiple file ingestion
- File arrival validation
- Archive layer
- GCS lifecycle policies
- Date-partitioned raw folders

### BigQuery

- MERGE-based incremental processing
- Partition expiration
- Advanced clustering
- Slowly Changing Dimensions
- Cost optimization

### Monitoring

- Cloud Logging
- Cloud Monitoring
- Pipeline failure alerts
- Data quality dashboards
- Execution metrics

### CI/CD

- Pull request quality gates
- Code coverage reporting
- Code formatting checks
- Static analysis
- Automated deployment

### Analytics

- Looker Studio dashboard
- Retail KPI dashboard
- Revenue trends
- Category performance
- Store performance
- Payment method analysis

---

## 48. Author

**Ramakrishna Vulli**

Data Engineer

Core technologies demonstrated:

```text
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
Pipeline Orchestration
PyTest
GitHub Actions
Google Cloud Platform
```

---

## 49. Conclusion

This project demonstrates a complete cloud-based retail data engineering pipeline.

The pipeline:

1. Stores raw data in GCS
2. Reads raw data using the GCS Python client
3. Processes data using PySpark
4. Performs data quality validation
5. Calculates retail sales metrics
6. Detects previously loaded order IDs
7. Loads only new records into BigQuery
8. Uses partitioning and clustering for the production sales table
9. Generates analytical tables
10. Uses reusable orchestration tasks
11. Supports retry and failure handling
12. Validates functionality using automated tests
13. Automatically validates code changes through GitHub Actions CI

The result is a scalable, testable, and production-oriented GCP retail data engineering architecture.
