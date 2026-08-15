# GCP Retail Data Engineering Pipeline

[![CI](https://github.com/ramakrishna-vulli/gcp-retail-data-engineering-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/ramakrishna-vulli/gcp-retail-data-engineering-pipeline/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/Python-3.12-blue)
![PySpark](https://img.shields.io/badge/PySpark-3.x-orange)
![GCP](https://img.shields.io/badge/Google%20Cloud-GCP-blue)
![BigQuery](https://img.shields.io/badge/BigQuery-Data%20Warehouse-blue)
![Tests](https://img.shields.io/badge/tests-20%20passed-success)
![CI/CD](https://img.shields.io/badge/CI%2FCD-GitHub%20Actions-black)

## Production-Style Retail Data Engineering Project

An end-to-end cloud data engineering pipeline built using Python, PySpark, Google Cloud Storage, BigQuery, GitHub Actions, and Google Cloud Workload Identity Federation.

The project demonstrates production-oriented data engineering practices including:

- Cloud data ingestion
- PySpark transformation
- Data quality validation
- Incremental data loading
- BigQuery partitioning
- BigQuery clustering
- Analytical data modeling
- Task orchestration
- Retry handling
- Pipeline monitoring
- Automated testing
- CI/CD
- Secure GitHub-to-GCP authentication

---

# ⭐ Project Highlights

- GCS → PySpark → BigQuery pipeline
- Incremental BigQuery loading
- Duplicate-order prevention
- BigQuery partitioning by `order_date`
- BigQuery clustering by `category` and `store_id`
- Data-quality validation before production loading
- Daily, category, store, and payment-method analytics
- Lightweight task orchestration
- Task-level retry handling
- Pipeline monitoring
- 20 automated pytest tests
- GitHub Actions CI
- Scheduled cloud execution
- GitHub OIDC authentication
- Google Cloud Workload Identity Federation
- No long-lived service-account JSON key stored in GitHub

---

# 1. Architecture

```text
                         GitHub Repository
                                |
                                v
                         GitHub Actions
                                |
                                v
                    GitHub OIDC Authentication
                                |
                                v
                Google Cloud Workload Identity
                       Federation
                                |
                                v
                     Google Cloud Project
                      vast-falcon-415411
                                |
              +-----------------+----------------+
              |                                  |
              v                                  v
      Google Cloud Storage                  BigQuery
              |                                  |
        sales.csv                         retail_analytics
              |                                  |
              v                                  v
           PySpark                       sales_partitioned
              |
      +-------+--------+
      |                |
      v                v
Transformation    Data Quality
      |                |
      +-------+--------+
              |
              v
         Analytics
              |
      +-------+--------+----------------+
      |       |        |                |
      v       v        v                v
    Daily   Category  Store       Payment Method
    Sales    Sales    Sales            Sales
              |
              v
      Incremental Loading
              |
              v
      BigQuery Production
           Table
              |
              v
       Monitoring / Reporting
2. Technology Stack
Category	Technology
Programming	Python 3.12
Data Processing	PySpark
Cloud Platform	Google Cloud Platform
Object Storage	Google Cloud Storage
Data Warehouse	BigQuery
Testing	pytest
CI/CD	GitHub Actions
Authentication	GitHub OIDC
Cloud Security	Workload Identity Federation
Java	Java 17
Version Control	Git / GitHub
3. Project Overview

The pipeline processes retail sales data from raw ingestion through analytical reporting.

Raw Sales CSV
     |
     v
Google Cloud Storage
     |
     v
PySpark
     |
     +--> Data Transformation
     |
     +--> Data Quality
     |
     v
Analytics Processing
     |
     v
Incremental Filtering
     |
     v
BigQuery
     |
     +--> sales_partitioned
     +--> daily_sales
     +--> category_sales
     +--> store_sales
     +--> payment_method_sales

The pipeline can be executed locally using Python or through GitHub Actions.

4. Google Cloud Configuration
Project
vast-falcon-415411
BigQuery Dataset
retail_analytics
Production Table
sales_partitioned
GCS Bucket
vast-falcon-415411-retail-raw
GCS Object
raw/retail/sales/sales.csv
5. Source Data

Local source:

data/sales.csv

Cloud source:

gs://vast-falcon-415411-retail-raw/raw/retail/sales/sales.csv

Source columns:

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

Example:

order_id,order_date,customer_id,product_id,category,quantity,unit_price,discount,store_id,payment_method

10001,2026-07-01,C001,P001,Electronics,2,25000,0.05,S001,UPI
10002,2026-07-01,C002,P002,Home Appliances,1,18000,0.10,S002,Credit Card
6. PySpark Transformation

PySpark transforms and enriches the raw sales data.

The transformation includes:

Convert order_date to DATE
Convert quantity to INTEGER
Convert numeric fields to BigQuery-compatible numeric values
Calculate gross_sales
Calculate discount_amount
Calculate net_sales
Formulas
gross_sales = quantity * unit_price

discount_amount = quantity * unit_price * discount

net_sales = quantity * unit_price * (1 - discount)
7. Data Quality

Data quality validation runs before production loading.

Current validation includes:

Valid sales data passes
Invalid discount values fail
Pipeline stops when quality validation fails

Example:

Data Quality : PASS

Failure behavior:

Data quality checks failed.
Pipeline stopped.

This prevents invalid data from reaching the production table.

8. Incremental Loading

The pipeline implements incremental loading to prevent duplicate records.

Source Data
    |
    v
PySpark Transformation
    |
    v
Get Existing BigQuery Order IDs
    |
    v
Compare Source Orders
    |
    +----------------------+
    |                      |
    v                      v
Existing                 New
    |                      |
    v                      v
  Skip                    Load
                           |
                           v
                       BigQuery

Existing order_id values are skipped.

Only new orders are inserted.

This allows the pipeline to be safely rerun.

9. BigQuery Production Table

Production table:

vast-falcon-415411.retail_analytics.sales_partitioned

Columns:

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
10. BigQuery Partitioning

The production table is partitioned by:

order_date

Partition type:

DAY

Partitioning helps reduce unnecessary data scanning for date-based queries.

Example:

SELECT
    *
FROM
    `vast-falcon-415411.retail_analytics.sales_partitioned`
WHERE
    order_date = '2026-07-08';
11. BigQuery Clustering

The production table uses:

1. category
2. store_id

Verified configuration:

category
clustering position = 1

store_id
clustering position = 2

Clustering helps optimize queries that filter or group data using these fields.

Example:

SELECT
    category,
    SUM(net_sales) AS total_sales
FROM
    `vast-falcon-415411.retail_analytics.sales_partitioned`
WHERE
    category = 'Electronics'
GROUP BY
    category;
12. Analytics Tables

The pipeline creates four analytical tables:

daily_sales
category_sales
store_sales
payment_method_sales

Verified row counts:

Daily Sales          : 8
Category Sales       : 4
Store Sales          : 3
Payment Method Sales : 4
13. Analytics Queries
Daily Sales
SELECT
    order_date,
    SUM(net_sales) AS total_net_sales
FROM
    `vast-falcon-415411.retail_analytics.daily_sales`
GROUP BY
    order_date
ORDER BY
    order_date;
Category Sales
SELECT
    category,
    SUM(net_sales) AS total_net_sales
FROM
    `vast-falcon-415411.retail_analytics.category_sales`
GROUP BY
    category
ORDER BY
    total_net_sales DESC;
Store Sales
SELECT
    store_id,
    SUM(net_sales) AS total_net_sales
FROM
    `vast-falcon-415411.retail_analytics.store_sales`
GROUP BY
    store_id
ORDER BY
    total_net_sales DESC;
Payment Method Sales
SELECT
    payment_method,
    SUM(net_sales) AS total_net_sales
FROM
    `vast-falcon-415411.retail_analytics.payment_method_sales`
GROUP BY
    payment_method
ORDER BY
    total_net_sales DESC;
14. Orchestration

The project includes a lightweight Python orchestrator.

File:

src/orchestrator.py

Task execution order:

1. create_spark_session
2. load_source_data
3. transform_sales
4. data_quality
5. create_analytics
6. create_bigquery_client
7. filter_new_sales
8. load_bigquery
9. cleanup

The orchestrator provides:

Explicit task dependencies
Task-level logging
Retry handling
Failure handling
Cleanup
Pipeline summary
Monitoring

The design is intentionally compatible with future Airflow / Cloud Composer migration.

15. Retry Handling

Configuration:

MAX_RETRIES = 2

A failed task can execute up to three total attempts.

Attempt 1
    |
  Failed
    |
  Retry
    |
Attempt 2
    |
  Failed
    |
  Retry
    |
Attempt 3
    |
Success / Failure

If all attempts fail, the pipeline is marked as failed.

16. Pipeline Monitoring

File:

src/monitoring.py

Monitoring captures:

Pipeline status
Start time
End time
Duration
Source records
Transformed records
New records
Data quality status
BigQuery status
Retry count
Failed tasks
Production table
Dataset
Project
Task-level status
Error messages

Example:

Status              : SUCCESS
Duration            : 86.17 seconds
Source Records      : 16
Transformed Records : 16
New Records         : 0
Data Quality        : PASS
BigQuery Load       : SUCCESS
Retries             : 0
Failed Tasks         : 0

Production Table    : sales_partitioned
Dataset             : retail_analytics
Project             : vast-falcon-415411

Task status:

create_spark_session       : SUCCESS
load_source_data           : SUCCESS
transform_sales            : SUCCESS
data_quality               : SUCCESS
create_analytics           : SUCCESS
create_bigquery_client     : SUCCESS
filter_new_sales           : SUCCESS
load_bigquery              : SUCCESS
cleanup                    : SUCCESS
17. Automated Testing

The project uses pytest.

Current test suite:

20 tests
20 passed

Run:

python -m pytest tests -v
BigQuery Incremental Loading Tests
Existing orders are skipped
New orders are loaded
Duplicate orders are prevented
All existing orders return an empty dataset
Partition configuration is validated
Clustering configuration is validated
Data Quality Tests
Valid data passes
Invalid discount values fail
Monitoring Tests
Monitoring starts correctly
Successful task status is recorded
Failed task status is recorded
Retry count is recorded
Successful monitoring completion works
Failed monitoring completion works
Duration calculation works
Orchestration Tests
Task success
Task retry
Task failure after retries
Pipeline task order
Transformation Tests
Sales transformation

Expected result:

20 passed
18. GitHub Actions CI

CI workflow:

.github/workflows/ci.yml

CI runs on:

Push to master
Push to main
Pull requests to master
Pull requests to main

CI performs:

1. Checkout repository
2. Setup Python 3.12
3. Setup Java 17
4. Upgrade pip
5. Install dependencies
6. Run pytest

Test command:

python -m pytest tests -v

Expected result:

20 passed
19. Scheduled Cloud Pipeline

Workflow:

.github/workflows/pipeline.yml

The workflow supports:

Manual execution
Scheduled execution

Manual trigger:

workflow_dispatch

Scheduled execution:

03:30 UTC
09:00 AM IST

Execution flow:

Checkout
   |
   v
Google Cloud Authentication
   |
   v
Python 3.12
   |
   v
Java 17
   |
   v
Install Dependencies
   |
   v
Run Pipeline
   |
   v
GCS → PySpark → BigQuery

Pipeline command:

python -m src.orchestrator
20. Secure GitHub-to-GCP Authentication

GitHub Actions uses:

GitHub OIDC
      |
      v
Workload Identity Federation
      |
      v
Google Cloud Service Account
      |
      v
GCS / BigQuery

The project does not store a long-lived Google Cloud service-account JSON key in GitHub.

Workload Identity Pool:

github-actions-pool

Workload Identity Provider:

github-actions-provider

Service Account:

github-actions-retail-pipeline

Repository:

ramakrishna-vulli/gcp-retail-data-engineering-pipeline
21. Security

Do not commit:

Service-account JSON files
Private keys
Passwords
API keys
OAuth credentials
Local credential files

The project uses GitHub OIDC and Google Cloud Workload Identity Federation instead of storing long-lived GCP credentials in the repository.

The GitHub service account should receive only the permissions required by the pipeline.

22. Project Structure
gcp-retail-data-engineering-pipeline/
│
├── .github/
│   └── workflows/
│       ├── ci.yml
│       └── pipeline.yml
│
├── data/
│   └── sales.csv
│
├── dags/
│   └── retail_pipeline_dag.py
│
├── src/
│   ├── __init__.py
│   ├── analytics.py
│   ├── bigquery_loader.py
│   ├── data_quality.py
│   ├── monitoring.py
│   ├── orchestrator.py
│   ├── pipeline.py
│   ├── setup_partitioned_sales.py
│   ├── tasks.py
│   └── transform.py
│
├── tests/
│   ├── test_bigquery_incremental.py
│   ├── test_data_quality.py
│   ├── test_monitoring.py
│   ├── test_orchestrator.py
│   └── test_transform.py
│
├── requirements.txt
├── README.md
└── .gitignore
23. Local Setup

Recommended environment:

Python 3.12.x
Java 17

Create virtual environment:

python -m venv .venv

Windows activation:

.venv\Scripts\activate

Install dependencies:

pip install -r requirements.txt
24. Run Locally

Run the orchestrated pipeline:

python -m src.orchestrator

The pipeline executes:

1. Start Spark
2. Load source data
3. Transform sales
4. Run data-quality checks
5. Create analytics
6. Create BigQuery client
7. Identify new records
8. Load new records
9. Cleanup
10. Generate monitoring summary
25. Run Tests Locally

Run:

python -m pytest tests -v

Expected:

20 passed
26. Verified Production Result

The production BigQuery table was verified after cloud pipeline execution.

Query:

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT order_id) AS unique_orders,
    MIN(order_date) AS min_order_date,
    MAX(order_date) AS max_order_date
FROM
    `vast-falcon-415411.retail_analytics.sales_partitioned`;

Verified result:

Total Rows       : 16
Unique Orders    : 16
Duplicate Orders : 0
Minimum Date     : 2026-07-01
Maximum Date     : 2026-07-08

Therefore:

16 total records
16 unique orders
0 duplicate orders
27. Verified BigQuery Configuration

Production table:

sales_partitioned

Partition:

order_date

Partition type:

DAY

Clustering:

1. category
2. store_id

The BigQuery metadata confirmed the partitioning and clustering configuration.

28. Verified Pipeline Execution

Example successful execution:

Status              : SUCCESS
Duration            : 86.17 seconds
Source Records      : 16
Transformed Records : 16
New Records         : 0
Data Quality        : PASS
BigQuery Load       : SUCCESS
Retries             : 0
Failed Tasks         : 0

This confirms that the pipeline can safely rerun the same source dataset without creating duplicate production records.

29. CI/CD Flow
Developer
    |
    v
Git Commit
    |
    v
Git Push
    |
    v
GitHub
    |
    +-----------------------+
    |                       |
    v                       v
CI Workflow          Scheduled Pipeline
    |                       |
    v                       v
20 Tests             GCP Authentication
    |                       |
    v                       v
PASS                  Run Pipeline
                            |
                            v
                           GCS
                            |
                            v
                         PySpark
                            |
                            v
                         BigQuery
30. Failure Handling

If a task fails:

Failure is logged
Task is retried
Retry count is recorded
If retries are exhausted, pipeline fails
Cleanup is attempted
Monitoring records the failure

Example:

TASK FAILED: transform_sales

Retrying...
31. Pipeline Re-run Behavior

When the same source data is processed again:

Source Records : 16
Already Loaded : 16
New Records    : 0

The production table remains:

16 rows

This behavior is validated by automated incremental-loading tests.

32. Cost Considerations

The project uses:

Google Cloud Storage
BigQuery
GitHub Actions

For a small portfolio dataset, usage can be kept low.

Recommended practices:

Avoid unnecessary BigQuery scans
Use partition filters
Use clustering appropriately
Keep datasets small
Remove unused cloud resources
Monitor GCP billing
Avoid unnecessarily frequent schedules
33. Future Enhancements

Potential future improvements:

Apache Airflow / Cloud Composer
Cloud Run execution
Cloud Scheduler integration
Cloud Logging
Cloud Monitoring alerts
Data lineage
Schema evolution
Dead-letter handling
Advanced data-quality rules
Test coverage reporting
Integration tests
Terraform infrastructure
Secret Manager integration
Development / staging / production environments
Automated deployment environments
Pipeline failure alerting

The current project uses a lightweight Python orchestrator. Airflow/Cloud Composer is considered a future orchestration option rather than the current production execution engine.

34. Key Engineering Skills Demonstrated
Programming
Python
PySpark
SQL
Data Engineering
ETL / ELT
Data transformation
Data quality
Incremental processing
Duplicate prevention
Analytical data modeling
Google Cloud
Google Cloud Storage
BigQuery
GCP IAM
Workload Identity Federation
OIDC authentication
BigQuery
Partitioning
Clustering
Production tables
Incremental loading
Analytical tables
DevOps
Git
GitHub
GitHub Actions
CI/CD
Automated testing
Engineering Practices
Modular design
Task orchestration
Retry handling
Monitoring
Failure handling
Secure authentication
Production validation
35. Project Achievements
GCS ingestion                    PASS
PySpark transformation           PASS
Data quality                     PASS
Incremental loading              PASS
BigQuery                         PASS
Partitioning                     PASS
Clustering                       PASS
Daily analytics                  PASS
Category analytics               PASS
Store analytics                  PASS
Payment analytics                PASS
Orchestration                    PASS
Retry handling                   PASS
Monitoring                       PASS
Automated tests                  20 PASSED
GitHub Actions CI                PASS
Scheduled cloud pipeline         PASS
OIDC authentication              PASS
Workload Identity Federation     PASS
GCS cloud access                 PASS
BigQuery cloud access            PASS
Production verification          PASS
36. Resume-Relevant Project Summary
GCP Retail Data Engineering Pipeline

Built an end-to-end retail data engineering pipeline using Python, PySpark, GCS, and BigQuery. Implemented PySpark transformations, data-quality validation, incremental BigQuery loading with duplicate prevention, partitioning by order_date, clustering by category and store_id, analytical sales datasets, task orchestration, retries, monitoring, automated pytest validation, and GitHub Actions CI/CD. Implemented secure GitHub-to-GCP authentication using OIDC and Workload Identity Federation without storing long-lived service-account keys.

37. Repository

GitHub:

https://github.com/ramakrishna-vulli/gcp-retail-data-engineering-pipeline

Conclusion

The GCP Retail Data Engineering Pipeline demonstrates an end-to-end cloud data engineering solution covering ingestion, transformation, validation, incremental processing, analytical modeling, cloud warehousing, orchestration, monitoring, automated testing, CI/CD, and secure cloud authentication.

The project is designed as a portfolio demonstration of production-oriented Data Engineering practices using Python, PySpark, and Google Cloud.


