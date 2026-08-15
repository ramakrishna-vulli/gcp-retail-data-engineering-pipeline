# GCP Retail Data Engineering Pipeline

Python | PySpark | Google Cloud Storage | BigQuery | Data Quality | Incremental ETL | Orchestration | Monitoring | GitHub Actions

A production-style retail data engineering pipeline built using Python, PySpark, Google Cloud Storage (GCS), and BigQuery.

The pipeline implements:

- Cloud Storage raw data ingestion
- PySpark data transformation
- Data quality validation
- Incremental BigQuery loading
- BigQuery partitioning
- BigQuery clustering
- Sales analytics
- Lightweight task orchestration
- Retry handling
- Pipeline monitoring
- Automated testing
- GitHub Actions CI
- Scheduled cloud execution
- GitHub OIDC authentication
- Google Cloud Workload Identity Federation


============================================================
1. PROJECT OVERVIEW
============================================================

This project demonstrates an end-to-end retail data engineering pipeline.

Raw retail sales data is stored in Google Cloud Storage.

The pipeline downloads the raw CSV data, processes it using PySpark, performs data quality checks, creates analytical datasets, and incrementally loads the processed data into BigQuery.

The production sales table uses:

- Partitioning by order_date
- Clustering by category and store_id

The pipeline can run locally or through GitHub Actions.

GitHub Actions authenticates to Google Cloud using OpenID Connect (OIDC) and Workload Identity Federation without storing a long-lived Google Cloud service-account JSON key in the repository.


============================================================
2. ARCHITECTURE
============================================================

                         GitHub Repository
                                |
                                |
                         GitHub Actions
                                |
                         OIDC Authentication
                                |
                    Workload Identity Federation
                                |
                                v
                     Google Cloud Project
                      vast-falcon-415411
                                |
                +---------------+---------------+
                |                               |
                v                               v
        Google Cloud Storage               BigQuery
                |                               |
          sales.csv                    retail_analytics
                |                               |
                v                               v
             PySpark                  sales_partitioned
                |
        +-------+-------+
        |               |
        v               v
 Transformation   Data Quality
        |               |
        +-------+-------+
                |
                v
          Analytics
                |
        +-------+---------+----------------+
        |       |         |                |
        v       v         v                v
      Daily   Category   Store       Payment Method
      Sales    Sales     Sales            Sales
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


============================================================
3. TECHNOLOGY STACK
============================================================

Programming Language:
- Python 3.12

Data Processing:
- PySpark

Cloud Platform:
- Google Cloud Platform (GCP)

Cloud Storage:
- Google Cloud Storage (GCS)

Data Warehouse:
- Google BigQuery

Testing:
- pytest

CI/CD:
- GitHub Actions

Authentication:
- GitHub OIDC
- Google Cloud Workload Identity Federation

Java:
- Java 17 for PySpark

Version Control:
- Git
- GitHub


============================================================
4. GCP PROJECT
============================================================

Google Cloud Project:

vast-falcon-415411

BigQuery Dataset:

retail_analytics

Production BigQuery table:

sales_partitioned

Raw GCS bucket:

vast-falcon-415411-retail-raw

Raw object:

raw/retail/sales/sales.csv


============================================================
5. SOURCE DATA
============================================================

The source file is:

data/sales.csv

The production cloud source is:

GCS:
vast-falcon-415411-retail-raw

Object:

raw/retail/sales/sales.csv


Source columns:

- order_id
- order_date
- customer_id
- product_id
- category
- quantity
- unit_price
- discount
- store_id
- payment_method


Example:

order_id,order_date,customer_id,product_id,category,quantity,unit_price,discount,store_id,payment_method

10001,2026-07-01,C001,P001,Electronics,2,25000,0.05,S001,UPI

10002,2026-07-01,C002,P002,Home Appliances,1,18000,0.10,S002,Credit Card


============================================================
6. DATA TRANSFORMATION
============================================================

PySpark is used to transform the raw sales data.

The transformation process includes:

1. Convert order_date to DATE
2. Convert quantity to INTEGER
3. Convert unit_price to DOUBLE/NUMERIC-compatible values
4. Convert discount to DOUBLE/NUMERIC-compatible values
5. Calculate gross_sales
6. Calculate discount_amount
7. Calculate net_sales


Gross sales:

quantity * unit_price


Discount amount:

quantity * unit_price * discount


Net sales:

quantity * unit_price * (1 - discount)


Example:

Quantity       = 2
Unit Price     = 25,000
Discount       = 5%

Gross Sales:

2 * 25,000 = 50,000

Discount Amount:

50,000 * 0.05 = 2,500

Net Sales:

50,000 - 2,500 = 47,500


============================================================
7. DATA QUALITY
============================================================

The pipeline performs data quality checks before loading production data.

Current tests include:

- Valid sales data passes quality checks
- Invalid discount values fail validation

The pipeline stops when data quality validation fails.

Example:

Data Quality Status:

PASS


If quality checks fail:

Data quality checks failed.
Pipeline stopped.


============================================================
8. INCREMENTAL LOADING
============================================================

The pipeline supports incremental BigQuery loading.

Before inserting records into the production sales table, existing order IDs are checked.

Existing records are skipped.

Only new sales records are inserted.

This prevents duplicate orders.

Example:

Source records:

16

Existing records:

16

New records:

0


If a new order is added:

Source records:

17

Existing records:

16

New records:

1


Only the new record is inserted.


============================================================
9. BIGQUERY PRODUCTION TABLE
============================================================

Production table:

vast-falcon-415411.retail_analytics.sales_partitioned


The production table contains:

- order_id
- order_date
- customer_id
- product_id
- category
- quantity
- unit_price
- discount
- store_id
- payment_method
- gross_sales
- discount_amount
- net_sales


Current verified production data:

Total rows:

16

Unique orders:

16

Duplicate orders:

0

Minimum order date:

2026-07-01

Maximum order date:

2026-07-08


============================================================
10. BIGQUERY PARTITIONING
============================================================

The production sales table is partitioned by:

order_date

Partition type:

DAY


Partitioning provides better query performance and helps reduce unnecessary data scanned for date-based queries.


Example query:

SELECT
    *
FROM
    `vast-falcon-415411.retail_analytics.sales_partitioned`
WHERE
    order_date = '2026-07-08';


============================================================
11. BIGQUERY CLUSTERING
============================================================

The production table is clustered using:

1. category
2. store_id


Verified clustering configuration:

category
clustering position = 1

store_id
clustering position = 2


Clustering helps optimize queries that filter or group data using these columns.


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


============================================================
12. ANALYTICS TABLES
============================================================

The pipeline creates the following analytical datasets:

daily_sales

category_sales

store_sales

payment_method_sales


Current verified row counts:

Daily sales:

8 rows

Category sales:

4 rows

Store sales:

3 rows

Payment method sales:

4 rows


============================================================
13. DAILY SALES ANALYTICS
============================================================

The daily sales table aggregates sales by order date.

Typical metrics include:

- Total quantity
- Gross sales
- Discount amount
- Net sales


Example:

SELECT
    order_date,
    SUM(net_sales) AS total_net_sales
FROM
    `vast-falcon-415411.retail_analytics.daily_sales`
GROUP BY
    order_date
ORDER BY
    order_date;


============================================================
14. CATEGORY SALES ANALYTICS
============================================================

The category sales table aggregates sales by product category.

Example categories:

- Electronics
- Home Appliances
- Furniture
- Clothing


Example query:

SELECT
    category,
    SUM(net_sales) AS total_net_sales
FROM
    `vast-falcon-415411.retail_analytics.category_sales`
GROUP BY
    category
ORDER BY
    total_net_sales DESC;


============================================================
15. STORE SALES ANALYTICS
============================================================

The store sales table aggregates sales by store.

Example stores:

- S001
- S002
- S003


Example query:

SELECT
    store_id,
    SUM(net_sales) AS total_net_sales
FROM
    `vast-falcon-415411.retail_analytics.store_sales`
GROUP BY
    store_id
ORDER BY
    total_net_sales DESC;


============================================================
16. PAYMENT METHOD ANALYTICS
============================================================

The payment method table aggregates sales by payment method.

Example payment methods:

- UPI
- Credit Card
- Debit Card
- Cash


Example query:

SELECT
    payment_method,
    SUM(net_sales) AS total_net_sales
FROM
    `vast-falcon-415411.retail_analytics.payment_method_sales`
GROUP BY
    payment_method
ORDER BY
    total_net_sales DESC;


============================================================
17. ORCHESTRATION
============================================================

The project contains a lightweight local orchestrator.

File:

src/orchestrator.py


The pipeline executes tasks in the following order:

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

- Explicit task dependencies
- Task-level logging
- Retry handling
- Failure handling
- Cleanup
- Pipeline summary
- Monitoring


============================================================
18. RETRY HANDLING
============================================================

The orchestrator supports retries.

Configuration:

MAX_RETRIES = 2

Therefore a task can run up to:

3 total attempts

Example:

Attempt 1
    |
    X Failed
    |
Retry
    |
Attempt 2
    |
    X Failed
    |
Retry
    |
Attempt 3
    |
Success / Failure


If all attempts fail, the pipeline fails.


============================================================
19. PIPELINE MONITORING
============================================================

File:

src/monitoring.py


The monitoring module records:

- Pipeline status
- Start time
- End time
- Duration
- Source records
- Transformed records
- New records
- Data quality status
- BigQuery status
- Retry count
- Failed tasks
- Production table
- Dataset
- Project
- Task-level status
- Error message


Example monitoring report:

============================================================
PIPELINE MONITORING REPORT
============================================================

Status              : SUCCESS
Duration            : 86.17 seconds
Source Records      : 16
Transformed Records : 16
New Records         : 0
Data Quality        : PASS
BigQuery Load       : SUCCESS
Retries             : 0
Failed Tasks        : 0

Production Table    : sales_partitioned
Dataset             : retail_analytics
Project             : vast-falcon-415411


============================================================
TASK STATUS
============================================================

create_spark_session           : SUCCESS
load_source_data               : SUCCESS
transform_sales                : SUCCESS
data_quality                   : SUCCESS
create_analytics               : SUCCESS
create_bigquery_client         : SUCCESS
filter_new_sales               : SUCCESS
load_bigquery                  : SUCCESS
cleanup                        : SUCCESS


============================================================
20. AUTOMATED TESTING
============================================================

The project uses pytest for automated testing.

Current test suite:

20 tests

20 passed


Test coverage includes:

BigQuery incremental loading:

- Existing orders are skipped
- New orders are loaded
- Duplicate orders are prevented
- All existing orders return an empty dataset
- Partition configuration is validated
- Clustering configuration is validated


Data quality:

- Valid data passes
- Invalid discount values fail


Monitoring:

- Monitoring starts correctly
- Successful task status is recorded
- Failed task status is recorded
- Retry count is recorded
- Successful monitoring completion works
- Failed monitoring completion works
- Duration calculation works


Orchestration:

- Task success
- Task retry
- Task failure after retries
- Pipeline task order


Transformation:

- Sales transformation


Run all tests:

python -m pytest tests -v


Expected result:

20 passed


============================================================
21. GITHUB ACTIONS CI
============================================================

The project uses GitHub Actions for continuous integration.

File:

.github/workflows/ci.yml


CI runs on:

- Push to master
- Push to main
- Pull requests to master
- Pull requests to main


CI performs:

1. Checkout repository
2. Setup Python 3.12
3. Setup Java 17
4. Upgrade pip
5. Install requirements
6. Run pytest


Example:

python -m pytest tests -v


CI validates the project before changes are considered complete.


============================================================
22. SCHEDULED CLOUD PIPELINE
============================================================

The project also contains:

.github/workflows/pipeline.yml


This workflow supports:

- Manual execution
- Scheduled execution


Manual trigger:

workflow_dispatch


Scheduled execution:

Every day at 03:30 UTC

Equivalent to:

09:00 AM IST


The workflow executes:

1. Checkout repository
2. Authenticate to Google Cloud
3. Setup Python 3.12
4. Setup Java 17
5. Upgrade pip
6. Install dependencies
7. Run the retail pipeline


Pipeline command:

python -m src.orchestrator


============================================================
23. GITHUB OIDC AUTHENTICATION
============================================================

GitHub Actions authenticates to Google Cloud using:

GitHub OpenID Connect (OIDC)

and:

Google Cloud Workload Identity Federation


No long-lived Google Cloud service-account JSON key is stored in the GitHub repository.


Authentication flow:

GitHub Actions
      |
      v
GitHub OIDC Token
      |
      v
Google Workload Identity Provider
      |
      v
Workload Identity Pool
      |
      v
Google Service Account
      |
      v
GCS / BigQuery


Workload Identity Pool:

github-actions-pool


Workload Identity Provider:

github-actions-provider


Service Account:

github-actions-retail-pipeline


Repository restriction:

ramakrishna-vulli/gcp-retail-data-engineering-pipeline


Branch restriction:

master


============================================================
24. GOOGLE CLOUD SECURITY
============================================================

The GitHub Actions workflow uses short-lived federated credentials.

The project does not require a long-lived service-account JSON key to be committed to GitHub.


Important security rules:

DO NOT:

- Commit service-account JSON files
- Put passwords in source code
- Put credentials in README.md
- Put GCP secrets in requirements.txt
- Upload private keys to GitHub
- Give the GitHub service account unnecessary Owner access


The service account should receive only the permissions required by the pipeline.


============================================================
25. LOCAL PROJECT STRUCTURE
============================================================

gcp-retail-data-engineering-pipeline/

|
+-- .github/
|   |
|   +-- workflows/
|       |
|       +-- ci.yml
|       +-- pipeline.yml
|
+-- data/
|   |
|   +-- sales.csv
|
+-- dags/
|
+-- src/
|   |
|   +-- __init__.py
|   +-- analytics.py
|   +-- bigquery_loader.py
|   +-- data_quality.py
|   +-- monitoring.py
|   +-- orchestrator.py
|   +-- pipeline.py
|   +-- setup_partitioned_sales.py
|   +-- tasks.py
|   +-- transform.py
|
+-- tests/
|   |
|   +-- test_bigquery_incremental.py
|   +-- test_data_quality.py
|   +-- test_monitoring.py
|   +-- test_orchestrator.py
|   +-- test_transform.py
|
+-- requirements.txt
+-- README.md


============================================================
26. REQUIREMENTS
============================================================

Main Python dependencies include:

- google-cloud-storage
- google-cloud-bigquery
- pyspark
- pytest
- pandas

The exact versions are maintained in:

requirements.txt


Install dependencies:

pip install -r requirements.txt


============================================================
27. LOCAL ENVIRONMENT
============================================================

Recommended environment:

Python:

3.12.x

Java:

17

Operating system:

Windows / Linux / macOS


For Windows development, PySpark may display Hadoop/winutils warnings.

These warnings do not necessarily indicate a pipeline failure if the Spark job completes successfully.


============================================================
28. LOCAL EXECUTION
============================================================

Activate the virtual environment.

Windows:

.venv\Scripts\activate


Run the orchestrated pipeline:

python -m src.orchestrator


The pipeline will:

1. Start Spark
2. Load GCS/source data
3. Transform sales data
4. Run quality checks
5. Create analytics
6. Connect to BigQuery
7. Identify new records
8. Load new records
9. Clean up Spark
10. Print monitoring information


============================================================
29. RUN TESTS LOCALLY
============================================================

Run:

python -m pytest tests -v


Expected:

20 passed


For a faster run:

python -m pytest tests


============================================================
30. GOOGLE CLOUD AUTHENTICATION LOCALLY
============================================================

For local development, Google Cloud Application Default Credentials can be configured separately.

Make sure your local environment has permission to access:

- Google Cloud Storage
- BigQuery


The GitHub Actions authentication configuration is separate from local authentication.


============================================================
31. GCS DATA LOCATION
============================================================

Bucket:

vast-falcon-415411-retail-raw


Object:

raw/retail/sales/sales.csv


The pipeline uses GCS as the cloud raw-data layer.


============================================================
32. BIGQUERY DATASET
============================================================

Project:

vast-falcon-415411


Dataset:

retail_analytics


Production table:

sales_partitioned


Analytics tables:

daily_sales

category_sales

store_sales

payment_method_sales


============================================================
33. VERIFIED PRODUCTION RESULT
============================================================

The production table has been verified after the GitHub Actions cloud execution.


Query:

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT order_id) AS unique_orders,
    MIN(order_date) AS min_order_date,
    MAX(order_date) AS max_order_date
FROM
    `vast-falcon-415411.retail_analytics.sales_partitioned`;


Verified result:

total_rows:

16


unique_orders:

16


min_order_date:

2026-07-01


max_order_date:

2026-07-08


Therefore:

16 total records
16 unique orders
0 duplicate orders


============================================================
34. VERIFIED PARTITIONING AND CLUSTERING
============================================================

Production table:

sales_partitioned


Partition:

order_date

Partition type:

DAY


Clustering:

1. category
2. store_id


The BigQuery INFORMATION_SCHEMA output confirmed:

order_date:
is_partitioning_column = YES


category:
clustering_ordinal_position = 1


store_id:
clustering_ordinal_position = 2


============================================================
35. PIPELINE OUTPUT EXAMPLE
============================================================

A successful execution produces output similar to:

======================================================================
PIPELINE COMPLETED SUCCESSFULLY
======================================================================

Duration: 60.47 seconds
Source records: 16
Transformed records: 16
New records: 0

======================================================================


Monitoring:

Status              : SUCCESS
Duration            : 60.xx seconds
Source Records      : 16
Transformed Records : 16
New Records         : 0
Data Quality        : PASS
BigQuery Load       : SUCCESS
Retries             : 0
Failed Tasks         : 0


============================================================
36. FAILURE HANDLING
============================================================

If a task fails:

1. The failure is logged.
2. The task is retried.
3. The retry count is recorded.
4. If all retries fail, the pipeline fails.
5. Cleanup is attempted.
6. Monitoring reports the failed task and error.


Example:

TASK FAILED: transform_sales

Retrying in 5 seconds...


============================================================
37. INCREMENTAL LOAD DESIGN
============================================================

The incremental loading process prevents duplicate orders.

Process:

Raw Data
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
Existing                New
   |                      |
Skip                    Load
                          |
                          v
                    BigQuery


This allows the pipeline to be safely rerun without inserting the same order multiple times.


============================================================
38. PIPELINE RE-RUN BEHAVIOR
============================================================

If the pipeline is executed again with the same source data:

Source records:

16

Already loaded:

16

New records:

0


The production table remains:

16 rows


This behavior has been validated by automated tests.


============================================================
39. CI/CD FLOW
============================================================

Developer:

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
    +----------------------+
    |                      |
    v                      v
CI Workflow          Scheduled Pipeline
    |                      |
    v                      v
20 Tests             GCP Authentication
    |                      |
    v                      v
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


============================================================
40. TROUBLESHOOTING
============================================================

Problem:

Docker is not recognized.

Example:

'docker' is not recognized as an internal or external command.


Resolution:

Docker is not required for the current local pipeline execution.

The pipeline can run using:

Python
PySpark
Java
Google Cloud libraries


Problem:

Apache Airflow package is not installed.

Example:

WARNING: Package(s) not found: apache-airflow


Resolution:

The current project uses a lightweight local orchestrator.

Airflow is not required for the current implementation.


Problem:

GitHub cannot authenticate to GCP.

Check:

- Workload Identity Pool
- Workload Identity Provider
- google.subject mapping
- repository attribute mapping
- GitHub branch condition
- service-account email
- Workload Identity User role
- Project Number


Problem:

BigQuery returns duplicate records.

Check:

- Incremental filtering
- order_id uniqueness
- Production table
- Source data


Problem:

No new records loaded.

Example:

New records:

0


This is expected when all source order IDs already exist in BigQuery.


============================================================
41. COST CONSIDERATIONS
============================================================

This project uses Google Cloud services that may incur charges depending on usage.

Main services:

- Google Cloud Storage
- BigQuery
- GitHub Actions


For a small portfolio dataset, usage can be kept low.

Monitor Google Cloud billing regularly.

Avoid unnecessary:

- Large BigQuery scans
- Large datasets
- Frequent scheduled executions
- Unused storage
- Unused cloud resources


The current scheduled workflow runs once per day.


============================================================
42. FUTURE ENHANCEMENTS
============================================================

Potential improvements include:

1. Apache Airflow / Cloud Composer orchestration
2. Google Cloud Composer DAG
3. Cloud Run execution
4. Cloud Scheduler integration
5. BigQuery cost monitoring
6. Cloud Logging integration
7. Cloud Monitoring alerts
8. Data lineage
9. Schema evolution
10. Dead-letter handling
11. More comprehensive data-quality rules
12. Unit-test coverage reporting
13. Integration tests
14. Terraform infrastructure
15. Secret Manager integration where secrets are required
16. CI/CD deployment environments
17. Development / staging / production datasets
18. Alerting on pipeline failures


============================================================
43. PROJECT ACHIEVEMENTS
============================================================

This project demonstrates practical experience with:

- Python
- PySpark
- ETL / ELT
- Google Cloud Storage
- BigQuery
- Incremental data processing
- Data quality
- Partitioning
- Clustering
- Analytical data modeling
- Task orchestration
- Retry mechanisms
- Monitoring
- Automated testing
- Git
- GitHub
- GitHub Actions
- OIDC
- Workload Identity Federation
- Cloud authentication
- CI/CD


============================================================
44. FINAL VALIDATION
============================================================

Current project validation:

GCS raw data                         PASS

PySpark transformation               PASS

Data quality                         PASS

Incremental loading                  PASS

BigQuery                             PASS

BigQuery partitioning                PASS

BigQuery clustering                  PASS

Daily analytics                      PASS

Category analytics                   PASS

Store analytics                      PASS

Payment analytics                    PASS

Orchestration                        PASS

Retry handling                       PASS

Monitoring                           PASS

Automated tests                      20 PASSED

GitHub Actions CI                    PASS

GitHub Actions cloud pipeline        PASS

OIDC authentication                  PASS

Workload Identity Federation         PASS

GCS cloud access                     PASS

BigQuery cloud access                PASS

Production data verification         PASS


============================================================
45. CONCLUSION
============================================================

The GCP Retail Data Engineering Pipeline is a complete end-to-end data engineering project demonstrating how raw retail data can be ingested from Google Cloud Storage, transformed using PySpark, validated through data-quality checks, processed incrementally, and loaded into a partitioned and clustered BigQuery production table.

The project also demonstrates production-oriented engineering practices including:

- Modular Python code
- Automated testing
- Task orchestration
- Retry handling
- Monitoring
- Incremental processing
- Cloud storage
- Cloud data warehousing
- CI/CD
- GitHub Actions
- Secure cloud authentication using OIDC and Workload Identity Federation


============================================================
END OF README
============================================================