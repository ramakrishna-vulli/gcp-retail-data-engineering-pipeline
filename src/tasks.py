"""
Reusable tasks for the GCP Retail Data Engineering Pipeline.

These functions are intentionally small so that they can later
be mapped directly to Airflow tasks.
"""

from pathlib import Path
import sys


# ============================================================
# Project paths
# ============================================================

PROJECT_ROOT = (
    Path(__file__).resolve().parent.parent
)

SRC_ROOT = (
    PROJECT_ROOT / "src"
)

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


# ============================================================
# Pipeline Context
# ============================================================

class PipelineContext:
    """
    Holds objects shared between pipeline tasks.

    Spark DataFrames are kept inside the same Python process.
    They are not serialized between tasks.
    """

    def __init__(self):
        self.spark = None
        self.sales_df = None
        self.transformed_df = None

        self.daily_df = None
        self.category_df = None
        self.store_df = None
        self.payment_df = None

        self.quality_results = None

        self.client = None

        self.new_sales_df = None
        self.new_sales_count = 0
        self.source_count = 0
        self.transformed_count = 0


# ============================================================
# Task 1 — Create Spark Session
# ============================================================

def task_create_spark(context):
    """
    Create the Spark session.
    """

    from transform import (
        create_spark_session,
    )

    print()
    print("=" * 60)
    print("TASK 1: CREATE SPARK SESSION")
    print("=" * 60)

    context.spark = (
        create_spark_session()
    )

    print(
        "Spark session created successfully."
    )

    return context


# ============================================================
# Task 2 — Load Source Data
# ============================================================

def task_load_source(context):
    """
    Load raw sales data from GCS/local source.
    """

    from transform import (
        load_sales,
    )

    print()
    print("=" * 60)
    print("TASK 2: LOAD SOURCE DATA")
    print("=" * 60)

    context.sales_df = (
        load_sales(
            context.spark
        )
    )

    context.source_count = (
        context.sales_df.count()
    )

    print(
        f"Source records: "
        f"{context.source_count}"
    )

    return context


# ============================================================
# Task 3 — Transform Data
# ============================================================

def task_transform(context):
    """
    Transform raw retail sales data.
    """

    from transform import (
        transform_sales,
    )

    print()
    print("=" * 60)
    print("TASK 3: TRANSFORM SALES")
    print("=" * 60)

    context.transformed_df = (
        transform_sales(
            context.sales_df
        )
    )

    context.transformed_count = (
        context.transformed_df.count()
    )

    print(
        f"Records transformed: "
        f"{context.transformed_count}"
    )

    return context


# ============================================================
# Task 4 — Data Quality
# ============================================================

def task_data_quality(context):
    """
    Run data quality checks.
    """

    from data_quality import (
        run_quality_checks,
        print_quality_report,
    )

    print()
    print("=" * 60)
    print("TASK 4: DATA QUALITY")
    print("=" * 60)

    context.quality_results = (
        run_quality_checks(
            context.transformed_df
        )
    )

    print_quality_report(
        context.quality_results
    )

    if (
        context.quality_results["status"]
        != "PASS"
    ):

        raise RuntimeError(
            "Data quality checks failed."
        )

    print(
        "Data quality validation passed."
    )

    return context


# ============================================================
# Task 5 — Create Analytics
# ============================================================

def task_create_analytics(context):
    """
    Create analytical DataFrames.
    """

    from analytics import (
        create_daily_sales,
        create_category_sales,
        create_store_sales,
        create_payment_method_sales,
    )

    print()
    print("=" * 60)
    print("TASK 5: CREATE ANALYTICS")
    print("=" * 60)

    context.daily_df = (
        create_daily_sales(
            context.transformed_df
        )
    )

    context.category_df = (
        create_category_sales(
            context.transformed_df
        )
    )

    context.store_df = (
        create_store_sales(
            context.transformed_df
        )
    )

    context.payment_df = (
        create_payment_method_sales(
            context.transformed_df
        )
    )

    print(
        f"Daily sales: "
        f"{context.daily_df.count()}"
    )

    print(
        f"Category sales: "
        f"{context.category_df.count()}"
    )

    print(
        f"Store sales: "
        f"{context.store_df.count()}"
    )

    print(
        f"Payment methods: "
        f"{context.payment_df.count()}"
    )

    return context


# ============================================================
# Task 6 — Create BigQuery Client
# ============================================================

def task_create_bigquery_client(context):
    """
    Create BigQuery client and validate production table.
    """

    from bigquery_loader import (
        create_bigquery_client,
        validate_sales_table,
        SALES_TABLE_NAME,
    )

    print()
    print("=" * 60)
    print("TASK 6: BIGQUERY CONNECTION")
    print("=" * 60)

    context.client = (
        create_bigquery_client()
    )

    validate_sales_table(
        context.client
    )

    print(
        f"Production table: "
        f"{SALES_TABLE_NAME}"
    )

    return context


# ============================================================
# Task 7 — Incremental Filter
# ============================================================

def task_filter_new_sales(context):
    """
    Identify records that are not already present
    in the production BigQuery table.
    """

    from bigquery_loader import (
        filter_new_sales,
    )

    print()
    print("=" * 60)
    print("TASK 7: INCREMENTAL CHECK")
    print("=" * 60)

    context.new_sales_df = (
        filter_new_sales(
            context.client,
            context.transformed_df,
        )
    )

    context.new_sales_count = (
        context.new_sales_df.count()
    )

    already_loaded = (
        context.transformed_count
        - context.new_sales_count
    )

    print(
        f"Source records: "
        f"{context.transformed_count}"
    )

    print(
        f"Already loaded: "
        f"{already_loaded}"
    )

    print(
        f"New records: "
        f"{context.new_sales_count}"
    )

    return context


# ============================================================
# Task 8 — Load BigQuery
# ============================================================

def task_load_bigquery(context):
    """
    Load incremental sales and analytical tables.
    """

    from bigquery_loader import (
        SALES_TABLE_NAME,
        create_sales_schema,
        create_daily_sales_schema,
        create_category_sales_schema,
        create_store_sales_schema,
        create_payment_method_sales_schema,
        load_dataframe_to_bigquery,
    )

    print()
    print("=" * 60)
    print("TASK 8: LOAD BIGQUERY")
    print("=" * 60)

    # --------------------------------------------------------
    # Production sales table
    # --------------------------------------------------------

    sales_table = (
        load_dataframe_to_bigquery(
            context.client,
            context.new_sales_df,
            SALES_TABLE_NAME,
            create_sales_schema(),
        )
    )

    # --------------------------------------------------------
    # Analytics tables
    # --------------------------------------------------------

    daily_table = (
        load_dataframe_to_bigquery(
            context.client,
            context.daily_df,
            "daily_sales",
            create_daily_sales_schema(),
        )
    )

    category_table = (
        load_dataframe_to_bigquery(
            context.client,
            context.category_df,
            "category_sales",
            create_category_sales_schema(),
        )
    )

    store_table = (
        load_dataframe_to_bigquery(
            context.client,
            context.store_df,
            "store_sales",
            create_store_sales_schema(),
        )
    )

    payment_table = (
        load_dataframe_to_bigquery(
            context.client,
            context.payment_df,
            "payment_method_sales",
            create_payment_method_sales_schema(),
        )
    )

    print()
    print(
        "BigQuery loading completed."
    )

    print(
        f"Sales table rows: "
        f"{sales_table.num_rows}"
    )

    print(
        f"Daily sales rows: "
        f"{daily_table.num_rows}"
    )

    print(
        f"Category sales rows: "
        f"{category_table.num_rows}"
    )

    print(
        f"Store sales rows: "
        f"{store_table.num_rows}"
    )

    print(
        f"Payment method rows: "
        f"{payment_table.num_rows}"
    )

    return context


# ============================================================
# Task 9 — Cleanup
# ============================================================

def task_cleanup(context):
    """
    Stop Spark after successful or failed execution.
    """

    print()
    print("=" * 60)
    print("TASK 9: CLEANUP")
    print("=" * 60)

    if context.spark is not None:

        context.spark.stop()

        print(
            "Spark session stopped."
        )

    return context