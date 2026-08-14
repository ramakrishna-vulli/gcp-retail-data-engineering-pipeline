from decimal import Decimal

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from pyspark.sql import DataFrame


# ============================================================
# Configuration
# ============================================================

PROJECT_ID = "vast-falcon-415411"
DATASET_ID = "retail_analytics"

# Production sales target.
#
# This table has already been created in BigQuery with:
#
# Partition:
#     order_date
#
# Clustering:
#     category
#     store_id
#
SALES_TABLE_NAME = "sales_partitioned"


# ============================================================
# BigQuery Client
# ============================================================

def create_bigquery_client():
    """
    Create a BigQuery client using
    Application Default Credentials.
    """

    return bigquery.Client(
        project=PROJECT_ID
    )


def get_table_id(table_name: str) -> str:
    """
    Return fully qualified BigQuery table ID.
    """

    return (
        f"{PROJECT_ID}."
        f"{DATASET_ID}."
        f"{table_name}"
    )


# ============================================================
# Table Validation
# ============================================================

def get_existing_table(
    client,
    table_name: str,
):
    """
    Return an existing BigQuery table.

    Raises RuntimeError if the table does not exist.
    """

    table_id = get_table_id(
        table_name
    )

    try:

        return client.get_table(
            table_id
        )

    except NotFound as exc:

        raise RuntimeError(
            f"BigQuery table does not exist: "
            f"{table_id}"
        ) from exc


def validate_sales_table(client):
    """
    Validate the production sales table.

    Expected:
        Partition field = order_date
        Partition type  = DAY
        Clusters        = category, store_id
    """

    table = get_existing_table(
        client,
        SALES_TABLE_NAME,
    )

    expected_partition_field = (
        "order_date"
    )

    expected_cluster_fields = [
        "category",
        "store_id",
    ]

    if not table.time_partitioning:

        raise RuntimeError(
            f"{SALES_TABLE_NAME} is not partitioned."
        )

    actual_partition_field = (
        table.time_partitioning.field
    )

    if (
        actual_partition_field
        != expected_partition_field
    ):

        raise RuntimeError(
            "Unexpected sales partition field. "
            f"Expected: {expected_partition_field}, "
            f"Found: {actual_partition_field}"
        )

    actual_cluster_fields = (
        table.clustering_fields or []
    )

    if (
        actual_cluster_fields
        != expected_cluster_fields
    ):

        raise RuntimeError(
            "Unexpected sales clustering fields. "
            f"Expected: {expected_cluster_fields}, "
            f"Found: {actual_cluster_fields}"
        )

    print(
        f"Validated production table: "
        f"{get_table_id(SALES_TABLE_NAME)}"
    )

    print(
        f"Partition: "
        f"{actual_partition_field}"
    )

    print(
        f"Clustering: "
        f"{', '.join(actual_cluster_fields)}"
    )

    print(
        f"Current rows: "
        f"{table.num_rows}"
    )

    return table


# ============================================================
# Generic Table Creation
# ============================================================

def create_table_if_not_exists(
    client,
    table_name: str,
    schema,
):
    """
    Create a BigQuery table if it does not exist.

    This is used for the analytical tables.

    The partitioned sales table is handled separately
    because it already exists with partitioning/clustering.
    """

    table_id = get_table_id(
        table_name
    )

    table = bigquery.Table(
        table_id,
        schema=schema,
    )

    table = client.create_table(
        table,
        exists_ok=True,
    )

    print(
        f"BigQuery table ready: "
        f"{table_id}"
    )

    return table


# ============================================================
# Sales Schema
# ============================================================

def create_sales_schema():
    """
    Schema for the transformed sales table.
    """

    return [

        bigquery.SchemaField(
            "order_id",
            "INT64",
            mode="REQUIRED",
        ),

        bigquery.SchemaField(
            "order_date",
            "DATE",
            mode="REQUIRED",
        ),

        bigquery.SchemaField(
            "customer_id",
            "STRING",
            mode="REQUIRED",
        ),

        bigquery.SchemaField(
            "product_id",
            "STRING",
            mode="REQUIRED",
        ),

        bigquery.SchemaField(
            "category",
            "STRING",
            mode="REQUIRED",
        ),

        bigquery.SchemaField(
            "quantity",
            "INT64",
            mode="REQUIRED",
        ),

        bigquery.SchemaField(
            "unit_price",
            "NUMERIC",
            mode="REQUIRED",
        ),

        bigquery.SchemaField(
            "discount",
            "NUMERIC",
            mode="REQUIRED",
        ),

        bigquery.SchemaField(
            "store_id",
            "STRING",
            mode="REQUIRED",
        ),

        bigquery.SchemaField(
            "payment_method",
            "STRING",
            mode="REQUIRED",
        ),

        bigquery.SchemaField(
            "gross_sales",
            "NUMERIC",
            mode="REQUIRED",
        ),

        bigquery.SchemaField(
            "discount_amount",
            "NUMERIC",
            mode="REQUIRED",
        ),

        bigquery.SchemaField(
            "net_sales",
            "NUMERIC",
            mode="REQUIRED",
        ),
    ]


# ============================================================
# Daily Sales Schema
# ============================================================

def create_daily_sales_schema():
    """
    Schema for daily sales analytics.
    """

    return [

        bigquery.SchemaField(
            "order_date",
            "DATE",
            mode="REQUIRED",
        ),

        bigquery.SchemaField(
            "order_count",
            "INT64",
            mode="REQUIRED",
        ),

        bigquery.SchemaField(
            "total_quantity",
            "INT64",
            mode="REQUIRED",
        ),

        bigquery.SchemaField(
            "gross_sales",
            "NUMERIC",
            mode="REQUIRED",
        ),

        bigquery.SchemaField(
            "total_discount",
            "NUMERIC",
            mode="REQUIRED",
        ),

        bigquery.SchemaField(
            "net_sales",
            "NUMERIC",
            mode="REQUIRED",
        ),
    ]


# ============================================================
# Category Sales Schema
# ============================================================

def create_category_sales_schema():
    """
    Schema for category sales analytics.
    """

    return [

        bigquery.SchemaField(
            "category",
            "STRING",
            mode="REQUIRED",
        ),

        bigquery.SchemaField(
            "order_count",
            "INT64",
            mode="REQUIRED",
        ),

        bigquery.SchemaField(
            "total_quantity",
            "INT64",
            mode="REQUIRED",
        ),

        bigquery.SchemaField(
            "net_sales",
            "NUMERIC",
            mode="REQUIRED",
        ),
    ]


# ============================================================
# Store Sales Schema
# ============================================================

def create_store_sales_schema():
    """
    Schema for store sales analytics.
    """

    return [

        bigquery.SchemaField(
            "store_id",
            "STRING",
            mode="REQUIRED",
        ),

        bigquery.SchemaField(
            "order_count",
            "INT64",
            mode="REQUIRED",
        ),

        bigquery.SchemaField(
            "total_quantity",
            "INT64",
            mode="REQUIRED",
        ),

        bigquery.SchemaField(
            "net_sales",
            "NUMERIC",
            mode="REQUIRED",
        ),
    ]


# ============================================================
# Payment Method Sales Schema
# ============================================================

def create_payment_method_sales_schema():
    """
    Schema for payment method analytics.
    """

    return [

        bigquery.SchemaField(
            "payment_method",
            "STRING",
            mode="REQUIRED",
        ),

        bigquery.SchemaField(
            "order_count",
            "INT64",
            mode="REQUIRED",
        ),

        bigquery.SchemaField(
            "net_sales",
            "NUMERIC",
            mode="REQUIRED",
        ),
    ]


# ============================================================
# Spark DataFrame -> Pandas
# ============================================================

def spark_to_pandas(
    df: DataFrame,
):
    """
    Convert Spark DataFrame to pandas.

    BigQuery NUMERIC columns are converted to
    Python Decimal objects for reliable PyArrow
    conversion.
    """

    pandas_df = df.toPandas()

    numeric_columns = [

        "unit_price",

        "discount",

        "gross_sales",

        "discount_amount",

        "net_sales",

        "total_discount",
    ]

    for column in numeric_columns:

        if column in pandas_df.columns:

            pandas_df[column] = pandas_df[
                column
            ].apply(
                lambda value: (
                    Decimal(str(value))
                    if value is not None
                    else None
                )
            )

    return pandas_df


# ============================================================
# Existing Order IDs
# ============================================================

def get_existing_order_ids(client):
    """
    Retrieve order IDs already loaded into
    the production sales table.
    """

    table_id = get_table_id(
        SALES_TABLE_NAME
    )

    print()
    print(
        f"Checking existing orders in: "
        f"{table_id}"
    )

    query = f"""
        SELECT DISTINCT order_id
        FROM `{table_id}`
        ORDER BY order_id
    """

    query_job = client.query(
        query
    )

    existing_ids = {
        row.order_id
        for row in query_job.result()
    }

    print(
        f"Existing BigQuery orders: "
        f"{len(existing_ids)}"
    )

    return existing_ids


# ============================================================
# Incremental Sales Filtering
# ============================================================

def filter_new_sales(
    client,
    df: DataFrame,
):
    """
    Return only sales records whose order_id
    does not already exist in BigQuery.
    """

    existing_ids = (
        get_existing_order_ids(
            client
        )
    )

    if not existing_ids:

        print(
            "No existing orders found."
        )

        print(
            "All source records will be "
            "treated as new."
        )

        return df

    new_df = df.filter(
        ~df.order_id.isin(
            list(existing_ids)
        )
    )

    return new_df


# ============================================================
# Generic BigQuery DataFrame Loader
# ============================================================

def load_dataframe_to_bigquery(
    client,
    df: DataFrame,
    table_name: str,
    schema,
    write_disposition=(
        bigquery.WriteDisposition.WRITE_TRUNCATE
    ),
):
    """
    Load a Spark DataFrame into BigQuery.
    """

    table_id = get_table_id(
        table_name
    )

    pandas_df = spark_to_pandas(
        df
    )

    row_count = len(
        pandas_df
    )

    if row_count == 0:

        print()
        print(
            f"No new rows to load into "
            f"{table_id}"
        )

        return client.get_table(
            table_id
        )

    job_config = (
        bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=(
                write_disposition
            ),
        )
    )

    print()
    print(
        f"Loading {row_count} rows "
        f"into {table_id}..."
    )

    load_job = (
        client.load_table_from_dataframe(
            pandas_df,
            table_id,
            job_config=job_config,
        )
    )

    load_job.result()

    table = client.get_table(
        table_id
    )

    print(
        f"Loaded {row_count} rows "
        f"into {table_id}"
    )

    print(
        f"Current table row count: "
        f"{table.num_rows}"
    )

    return table


# ============================================================
# Main BigQuery Loading Workflow
# ============================================================

def main():

    # Import project modules here to avoid
    # unnecessary Spark startup during imports.

    from analytics import (
        create_category_sales,
        create_daily_sales,
        create_payment_method_sales,
        create_store_sales,
    )

    from transform import (
        create_spark_session,
        load_sales,
        transform_sales,
    )

    print()
    print(
        "=" * 60
    )

    print(
        "Starting BigQuery loading..."
    )

    print(
        "=" * 60
    )

    # --------------------------------------------------------
    # Create Spark
    # --------------------------------------------------------

    spark = create_spark_session()

    try:

        # ----------------------------------------------------
        # Create BigQuery client
        # ----------------------------------------------------

        client = (
            create_bigquery_client()
        )

        # ----------------------------------------------------
        # Validate production sales table
        # ----------------------------------------------------

        print()
        print(
            "Validating production sales table..."
        )

        validate_sales_table(
            client
        )

        # ----------------------------------------------------
        # Load source data
        # ----------------------------------------------------

        print()
        print(
            "Loading source sales data..."
        )

        sales_df = load_sales(
            spark
        )

        source_count = (
            sales_df.count()
        )

        print(
            f"Source records: "
            f"{source_count}"
        )

        # ----------------------------------------------------
        # Transform sales
        # ----------------------------------------------------

        print()
        print(
            "Transforming sales data..."
        )

        transformed_df = (
            transform_sales(
                sales_df
            )
        )

        transformed_count = (
            transformed_df.count()
        )

        print(
            f"Transformed records: "
            f"{transformed_count}"
        )

        # ----------------------------------------------------
        # Create analytics
        # ----------------------------------------------------

        print()
        print(
            "Creating analytics..."
        )

        daily_df = (
            create_daily_sales(
                transformed_df
            )
        )

        category_df = (
            create_category_sales(
                transformed_df
            )
        )

        store_df = (
            create_store_sales(
                transformed_df
            )
        )

        payment_df = (
            create_payment_method_sales(
                transformed_df
            )
        )

        print(
            f"Daily sales rows: "
            f"{daily_df.count()}"
        )

        print(
            f"Category sales rows: "
            f"{category_df.count()}"
        )

        print(
            f"Store sales rows: "
            f"{store_df.count()}"
        )

        print(
            f"Payment method rows: "
            f"{payment_df.count()}"
        )

        # ----------------------------------------------------
        # Validate analytics tables
        # ----------------------------------------------------

        create_table_if_not_exists(
            client,
            "daily_sales",
            create_daily_sales_schema(),
        )

        create_table_if_not_exists(
            client,
            "category_sales",
            create_category_sales_schema(),
        )

        create_table_if_not_exists(
            client,
            "store_sales",
            create_store_sales_schema(),
        )

        create_table_if_not_exists(
            client,
            "payment_method_sales",
            create_payment_method_sales_schema(),
        )

        # ----------------------------------------------------
        # Incremental sales loading
        # ----------------------------------------------------

        print()
        print(
            "=" * 60
        )

        print(
            "INCREMENTAL SALES LOAD"
        )

        print(
            "=" * 60
        )

        new_sales_df = (
            filter_new_sales(
                client,
                transformed_df,
            )
        )

        new_sales_count = (
            new_sales_df.count()
        )

        already_loaded_count = (
            transformed_count
            - new_sales_count
        )

        print()
        print(
            f"Source sales records: "
            f"{transformed_count}"
        )

        print(
            f"Already loaded records: "
            f"{already_loaded_count}"
        )

        print(
            f"New records: "
            f"{new_sales_count}"
        )

        # ----------------------------------------------------
        # Append only new sales
        # ----------------------------------------------------

        sales_table = (
            load_dataframe_to_bigquery(
                client,
                new_sales_df,
                SALES_TABLE_NAME,
                create_sales_schema(),
                bigquery.WriteDisposition.WRITE_APPEND,
            )
        )

        # ----------------------------------------------------
        # Refresh analytical tables
        # ----------------------------------------------------

        print()
        print(
            "=" * 60
        )

        print(
            "REFRESHING ANALYTICS TABLES"
        )

        print(
            "=" * 60
        )

        daily_table = (
            load_dataframe_to_bigquery(
                client,
                daily_df,
                "daily_sales",
                create_daily_sales_schema(),
                bigquery.WriteDisposition.WRITE_TRUNCATE,
            )
        )

        category_table = (
            load_dataframe_to_bigquery(
                client,
                category_df,
                "category_sales",
                create_category_sales_schema(),
                bigquery.WriteDisposition.WRITE_TRUNCATE,
            )
        )

        store_table = (
            load_dataframe_to_bigquery(
                client,
                store_df,
                "store_sales",
                create_store_sales_schema(),
                bigquery.WriteDisposition.WRITE_TRUNCATE,
            )
        )

        payment_table = (
            load_dataframe_to_bigquery(
                client,
                payment_df,
                "payment_method_sales",
                create_payment_method_sales_schema(),
                bigquery.WriteDisposition.WRITE_TRUNCATE,
            )
        )

        # ----------------------------------------------------
        # Final summary
        # ----------------------------------------------------

        print()
        print(
            "=" * 60
        )

        print(
            "BIGQUERY LOADING COMPLETED"
        )

        print(
            "=" * 60
        )

        print()

        print(
            f"New sales loaded: "
            f"{new_sales_count}"
        )

        print(
            f"Production sales table: "
            f"{SALES_TABLE_NAME}"
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
            f"Payment method sales rows: "
            f"{payment_table.num_rows}"
        )

        print()

        print(
            f"Project: "
            f"{PROJECT_ID}"
        )

        print(
            f"Dataset: "
            f"{DATASET_ID}"
        )

        print(
            "=" * 60
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()