
from decimal import Decimal

from google.cloud import bigquery
from pyspark.sql import DataFrame


PROJECT_ID = "vast-falcon-415411"
DATASET_ID = "retail_analytics"


def create_bigquery_client():
    """Create a BigQuery client using ADC authentication."""

    return bigquery.Client(
        project=PROJECT_ID
    )


def get_table_id(table_name: str) -> str:
    """Return a fully qualified BigQuery table ID."""

    return (
        f"{PROJECT_ID}."
        f"{DATASET_ID}."
        f"{table_name}"
    )


def create_table_if_not_exists(
    client,
    table_name: str,
    schema,
):
    """Create a BigQuery table if it does not already exist."""

    table_id = get_table_id(table_name)

    table = bigquery.Table(
        table_id,
        schema=schema,
    )

    table = client.create_table(
        table,
        exists_ok=True,
    )

    print(
        f"BigQuery table ready: {table_id}"
    )

    return table


def create_sales_schema():
    """Create schema for the transformed sales table."""

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


def create_daily_sales_schema():
    """Create schema for daily sales analytics."""

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


def create_category_sales_schema():
    """Create schema for category sales analytics."""

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


def create_store_sales_schema():
    """Create schema for store sales analytics."""

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


def create_payment_method_sales_schema():
    """Create schema for payment-method analytics."""

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


def spark_to_pandas(
    df: DataFrame,
):
    """
    Convert Spark DataFrame to pandas.

    BigQuery NUMERIC columns are explicitly converted
    to Python Decimal objects so PyArrow can safely
    convert them to BigQuery NUMERIC.
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


def load_dataframe_to_bigquery(
    client,
    df: DataFrame,
    table_name: str,
    schema,
):
    """Load a Spark DataFrame into BigQuery."""

    table_id = get_table_id(
        table_name
    )

    pandas_df = spark_to_pandas(df)

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=(
            bigquery.WriteDisposition
            .WRITE_TRUNCATE
        ),
    )

    print(
        f"Loading {len(pandas_df)} rows "
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
        f"Loaded {table.num_rows} rows "
        f"into {table_id}"
    )

    return table


def main():

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

    print(
        "Starting BigQuery loading..."
    )

    spark = create_spark_session()

    try:

        client = create_bigquery_client()

        sales_df = load_sales(
            spark
        )

        transformed_df = (
            transform_sales(
                sales_df
            )
        )

        daily_df = create_daily_sales(
            transformed_df
        )

        category_df = (
            create_category_sales(
                transformed_df
            )
        )

        store_df = create_store_sales(
            transformed_df
        )

        payment_df = (
            create_payment_method_sales(
                transformed_df
            )
        )

        # Create BigQuery tables.

        create_table_if_not_exists(
            client,
            "sales",
            create_sales_schema(),
        )

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

        # Load transformed sales.

        load_dataframe_to_bigquery(
            client,
            transformed_df,
            "sales",
            create_sales_schema(),
        )

        # Load daily analytics.

        load_dataframe_to_bigquery(
            client,
            daily_df,
            "daily_sales",
            create_daily_sales_schema(),
        )

        # Load category analytics.

        load_dataframe_to_bigquery(
            client,
            category_df,
            "category_sales",
            create_category_sales_schema(),
        )

        # Load store analytics.

        load_dataframe_to_bigquery(
            client,
            store_df,
            "store_sales",
            create_store_sales_schema(),
        )

        # Load payment analytics.

        load_dataframe_to_bigquery(
            client,
            payment_df,
            "payment_method_sales",
            create_payment_method_sales_schema(),
        )

        print()
        print(
            "BigQuery loading completed "
            "successfully."
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
