from pathlib import Path
import os
import tempfile

from google.cloud import storage

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    round,
    to_date,
)


# ============================================================
# Project paths
# ============================================================

PROJECT_ROOT = (
    Path(__file__).resolve().parent.parent
)

LOCAL_SALES_SOURCE = (
    PROJECT_ROOT
    / "data"
    / "sales.csv"
)


# ============================================================
# GCS Configuration
# ============================================================

GCP_PROJECT_ID = "vast-falcon-415411"

GCS_BUCKET_NAME = (
    "vast-falcon-415411-retail-raw"
)

GCS_SALES_BLOB = (
    "raw/retail/sales/sales.csv"
)


# ============================================================
# Source Configuration
# ============================================================

# Options:
#
# LOCAL
#     Read from data/sales.csv
#
# GCS
#     Download sales.csv from Google Cloud Storage
#     and then load it with Spark.
#
# Change this to "GCS" after replacing
# YOUR_BUCKET_NAME with your actual bucket.
#
DATA_SOURCE = os.getenv(
    "RETAIL_DATA_SOURCE",
    "GCS",
)


# ============================================================
# GCS Download
# ============================================================

def download_sales_from_gcs():
    """
    Download the raw sales CSV from GCS.

    Returns:
        Path to a temporary local CSV file.
    """

    if (
        GCS_BUCKET_NAME
        == "YOUR_BUCKET_NAME"
    ):
        raise RuntimeError(
            "GCS_BUCKET_NAME is not configured. "
            "Update src/transform.py with your "
            "actual GCS bucket name."
        )

    print()
    print(
        "Downloading sales data from GCS..."
    )

    print(
        f"Bucket: {GCS_BUCKET_NAME}"
    )

    print(
        f"Object: {GCS_SALES_BLOB}"
    )

    storage_client = (
        storage.Client(
            project=GCP_PROJECT_ID
        )
    )

    bucket = (
        storage_client.bucket(
            GCS_BUCKET_NAME
        )
    )

    blob = bucket.blob(
        GCS_SALES_BLOB
    )

    if not blob.exists():

        raise FileNotFoundError(
            "GCS object does not exist: "
            f"gs://{GCS_BUCKET_NAME}/"
            f"{GCS_SALES_BLOB}"
        )

    # Create a temporary file.
    temp_file = tempfile.NamedTemporaryFile(
        suffix=".csv",
        delete=False,
    )

    temp_file.close()

    local_path = Path(
        temp_file.name
    )

    blob.download_to_filename(
        str(local_path)
    )

    print(
        "GCS download completed."
    )

    print(
        f"Temporary file: {local_path}"
    )

    return local_path


# ============================================================
# Resolve Source
# ============================================================

def resolve_sales_source():
    """
    Resolve the raw sales source.

    LOCAL:
        data/sales.csv

    GCS:
        gs://bucket/raw/retail/sales/sales.csv

    For GCS, the object is downloaded to a temporary
    local file because Spark is running locally on Windows.
    """

    source = (
        DATA_SOURCE.upper()
        .strip()
    )

    if source == "GCS":

        return (
            download_sales_from_gcs()
        )

    if source == "LOCAL":

        if not LOCAL_SALES_SOURCE.exists():

            raise FileNotFoundError(
                "Local sales file does not exist: "
                f"{LOCAL_SALES_SOURCE}"
            )

        print()
        print(
            "Using local sales source:"
        )

        print(
            LOCAL_SALES_SOURCE
        )

        return LOCAL_SALES_SOURCE

    raise ValueError(
        "Invalid RETAIL_DATA_SOURCE. "
        "Expected LOCAL or GCS. "
        f"Received: {DATA_SOURCE}"
    )


# ============================================================
# Load Sales
# ============================================================

def load_sales(
    spark,
) -> DataFrame:
    """
    Load raw retail sales data into Spark.

    The source can be:

        LOCAL
            data/sales.csv

        GCS
            gs://bucket/raw/retail/sales/sales.csv
    """

    source_path = (
        resolve_sales_source()
    )

    print()
    print(
        "Loading retail sales data..."
    )

    df = (
        spark.read
        .option(
            "header",
            True,
        )
        .option(
            "inferSchema",
            True,
        )
        .csv(
            str(source_path)
        )
    )

    print(
        f"Source records: "
        f"{df.count()}"
    )

    return df


# ============================================================
# Sales Transformation
# ============================================================

def transform_sales(
    df: DataFrame,
) -> DataFrame:
    """
    Transform raw retail sales data.

    Adds:

        gross_sales
        discount_amount
        net_sales
    """

    transformed_df = (
        df

        # ----------------------------------------------------
        # Date
        # ----------------------------------------------------

        .withColumn(
            "order_date",
            to_date(
                col("order_date"),
                "yyyy-MM-dd",
            ),
        )

        # ----------------------------------------------------
        # Numeric fields
        # ----------------------------------------------------

        .withColumn(
            "quantity",
            col("quantity").cast(
                "integer"
            ),
        )

        .withColumn(
            "unit_price",
            col("unit_price").cast(
                "double"
            ),
        )

        .withColumn(
            "discount",
            col("discount").cast(
                "double"
            ),
        )

        # ----------------------------------------------------
        # Gross sales
        # ----------------------------------------------------

        .withColumn(
            "gross_sales",
            round(
                col("quantity")
                * col("unit_price"),
                2,
            ),
        )

        # ----------------------------------------------------
        # Discount amount
        # ----------------------------------------------------

        .withColumn(
            "discount_amount",
            round(
                col("quantity")
                * col("unit_price")
                * col("discount"),
                2,
            ),
        )

        # ----------------------------------------------------
        # Net sales
        # ----------------------------------------------------

        .withColumn(
            "net_sales",
            round(
                col("quantity")
                * col("unit_price")
                * (
                    1
                    - col("discount")
                ),
                2,
            ),
        )
    )

    return transformed_df


# ============================================================
# Spark Session
# ============================================================

def create_spark_session():

    from pyspark.sql import (
        SparkSession,
    )

    return (
        SparkSession.builder

        .appName(
            "GCPRetailDataEngineeringPipeline"
        )

        .master(
            "local[*]"
        )

        .getOrCreate()
    )


# ============================================================
# Main
# ============================================================

def main():

    spark = (
        create_spark_session()
    )

    try:

        print()
        print(
            "=" * 60
        )

        print(
            "RETAIL SALES TRANSFORMATION"
        )

        print(
            "=" * 60
        )

        print()

        print(
            f"Data source: "
            f"{DATA_SOURCE.upper()}"
        )

        # ----------------------------------------------------
        # Load
        # ----------------------------------------------------

        sales_df = load_sales(
            spark
        )

        # ----------------------------------------------------
        # Transform
        # ----------------------------------------------------

        transformed_df = (
            transform_sales(
                sales_df
            )
        )

        print()
        print(
            "Transformation completed."
        )

        # ----------------------------------------------------
        # Display results
        # ----------------------------------------------------

        transformed_df.select(
            "order_id",
            "order_date",
            "category",
            "quantity",
            "gross_sales",
            "discount_amount",
            "net_sales",
        ).show(
            20,
            truncate=False,
        )

        print()
        print(
            f"Final records: "
            f"{transformed_df.count()}"
        )

    finally:

        spark.stop()


# ============================================================
# Entry Point
# ============================================================

if __name__ == "__main__":
    main()