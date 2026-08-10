from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    round,
    to_date,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent

SALES_SOURCE = (
    PROJECT_ROOT
    / "data"
    / "sales.csv"
)


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
        .withColumn(
            "order_date",
            to_date(
                col("order_date"),
                "yyyy-MM-dd",
            ),
        )
        .withColumn(
            "quantity",
            col("quantity").cast("integer"),
        )
        .withColumn(
            "unit_price",
            col("unit_price").cast("double"),
        )
        .withColumn(
            "discount",
            col("discount").cast("double"),
        )
        .withColumn(
            "gross_sales",
            round(
                col("quantity")
                * col("unit_price"),
                2,
            ),
        )
        .withColumn(
            "discount_amount",
            round(
                col("quantity")
                * col("unit_price")
                * col("discount"),
                2,
            ),
        )
        .withColumn(
            "net_sales",
            round(
                col("quantity")
                * col("unit_price")
                * (
                    1 - col("discount")
                ),
                2,
            ),
        )
    )

    return transformed_df


def load_sales(
    spark,
) -> DataFrame:
    """Load raw sales CSV into a Spark DataFrame."""

    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(SALES_SOURCE))
    )


def create_spark_session():

    from pyspark.sql import SparkSession

    return (
        SparkSession.builder
        .appName(
            "GCPRetailDataEngineeringPipeline"
        )
        .master("local[*]")
        .getOrCreate()
    )


def main():

    spark = create_spark_session()

    try:

        print(
            "Loading retail sales data..."
        )

        sales_df = load_sales(
            spark
        )

        print(
            f"Source records: "
            f"{sales_df.count()}"
        )

        transformed_df = transform_sales(
            sales_df
        )

        print(
            "Transformation completed."
        )

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

    finally:

        spark.stop()


if __name__ == "__main__":
    main()