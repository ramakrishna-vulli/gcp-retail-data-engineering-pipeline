
from pyspark.sql import SparkSession

from src.transform import transform_sales


def create_test_spark():
    """
    Create a small local Spark session for tests.
    """

    return (
        SparkSession.builder
        .appName("Project3TransformTests")
        .master("local[1]")
        .config(
            "spark.ui.enabled",
            "false",
        )
        .config(
            "spark.driver.host",
            "127.0.0.1",
        )
        .config(
            "spark.driver.bindAddress",
            "127.0.0.1",
        )
        .config(
            "spark.sql.shuffle.partitions",
            "1",
        )
        .getOrCreate()
    )


def test_sales_transformation():

    spark = create_test_spark()

    try:

        data = [
            (
                10001,
                "2026-07-01",
                "C001",
                "P001",
                "Electronics",
                2,
                25000.0,
                0.05,
                "S001",
                "UPI",
            )
        ]

        columns = [
            "order_id",
            "order_date",
            "customer_id",
            "product_id",
            "category",
            "quantity",
            "unit_price",
            "discount",
            "store_id",
            "payment_method",
        ]

        df = spark.createDataFrame(
            data,
            columns,
        )

        result = (
            transform_sales(df)
            .collect()[0]
        )

        assert result["gross_sales"] == 50000
        assert result["discount_amount"] == 2500
        assert result["net_sales"] == 47500

    finally:

        spark.stop()
