
from pyspark.sql import SparkSession

from src.data_quality import (
    run_quality_checks,
)


def create_test_spark():
    """
    Create a small local Spark session for tests.
    """

    return (
        SparkSession.builder
        .appName("Project3QualityTests")
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


def create_valid_dataframe(spark):

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

    return spark.createDataFrame(
        data,
        columns,
    )


def test_quality_checks_pass():

    spark = create_test_spark()

    try:

        df = create_valid_dataframe(
            spark
        )

        results = run_quality_checks(
            df
        )

        assert results["status"] == "PASS"
        assert results["total_errors"] == 0

    finally:

        spark.stop()


def test_invalid_discount_fails():

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
                1.50,
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

        results = run_quality_checks(
            df
        )

        assert (
            results["invalid_discount"]
            == 1
        )

        assert results["status"] == "FAIL"

    finally:

        spark.stop()
