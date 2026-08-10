from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    count,
    round,
    sum as spark_sum,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent


def create_daily_sales(
    df: DataFrame,
) -> DataFrame:
    """Create daily sales summary."""

    return (
        df.groupBy("order_date")
        .agg(
            count("order_id").alias(
                "order_count"
            ),
            spark_sum("quantity").alias(
                "total_quantity"
            ),
            round(
                spark_sum("gross_sales"),
                2,
            ).alias(
                "gross_sales"
            ),
            round(
                spark_sum("discount_amount"),
                2,
            ).alias(
                "total_discount"
            ),
            round(
                spark_sum("net_sales"),
                2,
            ).alias(
                "net_sales"
            ),
        )
        .orderBy("order_date")
    )


def create_category_sales(
    df: DataFrame,
) -> DataFrame:
    """Create category-level sales summary."""

    return (
        df.groupBy("category")
        .agg(
            count("order_id").alias(
                "order_count"
            ),
            spark_sum("quantity").alias(
                "total_quantity"
            ),
            round(
                spark_sum("net_sales"),
                2,
            ).alias(
                "net_sales"
            ),
        )
        .orderBy("category")
    )


def create_store_sales(
    df: DataFrame,
) -> DataFrame:
    """Create store-level sales summary."""

    return (
        df.groupBy("store_id")
        .agg(
            count("order_id").alias(
                "order_count"
            ),
            spark_sum("quantity").alias(
                "total_quantity"
            ),
            round(
                spark_sum("net_sales"),
                2,
            ).alias(
                "net_sales"
            ),
        )
        .orderBy("store_id")
    )


def create_payment_method_sales(
    df: DataFrame,
) -> DataFrame:
    """Create payment-method sales summary."""

    return (
        df.groupBy("payment_method")
        .agg(
            count("order_id").alias(
                "order_count"
            ),
            round(
                spark_sum("net_sales"),
                2,
            ).alias(
                "net_sales"
            ),
        )
        .orderBy("payment_method")
    )


def main():

    from transform import (
        create_spark_session,
        load_sales,
        transform_sales,
    )

    spark = create_spark_session()

    try:

        print(
            "Loading sales data..."
        )

        sales_df = load_sales(
            spark
        )

        transformed_df = transform_sales(
            sales_df
        )

        print(
            "\nDaily Sales:"
        )

        daily_sales = create_daily_sales(
            transformed_df
        )

        daily_sales.show(
            truncate=False
        )

        print(
            "\nCategory Sales:"
        )

        category_sales = create_category_sales(
            transformed_df
        )

        category_sales.show(
            truncate=False
        )

        print(
            "\nStore Sales:"
        )

        store_sales = create_store_sales(
            transformed_df
        )

        store_sales.show(
            truncate=False
        )

        print(
            "\nPayment Method Sales:"
        )

        payment_sales = (
            create_payment_method_sales(
                transformed_df
            )
        )

        payment_sales.show(
            truncate=False
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()