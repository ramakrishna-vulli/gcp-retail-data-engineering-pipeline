from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    when,
)


def check_nulls(
    df: DataFrame,
    columns: list[str],
) -> dict:
    """Check required columns for null values."""

    results = {}

    for column in columns:

        null_count = (
            df.filter(
                col(column).isNull()
            )
            .count()
        )

        results[column] = null_count

    return results


def check_positive_quantity(
    df: DataFrame,
) -> int:
    """Count records with invalid quantity."""

    return (
        df.filter(
            col("quantity") <= 0
        )
        .count()
    )


def check_valid_price(
    df: DataFrame,
) -> int:
    """Count records with invalid unit price."""

    return (
        df.filter(
            col("unit_price") < 0
        )
        .count()
    )


def check_valid_discount(
    df: DataFrame,
) -> int:
    """Count records with invalid discount."""

    return (
        df.filter(
            (col("discount") < 0)
            | (col("discount") > 1)
        )
        .count()
    )


def check_duplicate_orders(
    df: DataFrame,
) -> int:
    """Count duplicate order IDs."""

    total_count = df.count()

    distinct_count = (
        df.select("order_id")
        .distinct()
        .count()
    )

    return total_count - distinct_count


def run_quality_checks(
    df: DataFrame,
) -> dict:
    """Run all data-quality checks."""

    required_columns = [
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

    null_results = check_nulls(
        df,
        required_columns,
    )

    invalid_quantity = (
        check_positive_quantity(df)
    )

    invalid_price = (
        check_valid_price(df)
    )

    invalid_discount = (
        check_valid_discount(df)
    )

    duplicate_orders = (
        check_duplicate_orders(df)
    )

    total_nulls = sum(
        null_results.values()
    )

    total_errors = (
        total_nulls
        + invalid_quantity
        + invalid_price
        + invalid_discount
        + duplicate_orders
    )

    return {
        "nulls": null_results,
        "invalid_quantity": invalid_quantity,
        "invalid_price": invalid_price,
        "invalid_discount": invalid_discount,
        "duplicate_orders": duplicate_orders,
        "total_errors": total_errors,
        "status": (
            "PASS"
            if total_errors == 0
            else "FAIL"
        ),
    }


def print_quality_report(
    results: dict,
) -> None:
    """Print data-quality results."""

    print()
    print(
        "========== DATA QUALITY REPORT =========="
    )

    print(
        f"\nOverall status: "
        f"{results['status']}"
    )

    print(
        "\nNull checks:"
    )

    for column, count_value in (
        results["nulls"].items()
    ):
        print(
            f"  {column}: "
            f"{count_value}"
        )

    print(
        f"\nInvalid quantity: "
        f"{results['invalid_quantity']}"
    )

    print(
        f"Invalid price: "
        f"{results['invalid_price']}"
    )

    print(
        f"Invalid discount: "
        f"{results['invalid_discount']}"
    )

    print(
        f"Duplicate orders: "
        f"{results['duplicate_orders']}"
    )

    print(
        f"\nTotal quality errors: "
        f"{results['total_errors']}"
    )

    print(
        "=========================================="
    )


if __name__ == "__main__":

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
            f"Records checked: "
            f"{transformed_df.count()}"
        )

        results = run_quality_checks(
            transformed_df
        )

        print_quality_report(
            results
        )

    finally:

        spark.stop()