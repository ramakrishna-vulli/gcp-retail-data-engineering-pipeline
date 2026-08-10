
from pathlib import Path

from analytics import (
    create_category_sales,
    create_daily_sales,
    create_payment_method_sales,
    create_store_sales,
)
from bigquery_loader import (
    create_bigquery_client,
    create_category_sales_schema,
    create_daily_sales_schema,
    create_payment_method_sales_schema,
    create_sales_schema,
    create_store_sales_schema,
    create_table_if_not_exists,
    load_dataframe_to_bigquery,
)
from data_quality import (
    print_quality_report,
    run_quality_checks,
)
from transform import (
    create_spark_session,
    load_sales,
    transform_sales,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent


def run_pipeline():

    print()
    print("=" * 60)
    print(
        "GCP RETAIL DATA ENGINEERING PIPELINE"
    )
    print("=" * 60)

    spark = create_spark_session()

    try:

        # --------------------------------------------------
        # 1. Load source data
        # --------------------------------------------------

        print()
        print(
            "[1/5] Loading source data..."
        )

        sales_df = load_sales(
            spark
        )

        source_count = sales_df.count()

        print(
            f"Records loaded: {source_count}"
        )

        # --------------------------------------------------
        # 2. Transform data
        # --------------------------------------------------

        print()
        print(
            "[2/5] Transforming sales data..."
        )

        transformed_df = transform_sales(
            sales_df
        )

        transformed_count = (
            transformed_df.count()
        )

        print(
            f"Records transformed: "
            f"{transformed_count}"
        )

        # --------------------------------------------------
        # 3. Data quality
        # --------------------------------------------------

        print()
        print(
            "[3/5] Running data quality checks..."
        )

        quality_results = (
            run_quality_checks(
                transformed_df
            )
        )

        print_quality_report(
            quality_results
        )

        if quality_results["status"] != "PASS":

            raise RuntimeError(
                "Data quality checks failed. "
                "Pipeline stopped."
            )

        # --------------------------------------------------
        # 4. Create analytics
        # --------------------------------------------------

        print()
        print(
            "[4/5] Creating analytics..."
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

        print(
            f"Daily sales: "
            f"{daily_df.count()}"
        )

        print(
            f"Category sales: "
            f"{category_df.count()}"
        )

        print(
            f"Store sales: "
            f"{store_df.count()}"
        )

        print(
            f"Payment methods: "
            f"{payment_df.count()}"
        )

        # --------------------------------------------------
        # 5. Load to BigQuery
        # --------------------------------------------------

        print()
        print(
            "[5/5] Loading data to BigQuery..."
        )

        client = create_bigquery_client()

        # Create tables.

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

        # Load sales.

        sales_table = (
            load_dataframe_to_bigquery(
                client,
                transformed_df,
                "sales",
                create_sales_schema(),
            )
        )

        # Load daily analytics.

        daily_table = (
            load_dataframe_to_bigquery(
                client,
                daily_df,
                "daily_sales",
                create_daily_sales_schema(),
            )
        )

        # Load category analytics.

        category_table = (
            load_dataframe_to_bigquery(
                client,
                category_df,
                "category_sales",
                create_category_sales_schema(),
            )
        )

        # Load store analytics.

        store_table = (
            load_dataframe_to_bigquery(
                client,
                store_df,
                "store_sales",
                create_store_sales_schema(),
            )
        )

        # Load payment analytics.

        payment_table = (
            load_dataframe_to_bigquery(
                client,
                payment_df,
                "payment_method_sales",
                create_payment_method_sales_schema(),
            )
        )

        # --------------------------------------------------
        # Final summary
        # --------------------------------------------------

        print()
        print("=" * 60)
        print(
            "PIPELINE COMPLETED SUCCESSFULLY"
        )
        print("=" * 60)

        print()
        print(
            "BigQuery tables:"
        )

        print(
            f"sales: "
            f"{sales_table.num_rows} rows"
        )

        print(
            f"daily_sales: "
            f"{daily_table.num_rows} rows"
        )

        print(
            f"category_sales: "
            f"{category_table.num_rows} rows"
        )

        print(
            f"store_sales: "
            f"{store_table.num_rows} rows"
        )

        print(
            f"payment_method_sales: "
            f"{payment_table.num_rows} rows"
        )

        print()
        print(
            "Project: vast-falcon-415411"
        )

        print(
            "Dataset: retail_analytics"
        )

        print("=" * 60)

    finally:

        spark.stop()


def main():
    run_pipeline()


if __name__ == "__main__":
    main()
