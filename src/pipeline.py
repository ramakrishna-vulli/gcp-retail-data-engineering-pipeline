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
    filter_new_sales,
    load_dataframe_to_bigquery,
    SALES_TABLE_NAME,
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


def run_pipeline():

    print()
    print("=" * 60)
    print(
        "GCP RETAIL DATA ENGINEERING PIPELINE"
    )
    print("=" * 60)

    spark = create_spark_session()

    try:

        # ====================================================
        # 1. Load source data
        # ====================================================

        print()
        print(
            "[1/5] Loading source data..."
        )

        sales_df = load_sales(
            spark
        )

        source_count = (
            sales_df.count()
        )

        print(
            f"Records loaded: {source_count}"
        )

        # ====================================================
        # 2. Transform data
        # ====================================================

        print()
        print(
            "[2/5] Transforming sales data..."
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
            f"Records transformed: "
            f"{transformed_count}"
        )

        # ====================================================
        # 3. Data quality
        # ====================================================

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

        # ====================================================
        # 4. Create analytics
        # ====================================================

        print()
        print(
            "[4/5] Creating analytics..."
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

        # ====================================================
        # 5. BigQuery loading
        # ====================================================

        print()
        print(
            "[5/5] Loading data to BigQuery..."
        )

        client = (
            create_bigquery_client()
        )

        # ----------------------------------------------------
        # Validate production sales table
        # ----------------------------------------------------

        from bigquery_loader import (
            validate_sales_table,
        )

        sales_table = validate_sales_table(
            client
        )

        print()
        print(
            f"Production sales table: "
            f"{SALES_TABLE_NAME}"
        )

        # ----------------------------------------------------
        # Incremental sales filtering
        # ----------------------------------------------------

        print()
        print(
            "Checking for new sales records..."
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
            f"Source records: "
            f"{transformed_count}"
        )

        print(
            f"Already loaded: "
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
            )
        )

        # ----------------------------------------------------
        # Analytics tables
        # ----------------------------------------------------

        daily_table = (
            load_dataframe_to_bigquery(
                client,
                daily_df,
                "daily_sales",
                create_daily_sales_schema(),
            )
        )

        category_table = (
            load_dataframe_to_bigquery(
                client,
                category_df,
                "category_sales",
                create_category_sales_schema(),
            )
        )

        store_table = (
            load_dataframe_to_bigquery(
                client,
                store_df,
                "store_sales",
                create_store_sales_schema(),
            )
        )

        payment_table = (
            load_dataframe_to_bigquery(
                client,
                payment_df,
                "payment_method_sales",
                create_payment_method_sales_schema(),
            )
        )

        # ====================================================
        # Final summary
        # ====================================================

        print()
        print("=" * 60)
        print(
            "PIPELINE COMPLETED SUCCESSFULLY"
        )
        print("=" * 60)

        print()

        print(
            f"Production sales table: "
            f"{SALES_TABLE_NAME}"
        )

        print(
            f"New sales loaded: "
            f"{new_sales_count}"
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
            "Project: "
            "vast-falcon-415411"
        )

        print(
            "Dataset: "
            "retail_analytics"
        )

        print("=" * 60)

    finally:

        spark.stop()


def main():

    run_pipeline()


if __name__ == "__main__":
    main()