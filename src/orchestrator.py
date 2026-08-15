"""
Lightweight local orchestration for the GCP Retail
Data Engineering Pipeline.

This module provides:

- Task dependencies
- Retry handling
- Task-level logging
- Failure handling
- Pipeline summary
- Pipeline monitoring

The design is intentionally Airflow-compatible.

The same task functions can later be mapped to
Airflow PythonOperator / TaskFlow tasks.
"""

import time
from datetime import datetime

from src.tasks import (
    PipelineContext,
    task_create_spark,
    task_load_source,
    task_transform,
    task_data_quality,
    task_create_analytics,
    task_create_bigquery_client,
    task_filter_new_sales,
    task_load_bigquery,
    task_cleanup,
)

from src.monitoring import (
    start_monitoring,
    record_task_status,
    record_retry,
    complete_monitoring,
    print_monitoring_report,
)


# ============================================================
# Configuration
# ============================================================

MAX_RETRIES = 2

RETRY_DELAY_SECONDS = 5

PROJECT_ID = "vast-falcon-415411"

DATASET_ID = "retail_analytics"

PRODUCTION_TABLE = "sales_partitioned"


# ============================================================
# Task Runner
# ============================================================

def run_task(
    task_name,
    task_function,
    context,
    metrics=None,
):
    """
    Execute one task with retry support.

    If monitoring metrics are provided, task status
    and retry information are recorded.
    """

    print()
    print("#" * 70)

    print(
        f"TASK STARTED: {task_name}"
    )

    print("#" * 70)

    attempt = 0

    if metrics is not None:

        record_task_status(
            metrics,
            task_name,
            "RUNNING",
        )

    while True:

        attempt += 1

        start_time = time.time()

        try:

            print()
            print(
                f"Task: {task_name}"
            )

            print(
                f"Attempt: "
                f"{attempt}/"
                f"{MAX_RETRIES + 1}"
            )

            context = task_function(
                context
            )

            duration = (
                time.time()
                - start_time
            )

            print()
            print(
                f"TASK SUCCESS: "
                f"{task_name}"
            )

            print(
                f"Duration: "
                f"{duration:.2f} seconds"
            )

            if metrics is not None:

                record_task_status(
                    metrics,
                    task_name,
                    "SUCCESS",
                )

            return context

        except Exception as exc:

            duration = (
                time.time()
                - start_time
            )

            print()
            print(
                f"TASK FAILED: "
                f"{task_name}"
            )

            print(
                f"Duration: "
                f"{duration:.2f} seconds"
            )

            print(
                f"Error: {exc}"
            )

            if attempt > MAX_RETRIES:

                print()
                print(
                    f"No retries remaining "
                    f"for task: {task_name}"
                )

                if metrics is not None:

                    record_task_status(
                        metrics,
                        task_name,
                        "FAILED",
                    )

                raise

            print()
            print(
                f"Retrying in "
                f"{RETRY_DELAY_SECONDS} "
                f"seconds..."
            )

            if metrics is not None:

                record_retry(
                    metrics
                )

                record_task_status(
                    metrics,
                    task_name,
                    "RETRY",
                )

            time.sleep(
                RETRY_DELAY_SECONDS
            )


# ============================================================
# Pipeline
# ============================================================

def run_orchestrated_pipeline():
    """
    Execute the complete pipeline using explicit
    task dependencies and pipeline monitoring.
    """

    start_time = time.time()

    start_timestamp = datetime.now()

    print()
    print("=" * 70)

    print(
        "GCP RETAIL DATA ENGINEERING PIPELINE"
    )

    print(
        "LIGHTWEIGHT ORCHESTRATOR"
    )

    print("=" * 70)

    print()

    print(
        f"Started: "
        f"{start_timestamp}"
    )

    # --------------------------------------------------------
    # Start monitoring
    # --------------------------------------------------------

    metrics = start_monitoring(
        project=PROJECT_ID,
        dataset=DATASET_ID,
        production_table=PRODUCTION_TABLE,
    )

    context = PipelineContext()

    pipeline_success = False

    pipeline_error = ""

    try:

        # ====================================================
        # Task 1
        # ====================================================

        context = run_task(
            "create_spark_session",
            task_create_spark,
            context,
            metrics,
        )

        # ====================================================
        # Task 2
        # ====================================================

        context = run_task(
            "load_source_data",
            task_load_source,
            context,
            metrics,
        )

        # ====================================================
        # Task 3
        # ====================================================

        context = run_task(
            "transform_sales",
            task_transform,
            context,
            metrics,
        )

        # ====================================================
        # Task 4
        # ====================================================

        context = run_task(
            "data_quality",
            task_data_quality,
            context,
            metrics,
        )

        # ====================================================
        # Task 5
        # ====================================================

        context = run_task(
            "create_analytics",
            task_create_analytics,
            context,
            metrics,
        )

        # ====================================================
        # Task 6
        # ====================================================

        context = run_task(
            "create_bigquery_client",
            task_create_bigquery_client,
            context,
            metrics,
        )

        # ====================================================
        # Task 7
        # ====================================================

        context = run_task(
            "filter_new_sales",
            task_filter_new_sales,
            context,
            metrics,
        )

        # ====================================================
        # Task 8
        # ====================================================

        context = run_task(
            "load_bigquery",
            task_load_bigquery,
            context,
            metrics,
        )

        pipeline_success = True

    except Exception as exc:

        pipeline_error = str(exc)

        print()
        print(
            "Pipeline execution failed:"
        )

        print(
            pipeline_error
        )

    finally:

        # ====================================================
        # Task 9 - Cleanup
        # ====================================================

        try:

            context = run_task(
                "cleanup",
                task_cleanup,
                context,
                metrics,
            )

        except Exception as cleanup_error:

            print()
            print(
                "Cleanup failed:"
            )

            print(
                cleanup_error
            )

            if not pipeline_success:

                pipeline_error = (
                    pipeline_error
                    or str(cleanup_error)
                )

    # ========================================================
    # Collect Pipeline Metrics
    # ========================================================

    if (
        context.source_count
        is not None
    ):

        metrics.source_records = (
            context.source_count
        )

    if (
        context.transformed_count
        is not None
    ):

        metrics.transformed_records = (
            context.transformed_count
        )

    if (
        context.new_sales_count
        is not None
    ):

        metrics.new_records = (
            context.new_sales_count
        )

    # --------------------------------------------------------
    # Data quality status
    # --------------------------------------------------------

    if (
        metrics.task_status.get(
            "data_quality"
        )
        == "SUCCESS"
    ):

        metrics.data_quality_status = (
            "PASS"
        )

    elif (
        metrics.task_status.get(
            "data_quality"
        )
        == "FAILED"
    ):

        metrics.data_quality_status = (
            "FAIL"
        )

    # --------------------------------------------------------
    # BigQuery status
    # --------------------------------------------------------

    if (
        metrics.task_status.get(
            "load_bigquery"
        )
        == "SUCCESS"
    ):

        metrics.bigquery_status = (
            "SUCCESS"
        )

    elif (
        metrics.task_status.get(
            "load_bigquery"
        )
        == "FAILED"
    ):

        metrics.bigquery_status = (
            "FAILED"
        )

    # ========================================================
    # Complete Monitoring
    # ========================================================

    if pipeline_success:

        complete_monitoring(
            metrics,
            status="SUCCESS",
        )

    else:

        complete_monitoring(
            metrics,
            status="FAILED",
            error_message=pipeline_error,
        )

    # ========================================================
    # Final Summary
    # ========================================================

    duration = (
        time.time()
        - start_time
    )

    print()
    print("=" * 70)

    if pipeline_success:

        print(
            "PIPELINE COMPLETED SUCCESSFULLY"
        )

    else:

        print(
            "PIPELINE FAILED"
        )

    print("=" * 70)

    print()

    print(
        f"Duration: "
        f"{duration:.2f} seconds"
    )

    if (
        context.source_count
        is not None
    ):

        print(
            f"Source records: "
            f"{context.source_count}"
        )

    if (
        context.transformed_count
        is not None
    ):

        print(
            f"Transformed records: "
            f"{context.transformed_count}"
        )

    print(
        f"New records: "
        f"{context.new_sales_count}"
    )

    print()

    print("=" * 70)

    # ========================================================
    # Monitoring Report
    # ========================================================

    print_monitoring_report(
        metrics
    )

    return context


# ============================================================
# Main
# ============================================================

def main():

    run_orchestrated_pipeline()


if __name__ == "__main__":

    main()