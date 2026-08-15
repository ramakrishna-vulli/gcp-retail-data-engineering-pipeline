"""
Lightweight local orchestration for the GCP Retail
Data Engineering Pipeline.

This module provides:

- Task dependencies
- Retry handling
- Task-level logging
- Failure handling
- Pipeline summary

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


# ============================================================
# Configuration
# ============================================================

MAX_RETRIES = 2

RETRY_DELAY_SECONDS = 5


# ============================================================
# Task Runner
# ============================================================

def run_task(
    task_name,
    task_function,
    context,
):
    """
    Execute one task with retry support.
    """

    print()
    print(
        "#" * 70
    )

    print(
        f"TASK STARTED: {task_name}"
    )

    print(
        "#" * 70
    )

    attempt = 0

    while True:

        attempt += 1

        start_time = (
            time.time()
        )

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

            context = (
                task_function(
                    context
                )
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

                raise

            print()
            print(
                f"Retrying in "
                f"{RETRY_DELAY_SECONDS} "
                f"seconds..."
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
    task dependencies.
    """

    start_time = (
        time.time()
    )

    start_timestamp = (
        datetime.now()
    )

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

    context = (
        PipelineContext()
    )

    pipeline_success = False

    try:

        # ====================================================
        # Task 1
        # ====================================================

        context = run_task(
            "create_spark_session",
            task_create_spark,
            context,
        )

        # ====================================================
        # Task 2
        # ====================================================

        context = run_task(
            "load_source_data",
            task_load_source,
            context,
        )

        # ====================================================
        # Task 3
        # ====================================================

        context = run_task(
            "transform_sales",
            task_transform,
            context,
        )

        # ====================================================
        # Task 4
        # ====================================================

        context = run_task(
            "data_quality",
            task_data_quality,
            context,
        )

        # ====================================================
        # Task 5
        # ====================================================

        context = run_task(
            "create_analytics",
            task_create_analytics,
            context,
        )

        # ====================================================
        # Task 6
        # ====================================================

        context = run_task(
            "create_bigquery_client",
            task_create_bigquery_client,
            context,
        )

        # ====================================================
        # Task 7
        # ====================================================

        context = run_task(
            "filter_new_sales",
            task_filter_new_sales,
            context,
        )

        # ====================================================
        # Task 8
        # ====================================================

        context = run_task(
            "load_bigquery",
            task_load_bigquery,
            context,
        )

        pipeline_success = True

    finally:

        # ====================================================
        # Cleanup
        # ====================================================

        try:

            context = run_task(
                "cleanup",
                task_cleanup,
                context,
            )

        except Exception as cleanup_error:

            print()
            print(
                "Cleanup failed:"
            )

            print(
                cleanup_error
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

    return context


# ============================================================
# Main
# ============================================================

def main():

    run_orchestrated_pipeline()


if __name__ == "__main__":
    main()