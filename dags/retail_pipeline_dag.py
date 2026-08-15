"""
Retail GCP Data Engineering Pipeline - Airflow DAG

This DAG orchestrates the existing production pipeline.

Current architecture:

    GCS
     |
     v
    PySpark
     |
     v
    Data Quality
     |
     v
    Analytics
     |
     v
    Incremental BigQuery Load

The existing src/pipeline.py remains responsible for
the actual data processing logic.

Airflow is responsible for scheduling and orchestration.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


# ============================================================
# Configuration
# ============================================================

DAG_ID = "gcp_retail_data_engineering_pipeline"


# ============================================================
# Default arguments
# ============================================================

default_args = {
    "owner": "ramakrishna",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ============================================================
# Pipeline execution
# ============================================================

def run_retail_pipeline():
    """
    Execute the existing retail pipeline.

    The actual pipeline logic remains in:

        src/pipeline.py

    This keeps Airflow focused on orchestration rather than
    duplicating business logic.
    """

    import sys
    from pathlib import Path

    # --------------------------------------------------------
    # Project root
    # --------------------------------------------------------

    project_root = (
        Path(__file__)
        .resolve()
        .parent
        .parent
    )

    src_path = (
        project_root
        / "src"
    )

    # --------------------------------------------------------
    # Add src directory to Python path
    # --------------------------------------------------------

    if str(src_path) not in sys.path:

        sys.path.insert(
            0,
            str(src_path),
        )

    # --------------------------------------------------------
    # Import existing pipeline
    # --------------------------------------------------------

    from pipeline import run_pipeline

    # --------------------------------------------------------
    # Execute existing pipeline
    # --------------------------------------------------------

    print()
    print("=" * 70)
    print(
        "AIRFLOW: STARTING GCP RETAIL DATA ENGINEERING PIPELINE"
    )
    print("=" * 70)

    print()
    print(
        f"Project root: {project_root}"
    )

    print(
        f"Source path: {src_path}"
    )

    print()

    run_pipeline()

    print()
    print("=" * 70)
    print(
        "AIRFLOW: GCP RETAIL DATA ENGINEERING PIPELINE COMPLETED"
    )
    print("=" * 70)


# ============================================================
# DAG definition
# ============================================================

with DAG(
    dag_id=DAG_ID,

    description=(
        "Orchestrates the GCS to PySpark to BigQuery "
        "retail data engineering pipeline."
    ),

    default_args=default_args,

    start_date=datetime(
        2026,
        8,
        1,
    ),

    schedule=None,

    catchup=False,

    tags=[
        "gcp",
        "retail",
        "pyspark",
        "bigquery",
        "gcs",
        "data-engineering",
    ],
) as dag:

    run_pipeline_task = PythonOperator(
        task_id="run_retail_pipeline",

        python_callable=(
            run_retail_pipeline
        ),

    )

    run_pipeline_task