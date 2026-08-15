from datetime import datetime, timedelta

from src.monitoring import (
    PipelineMetrics,
    start_monitoring,
    record_task_status,
    record_retry,
    complete_monitoring,
)


def test_start_monitoring():
    metrics = start_monitoring(
        project="vast-falcon-415411",
        dataset="retail_analytics",
        production_table="sales_partitioned",
    )

    assert metrics.status == "RUNNING"

    assert (
        metrics.project
        == "vast-falcon-415411"
    )

    assert (
        metrics.dataset
        == "retail_analytics"
    )

    assert (
        metrics.production_table
        == "sales_partitioned"
    )

    assert metrics.start_time is not None


def test_record_task_success():
    metrics = PipelineMetrics()

    record_task_status(
        metrics,
        "transform_sales",
        "SUCCESS",
    )

    assert (
        metrics.task_status[
            "transform_sales"
        ]
        == "SUCCESS"
    )

    assert metrics.failed_tasks == 0


def test_record_task_failure():
    metrics = PipelineMetrics()

    record_task_status(
        metrics,
        "data_quality",
        "FAILED",
    )

    assert (
        metrics.task_status[
            "data_quality"
        ]
        == "FAILED"
    )

    assert metrics.failed_tasks == 1


def test_record_retry():
    metrics = PipelineMetrics()

    assert metrics.retries == 0

    record_retry(metrics)

    assert metrics.retries == 1

    record_retry(metrics)

    assert metrics.retries == 2


def test_complete_monitoring_success():
    metrics = start_monitoring(
        project="vast-falcon-415411",
        dataset="retail_analytics",
        production_table="sales_partitioned",
    )

    completed = complete_monitoring(
        metrics,
        status="SUCCESS",
    )

    assert completed.status == "SUCCESS"

    assert completed.end_time is not None

    assert completed.duration_seconds >= 0


def test_complete_monitoring_failure():
    metrics = start_monitoring()

    completed = complete_monitoring(
        metrics,
        status="FAILED",
        error_message="Test pipeline failure",
    )

    assert completed.status == "FAILED"

    assert (
        completed.error_message
        == "Test pipeline failure"
    )

    assert completed.end_time is not None


def test_duration_calculation():
    start_time = datetime.now()

    end_time = (
        start_time
        + timedelta(seconds=10)
    )

    metrics = PipelineMetrics(
        start_time=start_time,
        end_time=end_time,
    )

    assert (
        metrics.duration_seconds
        == 10.0
    )