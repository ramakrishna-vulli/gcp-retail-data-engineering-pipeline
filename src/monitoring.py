"""
Pipeline monitoring and reporting.

This module provides reusable helpers for collecting pipeline
execution metrics and printing a production-style monitoring report.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class PipelineMetrics:
    """
    Stores metrics collected during a pipeline execution.
    """

    status: str = "RUNNING"

    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None

    source_records: int = 0
    transformed_records: int = 0
    new_records: int = 0

    data_quality_status: str = "NOT_RUN"
    bigquery_status: str = "NOT_RUN"

    retries: int = 0
    failed_tasks: int = 0

    production_table: str = ""
    dataset: str = ""
    project: str = ""

    task_status: dict = field(default_factory=dict)

    error_message: str = ""

    @property
    def duration_seconds(self) -> float:
        """
        Return pipeline duration in seconds.
        """

        if self.start_time is None:
            return 0.0

        end_time = self.end_time or datetime.now()

        return round(
            (end_time - self.start_time).total_seconds(),
            2,
        )


def start_monitoring(
    project: str = "",
    dataset: str = "",
    production_table: str = "",
) -> PipelineMetrics:
    """
    Create and start a new pipeline monitoring object.
    """

    metrics = PipelineMetrics(
        status="RUNNING",
        start_time=datetime.now(),
        project=project,
        dataset=dataset,
        production_table=production_table,
    )

    return metrics


def record_task_status(
    metrics: PipelineMetrics,
    task_name: str,
    status: str,
) -> None:
    """
    Record the status of an individual pipeline task.

    Expected statuses include:

        RUNNING
        SUCCESS
        FAILED
        RETRY
        SKIPPED
    """

    metrics.task_status[task_name] = status

    if status == "FAILED":
        metrics.failed_tasks += 1


def record_retry(
    metrics: PipelineMetrics,
) -> None:
    """
    Increment the retry counter.
    """

    metrics.retries += 1


def complete_monitoring(
    metrics: PipelineMetrics,
    status: str = "SUCCESS",
    error_message: str = "",
) -> PipelineMetrics:
    """
    Complete pipeline monitoring.
    """

    metrics.end_time = datetime.now()
    metrics.status = status
    metrics.error_message = error_message

    return metrics


def print_monitoring_report(
    metrics: PipelineMetrics,
) -> None:
    """
    Print a production-style pipeline monitoring report.
    """

    print()

    print("=" * 60)
    print("PIPELINE MONITORING REPORT")
    print("=" * 60)

    print()

    print(
        f"Status              : "
        f"{metrics.status}"
    )

    print(
        f"Duration            : "
        f"{metrics.duration_seconds} seconds"
    )

    print(
        f"Source Records      : "
        f"{metrics.source_records}"
    )

    print(
        f"Transformed Records : "
        f"{metrics.transformed_records}"
    )

    print(
        f"New Records         : "
        f"{metrics.new_records}"
    )

    print(
        f"Data Quality        : "
        f"{metrics.data_quality_status}"
    )

    print(
        f"BigQuery Load       : "
        f"{metrics.bigquery_status}"
    )

    print(
        f"Retries             : "
        f"{metrics.retries}"
    )

    print(
        f"Failed Tasks        : "
        f"{metrics.failed_tasks}"
    )

    print()

    print(
        f"Production Table    : "
        f"{metrics.production_table}"
    )

    print(
        f"Dataset             : "
        f"{metrics.dataset}"
    )

    print(
        f"Project             : "
        f"{metrics.project}"
    )

    if metrics.error_message:

        print()

        print(
            f"Error               : "
            f"{metrics.error_message}"
        )

    print()

    if metrics.task_status:

        print("-" * 60)
        print("TASK STATUS")
        print("-" * 60)

        for task_name, status in (
            metrics.task_status.items()
        ):

            print(
                f"{task_name:<30} : "
                f"{status}"
            )

    print()

    print("=" * 60)
    print("END MONITORING REPORT")
    print("=" * 60)