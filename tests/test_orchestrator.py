"""
Tests for the lightweight pipeline orchestrator.

These tests validate:
    - Successful task execution
    - Task retry behavior
    - Task failure after retries
    - Pipeline task dependency/order

The tests do not call GCS or BigQuery.
They use small mock functions so the test suite remains
fast and independent of cloud services.
"""

import pytest

from src.orchestrator import run_task


# ============================================================
# Test 1 — Successful Task
# ============================================================

def test_task_success():

    context = {
        "value": 0
    }

    def successful_task(context):

        context["value"] += 1

        return context

    result = run_task(
        "successful_task",
        successful_task,
        context,
    )

    assert result["value"] == 1


# ============================================================
# Test 2 — Task Retry
# ============================================================

def test_task_retry():

    context = {
        "attempts": 0
    }

    def retry_task(context):

        context["attempts"] += 1

        if context["attempts"] < 2:

            raise RuntimeError(
                "Temporary failure"
            )

        return context

    result = run_task(
        "retry_task",
        retry_task,
        context,
    )

    assert result["attempts"] == 2


# ============================================================
# Test 3 — Task Failure After Retries
# ============================================================

def test_task_failure_after_retries():

    context = {
        "attempts": 0
    }

    def failing_task(context):

        context["attempts"] += 1

        raise RuntimeError(
            "Permanent failure"
        )

    with pytest.raises(
        RuntimeError,
        match="Permanent failure",
    ):

        run_task(
            "failing_task",
            failing_task,
            context,
        )

    # MAX_RETRIES = 2
    # Therefore the task runs:
    #
    # Attempt 1
    # Attempt 2
    # Attempt 3
    #
    # Total = 3 attempts

    assert context["attempts"] == 3


# ============================================================
# Test 4 — Task Dependency / Order
# ============================================================

def test_pipeline_task_order():

    execution_order = []

    context = {}

    def task_one(context):

        execution_order.append(
            "task_one"
        )

        context["task_one"] = True

        return context

    def task_two(context):

        assert (
            context.get("task_one")
            is True
        )

        execution_order.append(
            "task_two"
        )

        context["task_two"] = True

        return context

    def task_three(context):

        assert (
            context.get("task_two")
            is True
        )

        execution_order.append(
            "task_three"
        )

        context["task_three"] = True

        return context

    context = run_task(
        "task_one",
        task_one,
        context,
    )

    context = run_task(
        "task_two",
        task_two,
        context,
    )

    context = run_task(
        "task_three",
        task_three,
        context,
    )

    assert execution_order == [
        "task_one",
        "task_two",
        "task_three",
    ]

    assert (
        context["task_one"]
        is True
    )

    assert (
        context["task_two"]
        is True
    )

    assert (
        context["task_three"]
        is True
    )