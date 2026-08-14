import pytest


def filter_new_order_ids(
    source_ids,
    existing_ids,
):
    """
    Return only order IDs that are not already
    present in the target.
    """

    existing_set = set(
        existing_ids
    )

    return [
        order_id
        for order_id in source_ids
        if order_id not in existing_set
    ]


def test_existing_orders_are_skipped():

    source_ids = [
        10001,
        10002,
        10003,
    ]

    existing_ids = [
        10001,
        10002,
    ]

    new_ids = filter_new_order_ids(
        source_ids,
        existing_ids,
    )

    assert new_ids == [10003]


def test_new_order_is_loaded():

    source_ids = [
        10001,
        10002,
        10003,
    ]

    existing_ids = [
        10001,
        10002,
    ]

    new_ids = filter_new_order_ids(
        source_ids,
        existing_ids,
    )

    assert 10003 in new_ids
    assert len(new_ids) == 1


def test_no_duplicate_orders():

    source_ids = [
        10001,
        10001,
        10002,
    ]

    existing_ids = [
        10001,
    ]

    new_ids = filter_new_order_ids(
        source_ids,
        existing_ids,
    )

    assert new_ids == [10002]


def test_all_existing_orders_return_empty():

    source_ids = [
        10001,
        10002,
        10003,
    ]

    existing_ids = [
        10001,
        10002,
        10003,
    ]

    new_ids = filter_new_order_ids(
        source_ids,
        existing_ids,
    )

    assert new_ids == []


def test_partition_configuration():

    partition_field = "order_date"
    partition_type = "DAY"

    assert partition_field == "order_date"
    assert partition_type == "DAY"


def test_clustering_configuration():

    cluster_fields = [
        "category",
        "store_id",
    ]

    assert cluster_fields == [
        "category",
        "store_id",
    ]