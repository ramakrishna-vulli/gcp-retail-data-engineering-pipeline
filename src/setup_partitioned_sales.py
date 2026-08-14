
from google.cloud import bigquery


PROJECT_ID = "vast-falcon-415411"
DATASET_ID = "retail_analytics"

SOURCE_TABLE = (
    f"{PROJECT_ID}.{DATASET_ID}.sales"
)

TARGET_TABLE = (
    f"{PROJECT_ID}.{DATASET_ID}.sales_partitioned"
)


def main():

    print()
    print("=" * 60)
    print("BIGQUERY PARTITIONING AND CLUSTERING SETUP")
    print("=" * 60)

    client = bigquery.Client(
        project=PROJECT_ID
    )

    # --------------------------------------------------------
    # Check source table
    # --------------------------------------------------------

    print()
    print(
        f"Checking source table: {SOURCE_TABLE}"
    )

    source_table = client.get_table(
        SOURCE_TABLE
    )

    print(
        f"Source rows: {source_table.num_rows}"
    )

    # --------------------------------------------------------
    # Check whether target already exists
    # --------------------------------------------------------

    try:

        target_table = client.get_table(
            TARGET_TABLE
        )

        print()
        print(
            f"Target table already exists: "
            f"{TARGET_TABLE}"
        )

        print(
            f"Target rows: "
            f"{target_table.num_rows}"
        )

        print(
            "No migration required."
        )

        return

    except Exception:

        print()
        print(
            "Partitioned target table does not exist."
        )

    # --------------------------------------------------------
    # Create partitioned + clustered table
    # --------------------------------------------------------

    query = f"""
    CREATE TABLE `{TARGET_TABLE}`
    PARTITION BY order_date
    CLUSTER BY category, store_id
    AS
    SELECT *
    FROM `{SOURCE_TABLE}`
    """

    print()
    print(
        "Creating partitioned and clustered table..."
    )

    query_job = client.query(
        query
    )

    query_job.result()

    # --------------------------------------------------------
    # Validate target
    # --------------------------------------------------------

    target_table = client.get_table(
        TARGET_TABLE
    )

    print()
    print(
        "=" * 60
    )

    print(
        "PARTITIONED TABLE CREATED SUCCESSFULLY"
    )

    print(
        "=" * 60
    )

    print()
    print(
        f"Target table: {TARGET_TABLE}"
    )

    print(
        f"Rows copied: {target_table.num_rows}"
    )

    print()
    print(
        "Partitioning:"
    )

    if target_table.time_partitioning:

        print(
            f"Field: "
            f"{target_table.time_partitioning.field}"
        )

        print(
            f"Type: "
            f"{target_table.time_partitioning.type_}"
        )

    else:

        print(
            "No time partitioning detected."
        )

    print()
    print(
        "Clustering:"
    )

    if target_table.clustering_fields:

        print(
            ", ".join(
                target_table.clustering_fields
            )
        )

    else:

        print(
            "No clustering detected."
        )

    print()
    print(
        "=" * 60
    )


if __name__ == "__main__":
    main()
