from google.cloud import bigquery


PROJECT_ID = "vast-falcon-415411"
DATASET_ID = "retail_analytics"


def main():
    client = bigquery.Client(
        project=PROJECT_ID
    )

    dataset_id = (
        f"{PROJECT_ID}.{DATASET_ID}"
    )

    dataset = client.get_dataset(
        dataset_id
    )

    print(
        "BigQuery connection successful!"
    )
    print(
        f"Project: {dataset.project}"
    )
    print(
        f"Dataset: {dataset.dataset_id}"
    )


if __name__ == "__main__":
    main()