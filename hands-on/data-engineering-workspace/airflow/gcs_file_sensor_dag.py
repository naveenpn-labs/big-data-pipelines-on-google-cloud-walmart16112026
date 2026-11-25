
from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.sensors.gcs import (
    GCSObjectsWithPrefixExistenceSensor
)

from airflow.providers.google.cloud.hooks.gcs import GCSHook

from google.cloud import bigquery

from datetime import datetime


# GCS Configuration
BUCKET_NAME = "walmart-sandbox-bucket"
GCS_PREFIX = "apache-airflow-labs/lab01/"

# BigQuery Configuration
PROJECT_ID = "sukhada-cb324"
DATASET_ID = "retail_streaming"


def filter_and_load_csv_files():

    # Connect to GCS using Airflow connection
    gcs_hook = GCSHook(
        gcp_conn_id="google_cloud_default"
    )

    # List all objects under the specified prefix
    files = gcs_hook.list(
        bucket_name=BUCKET_NAME,
        prefix=GCS_PREFIX
    )

    print("Files found in GCS:", files)

    # Filter only CSV files
    csv_files = [
        file for file in files
        if file.lower().endswith(".csv")
    ]

    print("CSV files identified:", csv_files)

    if not csv_files:
        print("No CSV files found. Nothing to load.")
        return

    # Initialize BigQuery client using Application Default Credentials
    client = bigquery.Client(project=PROJECT_ID)

    for file in csv_files:
        print(f"Processing file: {file}")
        # Extract filename without extension
        table_name = file.split("/")[-1].rsplit(".", 1)[0]
        table_id = (
            f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
        )

        # GCS source URI
        source_uri = f"gs://{BUCKET_NAME}/{file}"

        # BigQuery load configuration
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        # Load CSV into BigQuery
        load_job = client.load_table_from_uri(
            source_uri,
            table_id,
            job_config=job_config
        )

        load_job.result()

        print(
            f"Successfully loaded {file} into {table_id}"
        )

    print("CSV ingestion completed!")


with DAG(
    dag_id="gcs_directory_csv_to_bigquery",
    description="Monitor GCS directory and load only CSV files into BigQuery",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["GCS", "BigQuery", "CSV", "Training"]
) as dag:

    # Task 1: Monitor GCS directory
    wait_for_files = GCSObjectsWithPrefixExistenceSensor(
        task_id="wait_for_gcs_files",
        bucket=BUCKET_NAME,
        prefix=GCS_PREFIX,
        google_cloud_conn_id="google_cloud_default",
        poke_interval=30,
        timeout=600,
        mode="reschedule"
    )

    # Task 2: Filter CSV files and load into BigQuery
    load_csv_files = PythonOperator(
        task_id="filter_and_load_csv_files",
        python_callable=filter_and_load_csv_files
    )

    wait_for_files >> load_csv_files
