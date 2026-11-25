from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def start_pipeline():
    print("Starting Data Pipeline")

def validate_data():
    print("Validating Data")

def finish_pipeline():
    print("Pipeline Completed")

with DAG(
    dag_id="first_data_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["training"]
) as dag:

    start = PythonOperator(
        task_id="start",
        python_callable=start_pipeline
    )

    validate = PythonOperator(
        task_id="validate",
        python_callable=validate_data
    )

    finish = PythonOperator(
        task_id="finish",
        python_callable=finish_pipeline
    )

    start >> validate >> finish