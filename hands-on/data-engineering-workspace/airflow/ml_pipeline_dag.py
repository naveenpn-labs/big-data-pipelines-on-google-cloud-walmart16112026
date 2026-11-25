
from datetime import datetime, timedelta
import os
import re
import tempfile
import pickle

import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_score, recall_score

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook


# --------------------------------------------------
# Configuration
# --------------------------------------------------

BUCKET_NAME = "walmart-sandbox-bucket"

INPUT_FILE = "ml-pipeline/input/customer_data.csv"

PROCESSED_PREFIX = "ml-pipeline/processed"
ARTIFACT_PREFIX = "ml-pipeline/artifacts"
APPROVED_PREFIX = "ml-pipeline/models/approved"

ACCURACY_THRESHOLD = 0.75

FEATURES = [
    "Age",
    "Salary",
    "Previous_Purchases"
]

TARGET = "Purchased"


# --------------------------------------------------
# Utility function
# --------------------------------------------------

def get_run_folder(run_id):
    return re.sub(r"[^a-zA-Z0-9_-]", "_", run_id)


# --------------------------------------------------
# Task 1: Extract and preprocess data
# --------------------------------------------------

def extract_preprocess_data(**context):

    hook = GCSHook(
        gcp_conn_id="google_cloud_default"
    )

    run_folder = get_run_folder(context["run_id"])

    with tempfile.TemporaryDirectory() as temp_dir:

        local_input = os.path.join(
            temp_dir, "customer_data.csv"
        )

        local_processed = os.path.join(
            temp_dir, "processed_customer_data.csv"
        )

        hook.download(
            bucket_name=BUCKET_NAME,
            object_name=INPUT_FILE,
            filename=local_input
        )

        df = pd.read_csv(local_input)

        # Remove duplicate and incomplete records
        df = df.drop_duplicates()
        df = df.dropna()

        # Retain required columns
        df = df[
            ["Customer_ID"] + FEATURES + [TARGET]
        ]

        # Save processed dataset
        df.to_csv(local_processed, index=False)

        processed_path = (
            f"{PROCESSED_PREFIX}/{run_folder}/"
            "processed_customer_data.csv"
        )

        hook.upload(
            bucket_name=BUCKET_NAME,
            object_name=processed_path,
            filename=local_processed
        )

    print("Preprocessing completed")
    print(f"Records processed: {len(df)}")

    return processed_path


# --------------------------------------------------
# Task 2: Train classification model
# --------------------------------------------------

def train_model(**context):

    hook = GCSHook(
        gcp_conn_id="google_cloud_default"
    )

    run_folder = get_run_folder(context["run_id"])

    ti = context["ti"]

    processed_path = ti.xcom_pull(
        task_ids="extract_preprocess_data"
    )

    with tempfile.TemporaryDirectory() as temp_dir:

        local_data = os.path.join(
            temp_dir, "processed_customer_data.csv"
        )

        local_model = os.path.join(
            temp_dir, "customer_model.pkl"
        )

        local_test = os.path.join(
            temp_dir, "test_data.csv"
        )

        hook.download(
            bucket_name=BUCKET_NAME,
            object_name=processed_path,
            filename=local_data
        )

        df = pd.read_csv(local_data)

        X = df[FEATURES]
        y = df[TARGET]

        # Split data into training and testing
        X_train, X_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=0.20,
            random_state=42,
            stratify=y
        )

        # Build classification pipeline
        model = Pipeline([
            ("scaler", StandardScaler()),
            ("classifier", LogisticRegression(
                max_iter=1000,
                random_state=42
            ))
        ])

        # Train the model
        model.fit(X_train, y_train)

        # Save model
        with open(local_model, "wb") as file:
            pickle.dump(model, file)

        # Save test data for evaluation
        test_df = X_test.copy()
        test_df[TARGET] = y_test

        test_df.to_csv(local_test, index=False)

        model_path = (
            f"{ARTIFACT_PREFIX}/{run_folder}/customer_model.pkl"
        )

        test_path = (
            f"{ARTIFACT_PREFIX}/{run_folder}/test_data.csv"
        )

        hook.upload(
            bucket_name=BUCKET_NAME,
            object_name=model_path,
            filename=local_model
        )

        hook.upload(
            bucket_name=BUCKET_NAME,
            object_name=test_path,
            filename=local_test
        )

    print("Model training completed")

    return {
        "model_path": model_path,
        "test_path": test_path
    }


# --------------------------------------------------
# Task 3: Evaluate classification model
# --------------------------------------------------

def evaluate_model(**context):

    hook = GCSHook(
        gcp_conn_id="google_cloud_default"
    )

    ti = context["ti"]

    artifacts = ti.xcom_pull(
        task_ids="train_model"
    )

    with tempfile.TemporaryDirectory() as temp_dir:

        local_model = os.path.join(
            temp_dir, "customer_model.pkl"
        )

        local_test = os.path.join(
            temp_dir, "test_data.csv"
        )

        hook.download(
            bucket_name=BUCKET_NAME,
            object_name=artifacts["model_path"],
            filename=local_model
        )

        hook.download(
            bucket_name=BUCKET_NAME,
            object_name=artifacts["test_path"],
            filename=local_test
        )

        # Load trained model
        with open(local_model, "rb") as file:
            model = pickle.load(file)

        test_df = pd.read_csv(local_test)

        X_test = test_df[FEATURES]
        y_test = test_df[TARGET]

        # Make predictions
        predictions = model.predict(X_test)

        # Calculate evaluation metrics
        accuracy = accuracy_score(
            y_test, predictions
        )

        precision = precision_score(
            y_test, predictions, zero_division=0
        )

        recall = recall_score(
            y_test, predictions, zero_division=0
        )

    print(f"Accuracy  : {accuracy:.2%}")
    print(f"Precision : {precision:.2%}")
    print(f"Recall    : {recall:.2%}")

    return {
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall
    }


# --------------------------------------------------
# Task 4: Decide whether to deploy the model
# --------------------------------------------------

def check_model_accuracy(**context):

    ti = context["ti"]

    metrics = ti.xcom_pull(
        task_ids="evaluate_model"
    )

    accuracy = metrics["accuracy"]

    print(f"Model accuracy: {accuracy:.2%}")

    if accuracy >= ACCURACY_THRESHOLD:
        print("Model approved for deployment")
        return "deploy_model"

    print("Model rejected")
    return "reject_model"


# --------------------------------------------------
# Task 5: Deploy approved model
# --------------------------------------------------

def deploy_model(**context):

    hook = GCSHook(
        gcp_conn_id="google_cloud_default"
    )

    ti = context["ti"]

    artifacts = ti.xcom_pull(
        task_ids="train_model"
    )

    run_folder = get_run_folder(context["run_id"])

    source_path = artifacts["model_path"]

    destination_path = (
        f"{APPROVED_PREFIX}/{run_folder}/"
        "customer_model.pkl"
    )

    with tempfile.TemporaryDirectory() as temp_dir:

        local_model = os.path.join(
            temp_dir, "customer_model.pkl"
        )

        hook.download(
            bucket_name=BUCKET_NAME,
            object_name=source_path,
            filename=local_model
        )

        hook.upload(
            bucket_name=BUCKET_NAME,
            object_name=destination_path,
            filename=local_model
        )

    print("Model successfully deployed")
    print(f"Approved model: gs://{BUCKET_NAME}/{destination_path}")


# --------------------------------------------------
# Task 6: Reject model
# --------------------------------------------------

def reject_model():

    print(
        "Model accuracy is below the required threshold."
    )

    print("Model deployment rejected.")


# --------------------------------------------------
# DAG definition
# --------------------------------------------------

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}


with DAG(
    dag_id="customer_purchase_ml_pipeline",
    default_args=default_args,
    description="Customer purchase prediction classification pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ML", "Classification", "GCS"]
) as dag:

    extract_task = PythonOperator(
        task_id="extract_preprocess_data",
        python_callable=extract_preprocess_data
    )

    train_task = PythonOperator(
        task_id="train_model",
        python_callable=train_model
    )

    evaluate_task = PythonOperator(
        task_id="evaluate_model",
        python_callable=evaluate_model
    )

    decision_task = BranchPythonOperator(
        task_id="check_model_accuracy",
        python_callable=check_model_accuracy
    )

    deploy_task = PythonOperator(
        task_id="deploy_model",
        python_callable=deploy_model
    )

    reject_task = PythonOperator(
        task_id="reject_model",
        python_callable=reject_model
    )

    # Define task dependencies
    extract_task >> train_task >> evaluate_task >> decision_task

    decision_task >> [deploy_task, reject_task]
