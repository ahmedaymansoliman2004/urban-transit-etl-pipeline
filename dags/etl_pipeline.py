from datetime import datetime, timedelta
import sys

sys.path.append("/opt/airflow/project")

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.run_ingestion_validation import run_ingestion_validation
from src.run_cleaning import run_cleaning_pipeline
from src.run_transformation import run_transformation_pipeline
from src.run_parquet import run_parquet_pipeline
from src.run_bigquery import run_bigquery_pipeline

default_args = {
    "owner": "Mai",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="etl_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ETL"],
) as dag:

    ingestion_validation = PythonOperator(
        task_id="ingestion_validation",
        python_callable=run_ingestion_validation,
        do_xcom_push=False,
    )

    cleaning = PythonOperator(
        task_id="cleaning",
        python_callable=run_cleaning_pipeline,
        do_xcom_push=False,
    )

    transformation = PythonOperator(
        task_id="transformation",
        python_callable=run_transformation_pipeline,
        do_xcom_push=False,
    )

    parquet = PythonOperator(
        task_id="parquet_export",
        python_callable=run_parquet_pipeline,
        do_xcom_push=False,
    )

    bigquery = PythonOperator(
        task_id="bigquery_load",
        python_callable=run_bigquery_pipeline,
        do_xcom_push=False,
    )

    ingestion_validation >> cleaning >> transformation >> parquet >> bigquery