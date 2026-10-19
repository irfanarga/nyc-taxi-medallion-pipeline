from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='nyc_taxi_medallion_pipeline',
    default_args=default_args,
    description='End-to-End Pipeline: Ingestion ke Bronze & Transformations ke Silver Layer',
    schedule_interval='0 2 1 * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['medallion', 'bronze', 'silver', 'nyc_taxi'],
) as dag:

    ingest_to_bronze = BashOperator(
        task_id='ingest_to_bronze_task',
        bash_command='python /opt/airflow/scripts/ingest_to_bronze.py',
    )

    transform_to_silver = BashOperator(
        task_id='transform_to_silver_task',
        bash_command='python /opt/airflow/scripts/transform_to_silver.py',
    )

    # Urutan eksekusi: Bronze -> Silver
    ingest_to_bronze >> transform_to_silver