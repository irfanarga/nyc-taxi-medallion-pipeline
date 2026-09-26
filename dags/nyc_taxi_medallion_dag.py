from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Default arguments untuk DAG Airflow
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definisi DAG Airflow
with DAG(
    dag_id='nyc_taxi_bronze_ingestion',
    default_args=default_args,
    description='Pipeline Ingestion Data NYC Taxi dari 2025 ke Bronze Layer (MinIO/GCS)',
    schedule_interval='0 2 1 * *',  # Berjalan otomatis tanggal 1 setiap bulan jam 02:00
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['medallion', 'bronze', 'nyc_taxi'],
) as dag:

    # Task untuk menjalankan skrip Ingestion
    ingest_to_bronze_task = BashOperator(
        task_id='ingest_to_bronze_task',
        bash_command='python /opt/airflow/scripts/ingest_to_bronze.py',
    )

    ingest_to_bronze_task