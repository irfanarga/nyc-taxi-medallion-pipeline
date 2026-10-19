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
    description='End-to-End Pipeline: Ingestion Bronze, Cleansing Silver, & dbt Gold Layer',
    schedule_interval='0 2 1 * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['medallion', 'bronze', 'silver', 'gold', 'dbt'],
) as dag:

    ingest_to_bronze = BashOperator(
        task_id='ingest_to_bronze_task',
        bash_command='python /opt/airflow/scripts/ingest_to_bronze.py',
    )

    transform_to_silver = BashOperator(
        task_id='transform_to_silver_task',
        bash_command='python /opt/airflow/scripts/transform_to_silver.py',
    )

    transform_to_gold = BashOperator(
        task_id='transform_to_gold_dbt_task',
        bash_command='python3 -m dbt.cli.main run --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project',
    )

    # Urutan Eksekusi End-to-End Medallion Pipeline
    ingest_to_bronze >> transform_to_silver >> transform_to_gold