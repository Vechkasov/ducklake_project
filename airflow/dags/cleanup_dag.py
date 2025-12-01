from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
from utils import  clean_files
from config import MINIO_HOST, MINIO_SECRET, MINIO_KEY


default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 14, 23, 0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
    'max_active_runs': 1
}

with DAG(
    'cleanup_ducklake',
    default_args=default_args,
    description='Удаление ненужных файлов в ducklake',
    schedule_interval='0 0 * * *', 
    catchup=False,
    max_active_runs=1
) as dag:
    task = PythonOperator(
            task_id=f'clean_files',
            python_callable=clean_files,
            provide_context=True
        )