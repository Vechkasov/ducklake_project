from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from utils import load_chunk, stage_update, mart_update, download_and_save_to_minio



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


DIMENSIONS = ['browser_events', 'device_events', 'geo_events', 'location_events']

with DAG(
    'jsonl_to_minio',
    default_args=default_args,
    description='Ежечасная загрузка JSONL файлов в MinIO',
    schedule_interval='0 * * * *', 
    catchup=True,
    max_active_runs=1
) as dag:
    
    dimension_tasks = []
    
    for dimension in DIMENSIONS:
        load = PythonOperator(
            task_id=f'process_{dimension}',
            python_callable=download_and_save_to_minio,
            provide_context=True,
            params={
                'dimension': dimension,
                'bucket_name': 'ducklake'
            },
            trigger_rule='all_done'
        )

        insert = PythonOperator(
            task_id=f'insert_{dimension}',
            python_callable=load_chunk,
            provide_context=True,
            params={
                'dimension': dimension,
                'bucket_name': 'ducklake'
            },
            trigger_rule='all_done'
        )


        load >> insert
        dimension_tasks.append(insert)

    update_stage = PythonOperator(
            task_id='update_stage',
            python_callable=stage_update,
            provide_context=True,
            trigger_rule='all_done'
        )
    update_mart = PythonOperator(
            task_id='update_mart',
            python_callable=mart_update,
            provide_context=True,
            trigger_rule='all_done'
        )

dimension_tasks >> update_stage >> update_mart 
