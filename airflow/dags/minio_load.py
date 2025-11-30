from datetime import datetime, timedelta
import requests
import zipfile
import io
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from minio import Minio

from utils import load_chunk, stage_update, mart_update
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

def download_and_save_to_minio(**context):
    
    execution_date = context['execution_date']
    
    year = execution_date.year
    month = f"{execution_date.month}"
    day = f"{execution_date.day}"
    hour = f"{execution_date.hour:02d}"
    
    dimension = context['params']['dimension']
    bucket_name = context['params']['bucket_name']
    
    file_path_template = f'year={year}/month={month}/day={day}/hour={hour}'
    
    def get_url_template(path: str, dimension: str) -> str:
        '''Функция для получения шаблона url'''
        url = f"https://storage.yandexcloud.net/npl-de17-lab8-data/{path}/{dimension}.jsonl.zip"
        return url
    
    url = get_url_template(file_path_template, dimension)
    
    try:
        print(f"Загружаем данные из: {url}")
        print(f"Время выполнения: {execution_date}")
        print(f"Dimension: {dimension}")
        
        response = requests.get(url, timeout=300)
        response.raise_for_status()
        
        def get_minio_client():

            return Minio(
                endpoint=MINIO_HOST, 
                access_key=MINIO_KEY, 
                secret_key=MINIO_SECRET,
                secure=False 
                )
        client = get_minio_client()
        
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"Бакет {bucket_name} создан")
        
        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
            zip_filename = zip_file.namelist()[0]
            with zip_file.open(zip_filename) as jsonl_file:
                jsonl_content = jsonl_file.read()
                
                object_name = f"data/{dimension}/{year}-{month}-{day}-H{hour}.jsonl"
                
                data_stream = io.BytesIO(jsonl_content)
                data_length = len(jsonl_content)
                
                client.put_object(
                    bucket_name=bucket_name,
                    object_name=object_name,
                    data=data_stream,
                    length=data_length,
                    content_type='application/jsonl'
                )
                
                result = {
                    'status': 'success',
                    'bucket': bucket_name,
                    'object': object_name,
                    'url': url,
                    'file_size': len(jsonl_content),
                    'execution_date': execution_date.isoformat(),
                    'dimension': dimension,
                    'year': year,
                    'month': month,
                    'day': day,
                    'hour': hour
                }
                
                print(f"Файл успешно загружен: {bucket_name}/{object_name}")
                
                return result
                
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            print(f"Файл не найден: {url}")
            return {
                'status': 'skipped',
                'reason': 'file_not_found',
                'url': url,
                'execution_date': execution_date.isoformat(),
                'dimension': dimension
            }
        else:
            raise AirflowException(f"Ошибка HTTP {e.response.status_code} при загрузке {url}: {e}")
    except requests.RequestException as e:
        raise AirflowException(f"Ошибка загрузки файла {url}: {e}")
    except zipfile.BadZipFile as e:
        raise AirflowException(f"Ошибка распаковки ZIP архива: {e}")
    except Exception as e:
        raise AirflowException(f"Ошибка при работе с MinIO: {e}")

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
            task_id=f'update_stage',
            python_callable=stage_update,
            provide_context=True,
            trigger_rule='all_done'
        )
    update_mart = PythonOperator(
            task_id=f'update_mart',
            python_callable=mart_update,
            provide_context=True,
            trigger_rule='all_done'
        )

dimension_tasks >> update_stage >> update_mart 