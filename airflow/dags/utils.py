import io
import os

import duckdb
import requests
import zipfile
from airflow.exceptions import AirflowException
from minio import Minio

from config import (MINIO_HOST, MINIO_SECRET, MINIO_KEY, MINIO_REGION, POSTGRES_PASSWORD,
                    POSTGRES_HOST, POSTGRES_PORT,POSTGRES_USER)


def chech_extensions(conn: duckdb.DuckDBPyConnection) -> None:
    print("Extensions:")
    conn.sql("""
        select extension_name, loaded, installed, description, aliases
        from duckdb_extensions()
        where 1 = 1
            and extension_name in ('ducklake', 'httpfs', 'postgres_scanner')
    """).show()

def create_secrets(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(f"""
    CREATE OR REPLACE SECRET minio_storage (
        TYPE s3,
        KEY_ID '{MINIO_KEY}',
        SECRET '{MINIO_SECRET}',
        ENDPOINT '{MINIO_HOST}',
        SCOPE 's3://ducklake/',
        REGION '{MINIO_REGION}',
        USE_SSL false,
        URL_STYLE 'path'
    );""")

    conn.execute(f"""
    CREATE OR REPLACE SECRET pg_meta (
        TYPE postgres,
        HOST '{POSTGRES_HOST}',
        PORT {POSTGRES_PORT},
        DATABASE 'ducklake_catalog',
        USER '{POSTGRES_USER}',
        PASSWORD '{POSTGRES_PASSWORD}'
    );""")

    conn.execute(f"""
    ATTACH 'ducklake:postgres:' AS lake (
        META_SECRET pg_meta
        -- , DATA_PATH 's3://ducklake'
    );
    USE lake;
    """)


def stage_update(**context):
    tables = ['conversion', 'devices', 'events', 'geo', 'purchases', 'session_info', 'transitions']
    execution_date = context['execution_date']
    with duckdb.connect() as conn:
        conn.execute("""
            INSTALL httpfs;   -- LOAD httpfs;
            INSTALL postgres; -- LOAD postgres;
            INSTALL ducklake; -- LOAD ducklake;
        """)

    with duckdb.connect() as conn:
        create_secrets(conn)
        current_dir = os.path.dirname(os.path.abspath(__file__))
        folder = os.path.join(current_dir, 'sql/stage')
        for t in tables:
            sql = f"""DROP TABLE IF EXISTS stage.{t}"""
            #print(f'{sql}')
            conn.sql(sql)
        for file_name in os.listdir(folder):
            with open(os.path.join(folder, file_name), 'r') as f:
                query = f.read()
                #print(f"{query}\n")
                conn.sql(query)
    print(f'Загрузка завершена за {execution_date}')


def mart_update(**context):
    tables = ['campaign_purchase_analysis_hourly', 'click_conversion_rates_hourly', 'events_distribution_hourly', 
              'purchased_products_count_hourly', 'top_performing_products', 'users_segments_agg', 'users_purchases_segments_agg']
    execution_date = context['execution_date']
    with duckdb.connect() as conn:
        conn.execute("""
            INSTALL httpfs;   -- LOAD httpfs;
            INSTALL postgres; -- LOAD postgres;
            INSTALL ducklake; -- LOAD ducklake;
        """)

    with duckdb.connect() as conn:
        create_secrets(conn)
        current_dir = os.path.dirname(os.path.abspath(__file__))
        folder = os.path.join(current_dir, 'sql/mart')
        for t in tables:
            sql = f"""DROP TABLE IF EXISTS mart.{t}"""
            #print(f'{sql}')
            conn.sql(sql)
        for file_name in os.listdir(folder):
            with open(os.path.join(folder, file_name), 'r') as f:
                query = f.read()
                #print(f"{query}\n")
                conn.sql(query)
    print(f'Загрузка завершена за {execution_date}')


def clean_files(**context):
    execution_date = context['execution_date']
    with duckdb.connect() as conn:
        create_secrets(conn)

        conn.sql("""
                    CALL ducklake_cleanup_old_files('lake', 
                                    cleanup_all => true)""")
        print(f'Done for {execution_date}')

def get_minio_client():
    return Minio(
        endpoint=MINIO_HOST, 
        access_key=MINIO_KEY, 
        secret_key=MINIO_SECRET,
        secure=False 
        )

def download_and_save_to_minio(**context):
    
    execution_date = context['execution_date']
    
    year = execution_date.year
    month = f"{execution_date.month}"
    day = f"{execution_date.day:02d}"
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

def remove_file(bucket_name: str, object_name: str):
    client = get_minio_client()
    client.remove_object(bucket_name=bucket_name, object_name=object_name)

def load_chunk(**context):
    dimension = context['params']['dimension']
    execution_date = context['execution_date']
    bucket_name = context['params']['bucket_name']
    
    year = execution_date.year
    month = f"{execution_date.month}"
    day = f"{execution_date.day:02d}"
    hour = f"{execution_date.hour:02d}"

    filename = f"{year}-{month}-{day}-H{hour}"

    with duckdb.connect() as conn:
        conn.execute("""
            INSTALL httpfs;   -- LOAD httpfs;
            INSTALL postgres; -- LOAD postgres;
            INSTALL ducklake; -- LOAD ducklake;
        """)


    with duckdb.connect() as conn:
        create_secrets(conn)
        print(f"Загрузка {filename} в {dimension}")
        try:
            conn.sql(f"""
                INSERT INTO raw.{dimension}
                select *
                from read_json('s3://{bucket_name}/data/{dimension}/{filename}.jsonl') """)
            print("Загрузка завершена")
        except Exception as e:
            raise(f"Загрузка не выполнена {e}")
    
    object_name = f"data/{dimension}/{filename}.jsonl"
    print(f'Remove {object_name}')
    remove_file(bucket_name, object_name)
