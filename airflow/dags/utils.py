import duckdb
import os
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

def load_chunk(**context):
    dimension = context['params']['dimension']
    execution_date = context['execution_date']
    bucket_name = context['params']['bucket_name']
    
    year = execution_date.year
    month = f"{execution_date.month}"
    day = f"{execution_date.day}"
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
        print('Done!')