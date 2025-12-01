from airflow.models import Variable

MINIO_HOST = Variable.get('MINIO_HOST')
MINIO_KEY = Variable.get('MINIO_KEY')
MINIO_SECRET = Variable.get('MINIO_SECRET')
MINIO_REGION = Variable.get('MINIO_REGION')
POSTGRES_USER = Variable.get('POSTGRES_USER')
POSTGRES_PASSWORD = Variable.get('POSTGRES_PASSWORD')
POSTGRES_HOST = Variable.get('POSTGRES_HOST')
POSTGRES_PORT = Variable.get('POSTGRES_PORT')