
FROM python:3.10-slim

RUN pip install --user psycopg2-binary==2.9.9 'apache-airflow[postgres,clickhouse,minio]==2.10.2' \
 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.2/constraints-3.10.txt"

COPY requierments.txt .
RUN pip install --user --no-cache-dir -r requierments.txt

RUN mkdir -p /usr/local/airflow/dags

WORKDIR /usr/local/airflow
ENV AIRFLOW_HOME=/usr/local/airflow
ENV PATH=/root/.local/bin:$PATH

# меняем тип эксзекутора для Airflow на LocalExecutor (запускает задачи параллельно, но только на одной машине)
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
# указываем подключение к постгре
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
# отключаем загрузку примеров дагов
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
# отключаем загрузку соединений по умолчанию
ENV AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS=False
# и еще два флажка, полезных при отладке
# отображать имя хоста в логах
ENV AIRFLOW__CORE__EXPOSE_HOSTNAME=True
# отображать трассировку стека при возникновении ошибки
ENV AIRFLOW__CORE__EXPOSE_STACKTRACE=True

COPY airflow.cfg /usr/local/airflow/airflow.cfg
COPY dags/dag.py /usr/local/airflow/dags/dag.py
