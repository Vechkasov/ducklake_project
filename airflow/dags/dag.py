from datetime import timedelta

from airflow.models import DAG
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.utils import timezone

default_args = {
   'owner': 'airflow',
   'start_date': timezone.utcnow().replace(
      hour=0,
      minute=0,
      second=0,
      microsecond=0
   ) - timedelta(days=1),
}

with DAG(
   dag_id='lab02_dag',
   default_args=default_args,
   schedule=None
) as dag:
   lab02_agg_hourly = ClickHouseOperator(
        task_id='lab02_agg_hourly',
        database='default',
        sql=('''OPTIMIZE TABLE yana_glazova.yana_glazova_lab02_rt FINAL''',
            '''
                INSERT INTO yana_glazova.yana_glazova_lab02_agg_hourly
                (ts_start, ts_end, revenue, visitors, buyers, purchases, aov)
                SELECT 
                    toStartOfHour(timestamp) as ts_start,
                    ts_start + toIntervalHour(1) as ts_end,
                    sumIf(item_price, eventType='itemBuyEvent') as revenue,
                    uniq(partyId) as visitors,
                    uniqIf(partyId, eventType='itemBuyEvent') as buyers,
                    uniqIf(sessionId, eventType='itemBuyEvent') as purchases,
                    revenue/purchases as aov
                FROM yana_glazova.yana_glazova_lab02_rt
                WHERE detectedDuplicate = false and detectedCorruption = false
                    and timestamp < (SELECT max(toStartOfHour(timestamp)) max_hr
				                    FROM yana_glazova.yana_glazova_lab02_rt)
                GROUP BY 1, 2
            '''        ),
        query_id='{{ ti.dag_id }}-{{ ti.task_id }}-{{ ti.run_id }}-{{ ti.try_number }}',
        clickhouse_conn_id='npl_clickhouse',
    ) 


lab02_agg_hourly
