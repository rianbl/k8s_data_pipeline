from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator

with DAG(dag_id='trigger_airbyte_job_example',
         default_args={'owner': 'airflow'},
         schedule_interval='*/30 * * * *',
         start_date=days_ago(1)
    ) as dag:

    money_to_json = AirbyteTriggerSyncOperator(
        task_id='airbyte_example',
        airbyte_conn_id='airbyte_conn_example',
        connection_id='20c74bf6-54fe-4016-8e1a-bcf0805636c2',
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )
