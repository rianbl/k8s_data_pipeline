from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'submit_spark_job',
    default_args=default_args,
    schedule_interval='*/15 * * * *',  # Run every 15 minutes
)

submit_spark_job = SparkSubmitOperator(
    application="/mnt/apps/HelloWorld.py",  # Adjust the path
    task_id="submit_spark_job",
    conn_id="AIRFLOW_CONN_SPARK_DEFAULT",  # Adjust if needed
    verbose=True,
    dag=dag,
)
