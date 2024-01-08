from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
import os

# Define default_args and DAG configuration
default_args = {
    'owner': 'your_username',
    'start_date': datetime(2022, 1, 1),  # Set to a past date
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spark_job_dag',
    default_args=default_args,
    description='DAG to trigger Spark job',
    schedule_interval='*/15 * * * *',  # Run every 15 minutes
    catchup=False,  # Do not backfill for past intervals
)

# Print the current working directory
list_files_task = BashOperator(
    task_id='list_files_on_spark_master',
    bash_command='kubectl exec spark-master-0 -- ls /opt/bitnami/spark/apps',
    dag=dag,
)

# Define Spark job task using KubernetesPodOperator
# spark_job_task = KubernetesPodOperator(
#     task_id='run_spark_job',
#     name='spark-job-task',
#     namespace='default',  # Set your Kubernetes namespace
#     image='bitnami/spark:3.5.0-debian-11-r16',  # Set the Spark image
    # cmds=[
    #     '/opt/bitnami/spark/bin/spark-submit',
    #     '--conf', 'spark.jars.ivy=/tmp/.ivy',  # Set Ivy directory
    #     '--master', 'spark://spark-master-svc:7077',
    #     '--name', 'helloWorld',
    #     '/opt/bitnami/spark/apps/HelloWorld.py'
    # ],
#     dag=dag,
# )
