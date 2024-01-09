from datetime import datetime, timedelta
from airflow import DAG
from kubernetes.client import V1Volume, V1VolumeMount
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
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

# spark_task = SparkKubernetesOperator(
#     task_id='run_spark_job',
#     namespace='default',
#     application_file='/mnt/host/apps/HelloWorld.yaml',  # or pass a dictionary
#     kubernetes_conn_id='my_k8s_id',  # specify your Kubernetes connection ID
#     dag=dag,
# )

### Define Spark job task using KubernetesPodOperator
spark_job_task = KubernetesPodOperator(
    task_id='run_spark_job',
    name='spark-job-task',
    namespace='default',
    image='bitnami/spark:3.5.0-debian-11-r16',
    cmds=[
        '/opt/bitnami/spark/bin/spark-submit',
        '--conf', 'spark.jars.ivy=/tmp/.ivy',
        '--master', 'spark://spark-master-svc:7077',
        '--name', 'helloWorld',
        '/mnt/scripts/HelloWorld.py'
    ],
    volume_mounts=[
        V1VolumeMount(
            name='scripts-volume',
            mount_path='/mnt/scripts'
        )
    ],
    volumes=[
        V1Volume(
            name='scripts-volume',
            config_map=V1ConfigMapVolumeSource(
                name='my-scripts'
            )
        )
    ],
    dag=dag,
)
