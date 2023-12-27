from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# Define default_args and DAG configuration
default_args = {
    'owner': 'your_username',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spark_job_dag',
    default_args=default_args,
    description='DAG to trigger Spark job',
    schedule_interval='*/15 * * * *',  # Run every 15 minutes
)

# Define Spark job task
spark_job_task = BashOperator(
    task_id='run_spark_job',
    bash_command='spark-submit --master spark://spark-master-svc:7077 your_spark_job.py',  # Adjust as needed
    dag=dag,
)

# Save your DAG file to the DAGs folder in your Airflow installation
# /path/to/your/airflow/dags/spark_job_dag.py
