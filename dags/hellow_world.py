from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def print_hello_world():
    print("Hello, World!")

default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hello_world_dag',
    default_args=default_args,
    description='A simple DAG that prints Hello, World!',
    schedule_interval=timedelta(minutes=10),  # Run every ten minutes
    concurrency=1,
    max_active_runs=1, 
)

hello_task = PythonOperator(
    task_id='print_hello_task',
    python_callable=print_hello_world,
    dag=dag,
)

if __name__ == "__main__":
    dag.cli()
