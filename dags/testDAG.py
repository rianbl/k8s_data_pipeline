from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta, datetime

# Define the default_args dictionary to specify the default parameters of the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG
dag = DAG(
    'kubectl_execution_dag',
    default_args=default_args,
    description='DAG to execute kubectl get pod command every 15 minutes',
    schedule_interval='*/15 * * * *',  # Set the schedule interval to 15 minutes
)

# Define the kubectl get pod command to be executed
kubectl_command = "kubectl get pod"

# Use BashOperator to execute the kubectl command and echo its stdout
kubectl_operator = BashOperator(
    task_id='execute_kubectl_get_pod',
    bash_command=f'{kubectl_command} && echo "Command executed successfully"',
    dag=dag,
)

# Set the task dependency
kubectl_operator

# You can add more tasks or operators as needed for your workflow

if __name__ == "__main__":
    dag.cli()
