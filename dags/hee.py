from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args = {
    'owner' : 'ndst',
    'retries' : 5,
    'retry_delay' : timedelta(minutes = 2)
}

with DAG(
    dag_id = "hee_v2",
    default_args = default_args,
    description = "heekuytat",
    start_date = datetime(2024, 11, 9, 3, 58),
    schedule_interval='@daily'
) as dag:
    task1 = BashOperator(
        task_id = '1',
        bash_command="echo hello world, fuck you heekuy"
    )

    task2 = BashOperator(
        task_id = "2",
        bash_command="echo i am task 2 that run after task 1"
    )

task1 >> task2