from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args = {
    'owner' : 'ndst',
    'retries' : 5,
    'retry_delay' : timedelta(minutes = 2)
}

with DAG(
    dag_id = "catchup_and_backfill",
    default_args = default_args,
    description = "heekuytat",
    start_date = datetime(2024, 11, 9),
    schedule_interval='@daily',
    catchup = False

) as dag:
    task1 = BashOperator(
        task_id = '1',
        bash_command="echo bashbashabsh"
    )