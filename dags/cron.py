from airflow import DAG # type: ignore
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator # type: ignore

default_args = {
    'owner' : 'ndst',
    'retries' : 5,
    'retry_delay' : timedelta(minutes = 2)
}

with DAG(
    dag_id = "cron3",
    default_args = default_args,
    description = "heekuytat",
    start_date = datetime(2024, 10, 20),
    schedule_interval='0 3 * * Tue-Fri'

) as dag:
    task1 = BashOperator(
        task_id = 'task1',
        bash_command="echo croncroncroncroncroncroncron"
    )
    task1