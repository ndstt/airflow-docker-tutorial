from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

def get_name(ti):
    ti.xcom_push(key = 'first_name', value = 'Jerry')
    ti.xcom_push(key = 'last_name', value = 'Godaryai')

def get_age(ti):
    ti.xcom_push(key = 'age', value = 2)

def hee(ti):
    first_name = ti.xcom_pull(task_ids = 'get_name', key = 'first_name')
    last_name = ti.xcom_pull(task_ids = 'get_name', key = 'last_name')
    age = ti.xcom_pull(task_ids = "get_age", key = "age")
    print(f"kuykuyukuykukyukyuk {first_name} {last_name} is {age} years old")

default_args = {
    'owner' : 'ndst',
    'retries' : 5,
    'retry_delay' : timedelta(minutes = 2)
}

with DAG(
    dag_id = "hee_python",
    default_args = default_args,
    description = "heekuytat",
    start_date = datetime(2024, 11, 8, 15, 45),
    schedule_interval = '@daily'
) as dag:
    task1 = PythonOperator(
        task_id = '1',
        python_callable = hee,
    )

    task2 = PythonOperator(
        task_id = 'get_name',
        python_callable = get_name
    )

    task3 = PythonOperator(
        task_id = "get_age",
        python_callable = get_age
    )

    [task2, task3] >> task1