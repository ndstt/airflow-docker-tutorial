from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner' : 'ndst',
    'retries' : 5,
    'retry_delay' : timedelta(minutes = 2)
}

@dag(dag_id = 'taskflow_api',
     default_args = default_args,
     start_date = datetime(2024, 11, 9),
     schedule_interval = '@daily')
def hello_world_etl():
    @task()
    def get_name():
        return "Jerry"

    @task()
    def get_age():
        return 19

    @task()
    def greet(name, age):
        print(f"kuykukyukyuk {name} is {age} years old")

    name = get_name()
    age = get_age()
    greet(name = name, age = age)

greet_dag = hello_world_etl()