from datetime import datetime, timedelta
from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore

default_args = {
    'owner' : 'ndst',
    'retries' : 5,
    'retry_delay' : timedelta(minutes = 2)
}

def get_sklearn():
    import sklearn # type: ignore
    print(f"scikit-learn with version :{sklearn.__version__}")

with DAG(
    dag_id = "dag_python_dependencies",
    default_args = default_args,
    description = "depend",
    start_date = datetime(2024, 11, 8),
    schedule_interval = '@daily'
) as dag:
    task1 = PythonOperator(
        task_id = 'get_sklearn',
        python_callable= get_sklearn
    )

    task1