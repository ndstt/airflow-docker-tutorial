from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

default_args = {
    'owner' : 'ndst',
    'retries' : 5,
    'retry_delay' : timedelta(minutes = 2)
}

with DAG(
    dag_id = "minio_v2",
    default_args = default_args,
    description = "minio",
    start_date = datetime(2022, 5, 20),
    schedule_interval='@daily'
) as dag:
    task1 = S3KeySensor(
    task_id="S3KeySensor",
    bucket_name='airflow',
    bucket_key="orders/{{ ds_nodash }}.txt",
    aws_conn_id='minio_conn',
    poke_interval=30,  # Checks every 30 seconds
    timeout=3600       # Waits up to 1 hour
)