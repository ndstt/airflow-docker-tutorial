from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import csv
import logging

def postgres_to_s3(ds_nodash, next_ds_nodash):
    # step 1: query data from postgressql db and save into text file
    hook = PostgresHook(postgres_conn_id = "postgres_localhost")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("select * from orders where date >= %s and date < %s",
                   (ds_nodash, next_ds_nodash))
    with open(f"dags/get_orders_{ds_nodash}.txt", "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)
    cursor.close()
    conn.close()
    logging.info("Saved orders data in text file: %s",f"dags/get_orders_{ds_nodash}.txt")
    # step 2: upload text file into s3
    s3_hook = S3Hook(aws_conn_id = "minio_conn")
    s3_hook.load_file(
        filename = f"dags/get_orders_{ds_nodash}.txt",
        key = f"orders/{ds_nodash}.txt",
        bucket_name = "airflow",
        replace = True
    )

default_args = {
    'owner' : 'ndst',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=2)
}

with DAG(
    dag_id = "postgres_hook_v11",
    default_args = default_args,
    start_date = datetime(2022, 5, 20),
    schedule_interval = '@daily'
) as dag:
    task1 = PythonOperator(
    task_id="postgres_to_s3",
    python_callable=postgres_to_s3,
    op_kwargs={
        'ds_nodash': '{{ data_interval_start | ds_nodash }}',
        'next_ds_nodash': '{{ data_interval_end | ds_nodash }}'
    }
    )
    task1