from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner' : 'ndst',
    'retries' : 5,
    'retry_delay' : timedelta(minutes = 2)
}

with DAG(
    dag_id = "postgres_op_v3",
    default_args = default_args,
    description = "postgres",
    start_date = datetime(2024, 11, 9),
    schedule_interval='@daily'

) as dag:
    task1 = PostgresOperator(
        task_id = 'create_postgres_table',
        postgres_conn_id = 'postgres_localhost',
        sql = """
            create table if not exists dag_run (
                dt date,
                dag_id character varying,
                primary key (dt, dag_id)
            )
        """
    )

    task2 = PostgresOperator(
        task_id = 'insert_into_table',
        postgres_conn_id = 'postgres_localhost',
        sql = """
            insert into dag_run (dt, dag_id) values ('{{ ds }}', '{{ dag.dag_id }}')
        """
    )

    task3 = PostgresOperator(
        task_id = 'delete_data_from_table',
        postgres_conn_id = 'postgres_localhost',
        sql = """
            delete from dag_run where dt = '{{ ds }}' and dag_id = '{{ dag.dag_id }}';
        """
    )

    task1 >> task3 >> task2