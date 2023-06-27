from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
with DAG(
    "postgres_dag",
    description="Postgres DAG",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
    default_args={"owner": "airflow",
                  "email": ["n.tensor@gmail.com"],
                  "email_on_failure": True,
                  },
) as dag:
    t1 = BashOperator(
        task_id="print_date",
        bash_command="date",
    )
    t2 = PostgresOperator(
        task_id="list_tables",
        postgres_conn_id="postgres",
        sql="SELECT * FROM pg_catalog.pg_tables;",  # list all tables
        xcom_push=True,
    )

    t1 >> t2
