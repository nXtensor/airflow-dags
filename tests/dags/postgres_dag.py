from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
with DAG(
    "basic",
    description="Postgres DAG",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
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
    )

    t1 >> t2
