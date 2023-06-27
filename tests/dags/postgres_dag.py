import datetime as dt
import pendulum
import os
import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator


@dag(
    dag_id="process_employees_dag",
    schedule_interval="0 0 * * *",  # run at midnight every day
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=dt.timedelta(minutes=60))
def process_employees():
    create_employee_table = PostgresOperator(
        task_id="create_employee_table",
        postgres_conn_id="postgres_dev",
        sql="""
            CREATE TABLE IF NOT EXISTS employees (
                "Serial Number" NUMERIC PRIMARY KEY,
                "Company Name" TEXT,
                "Employee Markme" TEXT,
                "Description" TEXT,
                "Leave" INTEGER
            );
        """,)
    create_employee_table.doc_md = """\Emplyee Table"""
    create_employee_temp_table = PostgresOperator(
        task_id="create_employee_temp_table",
        postgres_conn_id="postgres_dev",
        sql="""
            DROP TABLE IF EXISTS employees_temp;
            CREATE TABLE IF NOT EXISTS employees_temp (
                "Serial Number" NUMERIC PRIMARY KEY,
                "Company Name" TEXT,
                "Employee Markme" TEXT,
                "Description" TEXT,
                "Leave" INTEGER
            );
        """,)
    create_employee_temp_table.doc_md = """\Emplyee Temp Table to transform data before inserting into main table"""

    @task()
    def get_data():
        data_path = "/opt/airflow/data/employees.csv"
        os.makedirs(os.path.dirname(data_path), exist_ok=True)
        url = "https://raw.githubusercontent.com/apache/airflow/main/docs/apache-airflow/tutorial/pipeline_example.csv"
        response = requests.get(url)
        with open(data_path, "w") as f:
            f.write(response.text)

        postgres_hook = PostgresHook(postgres_conn_id="postgres_dev")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        with open(data_path, "r") as f:
            cur.copy_expert(
                "COPY employees_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'", f,
            )
        conn.commit()
        cur.close()
        conn.close()

    @task()
    def merge_data():
        query = """
            INSERT INTO employees
            SELECT * FROM(SELECT DISTINCT * FROM employees_temp) t
            ON CONFLICT ("Serial Number") DO UPDATE 
            SET "Serial Number" = EXCLUDED."Serial Number";
        """
        try:
            postgres_hook = PostgresHook(postgres_conn_id="postgres_dev")
            postgres_hook.run(query)
        except Exception as e:
            print(e)
            raise Exception("Error while inserting data into table")
            return 1

    [create_employee_table, create_employee_temp_table] >> get_data() >> merge_data()


dag = process_employees()
