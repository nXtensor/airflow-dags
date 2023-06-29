import datetime as dt
import pendulum
import os
import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator


@dag(
    dag_id="coding_challenge_data_dag",
    schedule_interval="0 0 * * *",  # run at midnight every day
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=dt.timedelta(minutes=60))
def process_employees():
    create_tables = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="postgres_dev",
        sql="""
            CREATE TABLE IF NOT EXISTS continent_map (
                "id" SERIAL PRIMARY KEY,
                "country_code" TEXT,
                "continent_code" TEXT
            );
            CREATE TABLE IF NOT EXISTS continents (
               "id" SERIAL PRIMARY KEY,
                "continent_code" TEXT,
                "continent_name" TEXT
            );
            CREATE TABLE IF NOT EXISTS countries (
                "id" SERIAL PRIMARY KEY,
                "country_code" TEXT,
                "country_name" TEXT
            );
            CREATE TABLE IF NOT EXISTS per_capita (
                "id" SERIAL PRIMARY KEY,
                "country_code" TEXT,
                "year" TEXT,
                "gdp_per_capita" TEXT
            );
        """,)
    create_tables.doc_md = """\Create Tables from 
    https://github.com/AlexanderConnelly/BrainTree_SQL_Coding_Challenge_Data_Analyst/tree/master
    """

    @task()
    def get_data():
        files_name = ["continent_map.csv", "continents.csv",
                      "countries.csv", "per_capita.csv"]
        for file_name in files_name:
            data_path = f"/opt/airflow/data/{file_name}"
            os.makedirs(os.path.dirname(data_path), exist_ok=True)
            url = f"https://raw.githubusercontent.com/AlexanderConnelly/BrainTree_SQL_Coding_Challenge_Data_Analyst/master/data_csv/{file_name}"
            response = requests.get(url)
            with open(data_path, "w") as f:
                f.write(response.text)

            postgres_hook = PostgresHook(postgres_conn_id="postgres_dev")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            with open(data_path, "r") as f:
                cur.copy_expert(
                    f"COPY {file_name.split['.'][0]} FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'", f,
                )
            conn.commit()
            cur.close()
            conn.close()

    create_tables >> get_data()


dag = process_employees()
