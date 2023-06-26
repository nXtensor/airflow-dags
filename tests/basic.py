from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
with DAG(
    "basic",
    description="Basic DAG",
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
    t2 = BashOperator(
        task_id="sleep",
        bash_command="sleep 5",
    )
    t3 = PythonOperator(
        task_id="print_hello",
        python_callable=lambda: print("hello"),
    )
    t1.doc_md = dedent(
        """\
        #### Task Documentation
        You can document your task using the attributes `doc_md` (markdown),
        `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
        rendered in the UI's Task Instance Details page.
        ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
         **Image Credit:** Randall Munroe, [XKCD](https://xkcd.com/license.html)
        """
    )

    dag.doc_md = __doc__
    templated_command = dedent(
        """
        {% for i in range(5)%}
            echo "{{ ds }}"
            echo "{{ macros.ds_add(ds, 7)}}"
        {% endfor %}
        """)

    t1 >> [t2, t3]
