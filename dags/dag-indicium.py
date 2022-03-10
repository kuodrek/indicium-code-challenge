from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from pendulum import today

default_args = {
    "owner": "Luis Kuodrek",
    "depends_on_past": False,
    "start_date": datetime(2022, 3, 10),
    "schedule_interval": "@daily",
    "retries": 0,
    "retries_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(seconds=60),
}

with DAG(
    "dag-indicium",
    default_args=default_args,
    tags=['indicium'],
    description="Data pipeline for the coding challenge brought by Indicium"
) as dag:

    t1 = BashOperator(
        task_id="extract_northwind_db",
        bash_command="""
   cd $AIRFLOW_HOME/dags/tasks/
   python3 extract_northwind_db.py {{ execution_date }}
   """,
    )

    t2 = BashOperator(
        task_id="extract_csv",
        bash_command="""
   cd $AIRFLOW_HOME/dags/tasks/
   python3 extract_csv.py {{ execution_date }}
   """,
    )

    t3 = BashOperator(
        task_id="build_output_db",
        bash_command="""
   cd $AIRFLOW_HOME/dags/tasks/
   python3 build_output_db.py {{ execution_date }}
   """,
    )

    t4 = BashOperator(
        task_id="output_query",
        bash_command="""
   cd $AIRFLOW_HOME/dags/tasks/
   python3 output_query.py {{ execution_date }}
   """,
    )


[t1, t2] >> t3 >> t4
