from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG (
    dag_id="dag2",
    start_date=datetime(2025, 9, 30),
    schedule_interval=timedelta(minutes=1),
    catchup=False
) as dag:
    test_hello = BashOperator(
        task_id="test_hello",
        bash_command="echo 'Mohamed Hamed'"
    )

    task_pwd = BashOperator(
        task_id="task_pwd",
        bash_command="pwd"
    )

test_hello >> task_pwd  