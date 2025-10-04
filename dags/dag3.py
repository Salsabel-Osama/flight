from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def print_nums ():
    for i in range (1, 10):
        print (i)

with DAG (
    dag_id="dag3",
    start_date=datetime(2025, 9, 30),
    schedule_interval=timedelta(minutes=1),
    catchup=False
) as dag:
    test_hello = BashOperator(
        task_id="test_hello",
        bash_command="echo 'Mohamed Hamed'"
    )

    task_pwd = PythonOperator(
        task_id="task_pwd",
        python_callable= print_nums
    )

test_hello >> task_pwd  