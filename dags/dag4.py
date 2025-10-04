from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta
import sqlite3


output_file = '/opt/airflow/shared/tasks_data.txt'
def get_student_tasks ():
    conn = sqlite3.connect('/opt/airflow/shared/alex_student.db')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM tasks")
    tasks = cursor.fetchall()
    
    with open(output_file, 'w') as f:
        for data in tasks:
            f.write(str(data) + '\n')

    cursor.close()    
    conn.close()
  
with DAG (
    dag_id="dag4",
    start_date=datetime(2025, 9, 30),
    schedule_interval=timedelta(minutes=10),
    catchup=False
) as dag:
    task_pwd = PythonOperator(
        task_id="task_tasks_data",
        python_callable= get_student_tasks
    )

    task_mail = EmailOperator(
        task_id="task_mail",
        to = "abdullah.elmogy@gmail.com",
        subject = "Tasks Data Alex 4",
        html_content ='<h3> This is data about the task </h3>',
        files = [output_file], 
    )

task_pwd >> task_mail