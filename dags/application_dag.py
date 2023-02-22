from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task
import requests
from airflow.operators.python import BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import time

# Define the DAG
dag = DAG(
    'POC_Feasibility',
    description='DAG that prompts the user for input and executes different tasks based on the input',
    schedule_interval=None,
    start_date=datetime(2023, 2, 22),
    catchup=False
)

start = EmptyOperator(task_id="start")
end = EmptyOperator(task_id="end")

def hello_world():
    print("Hello World")

hello_world_task = PythonOperator(
    task_id='hello_world',
    python_callable=hello_world,
    dag=dag,
)

def http_invoke():
    x = requests.get("https://ifsc.razorpay.com/HDFC0CAGSBK")
    print(x.status_code)

API_INVOKE = PythonOperator(
    task_id='API_Invoke',
    python_callable=hello_world,
    dag=dag,
)

def poll_user():
    # counter = 0
    # while True:
    #     response = requests.get('http://0.0.0.0:5000/monitor')
    #     print(response.status_code)
    #     if response.status_code == 200:
    #         counter += 1
    #         if response.json():
    #             return 'success_task'
    #     if counter == 5:
    #         return 'failure_task'
    #     print('Sleeping')
    #     time.sleep(5)
    time.sleep(5)
    response = requests.get('https://httpbin.org/get')
    # if response.status_code == 200:
    #     return 'Success_Task'
    # else:
    return 'Failure_Task'



user_approval = BranchPythonOperator(
    task_id='User_Approval_Task',
    python_callable=poll_user
)

def success_process():
    print("Success Task")

success_task = PythonOperator(
    task_id='Success_Task',
    python_callable=success_process,
    dag=dag,
)

def failure_process():
    print("Failure Task")

failure_task = PythonOperator(
    task_id='Failure_Task',
    python_callable=failure_process,
    dag=dag,
)

POC_DAG_Trigger = TriggerDagRunOperator(
    task_id='Trigger_Another_Dag',
    trigger_dag_id='POC_Hello_World'
)

start >> hello_world_task >> API_INVOKE >> user_approval >> success_task >> end
start >> hello_world_task >> API_INVOKE >> user_approval >> failure_task >> POC_DAG_Trigger >> end