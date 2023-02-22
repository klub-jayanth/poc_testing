from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task

# Define the DAG
dag = DAG(
    'POC_Hello_World',
    description='DAG that prompts the user for input and executes different tasks based on the input',
    schedule_interval=None,
    start_date=datetime(2023, 2, 22),
    catchup=False
)

# Define a function that prompts the user for input
def hello_world():
    print("Hello World")

# Define a PythonOperator that runs the prompt_user function and sets the output to xcom
user_input_task = PythonOperator(
    task_id='user_input_task',
    python_callable=hello_world,
    dag=dag
)

# Define a BashOperator that prints a message if the user input is 'yes'
print_yes_task = BashOperator(
    task_id='print_yes_task',
    bash_command='echo "You entered yes"',
    dag=dag
)

# Define a BashOperator that prints a message if the user input is not 'yes'
print_no_task = BashOperator(
    task_id='print_no_task',
    bash_command='echo "You did not enter yes"',
    dag=dag
)

@task(dag=dag)
def increment(x: int):
    return x + 1
@task(dag=dag)
def total(values):
    total = sum(values)
    print(f"Total was {total}")


start = EmptyOperator(task_id="start")
end = EmptyOperator(task_id="end", trigger_rule="none_failed")

added_values = increment.expand(x=[1, 2, 3])
total(added_values)

start >> user_input_task >> print_yes_task >> print_no_task >> end