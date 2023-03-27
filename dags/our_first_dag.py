from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'ThinhHuynh',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id = "our_first_dag_v1",
    default_args = default_args,
    description = "This is our first dag that we write",
    start_date = datetime(2023, 3, 17, 2),
    schedule_interval = '@daily'
) as dag:
    Task1 = BashOperator(
        task_id = 'first_task',
        bash_command='echo Hello world, this is task 1'
    )

    Task2 = BashOperator(
        task_id = 'second_task',
        bash_command='echo Hello world, this is task 2'
    )
    
    Task3 = BashOperator(
        task_id = 'third_task',
        bash_command='echo Hello world, this is task 3'
    )


    Task1 >> [Task2, Task3]