from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator 

default_args = {
    'owner': 'ThinhHuynh',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def greet(ti):
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    age = ti.xcom_pull(task_ids='get_age', key='age')

    print(f"Hello world! My name is {first_name} {last_name}, "
          f"and I am {age} years old!")
    
def get_name(ti):
    ti.xcom_push(key='first_name', value='Thinh')
    ti.xcom_push(key='last_name', value='Huynh')

def get_age(ti):
    ti.xcom_push(key='age', value='22')

with DAG(
    dag_id='our_dag_with_python_operator_v05',
    default_args=default_args,
    description='Our first dag using python operator',
    start_date=datetime(2023, 3, 17),
    schedule_interval='@daily'
) as dag:
    Task1 = PythonOperator(
        task_id = 'greet',
        python_callable=greet
    )

    Task2 = PythonOperator(
        task_id = 'get_name',
        python_callable=get_name
    )

    Task3 = PythonOperator(
        task_id = 'get_age',
        python_callable=get_age
    )

    [Task3, Task2] >> Task1