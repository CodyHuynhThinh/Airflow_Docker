from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import psycopg2

import google_sheet

hostname='localhost'
database='postgres'
username='postgres'
pwd='123456'
portID=5432
conn=None
cur=None

def db_conn():
    try:
        conn = psycopg2.connect(
            host=hostname,
            dbname=database,
            user=username,
            password=pwd,
            port=portID
        )

        cur = conn.cursor()

        conn.commit()
    except Exception as error:
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

def insert_master():
    # Add data to dataframe
    dic = {}
    dic = google_sheet.main()
    df = pd.DataFrame.from_dict(dic)
    print(df.head())



default_args = {
    'owner': 'ThinhHuynh',
    #'retries': 5,
    'retry_delay': timedelta(minutes=10)
}

with DAG(
    dag_id='dag_with_google_sheet_v01',
    start_date=datetime(2023, 3, 25),
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False
) as dag:
    task1=PythonOperator(
        task_id='get_data_from_google_sheet',
        python_callable=insert_master
    )

    task1