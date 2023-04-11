from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
import pandas as pd
import boto3

def read_csv():
    s3_hook = S3Hook(aws_conn_id='minio_conn')
    bucket_name = 'airflow'
    key = 'data.csv'
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    df = pd.read_csv(obj['Body'])
    print(df.head())

default_args = {
    'owner': 'ThinhHuynh',
    #'retries': 5,
    'retry_delay': timedelta(minutes=10)
}

with DAG(
    dag_id='dag_with_minio_s3_v04',
    start_date=datetime(2023, 3, 22),
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False
) as dag:
    
    # task1 = S3KeySensor(
    #     task_id='sensor_minio_s3',
    #     bucket_name='airflow',
    #     bucket_key='data.csv',
    #     aws_conn_id='minio_conn'
    # )

   task2=PythonOperator(
        task_id='read_csv',
        python_callable=read_csv
    )
