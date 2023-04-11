from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.email import EmailOperator
import google_sheet

POSTGRES_CONN_LOCAL = 'postgres_conn_local'

default_args = {
    'owner': 'ThinhHuynh',
    #'retries': 5,
    'email': ['huynhdangthinh@gmail.com'], 
    'email_on_failure': False, 
    'email_on_retry': False, 
    'retry_delay': timedelta(minutes=10)
}

with DAG(
    dag_id='dag_read_google_sheet_v04',
    start_date=datetime(2023, 3, 25),
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False
) as dag:
    task_googlesheet_conn=PythonOperator(
        task_id='google_sheet_connection',
        python_callable=google_sheet.google_sheet_conn
    )

    task_insert_master=PythonOperator(
        task_id='insert_master',
        python_callable=google_sheet.insert_master
    )

    task_insert_master_his=PostgresOperator(
        task_id='insert_master_his',
        postgres_conn_id='postgres_conn_local',
        sql="""
            INSERT INTO tb_master_his
            SELECT *
            FROM tb_master
        """
    )

    task_del_master=PostgresOperator(
        task_id='del_master',
        postgres_conn_id='postgres_conn_local',
        sql="""
            DELETE
            FROM tb_master
        """
    )

    task_insert_sprint=PythonOperator(
        task_id='insert_sprint',
        python_callable=google_sheet.insert_sprint
    )

    task_insert_sprint_his=PostgresOperator(
        task_id='insert_sprint_his',
        postgres_conn_id='postgres_conn_local',
        sql="""
            INSERT INTO tb_sprint_his
            SELECT *
            FROM tb_sprint
        """
    )

    task_del_sprint=PostgresOperator(
        task_id='del_sprint',
        postgres_conn_id='postgres_conn_local',
        sql="""
            DELETE
            FROM tb_sprint
        """
    )

    task_insert_issue=PythonOperator(
        task_id='insert_issue',
        python_callable=google_sheet.insert_issue
    )

    task_insert_issue_his=PostgresOperator(
        task_id='insert_issue_his',
        postgres_conn_id='postgres_conn_local',
        sql="""
            INSERT INTO tb_issue_his
            SELECT *
            FROM tb_issue
        """
    )

    task_del_issue=PostgresOperator(
        task_id='del_issue',
        postgres_conn_id='postgres_conn_local',
        sql="""
            DELETE
            FROM tb_issue
        """
    )

    task_send_mail=EmailOperator( 
                    task_id='send_email', 
                    to='thinh.huynh.tpv@one-line.com', 
                    subject='Raw data from google sheet', 
                    html_content="""<h1>Hello Looker team</h1>""" 
                )

    task_googlesheet_conn >> task_insert_master_his >> task_del_master >> task_insert_master >> task_send_mail

    task_googlesheet_conn >> task_insert_sprint_his >> task_del_sprint >> task_insert_sprint >> task_send_mail

    task_googlesheet_conn >> task_insert_issue_his >> task_del_issue >> task_insert_issue >> task_send_mail