from __future__ import print_function

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account

import pandas as pd

from sqlalchemy import create_engine, Table, MetaData, select, insert, delete
import psycopg2

import os
import json
import numpy as np

# The ID and range of a sample spreadsheet.
SPREADSHEET_ID = '1qKy8Q84RbJZo7_QHRIudXkgqgLolzbNvgFheUf06wbg'
key_path = '/opt/airflow/data/credentials_service.json'
service_account_info = json.load(open(key_path))
creds = service_account.Credentials.from_service_account_info(service_account_info)

#engine = create_engine("postgresql://kitpdzdt:123456@tiny.db.elephantsql.com:5432/kitpdzdt")
engine = create_engine("postgresql://postgres:123456@host.docker.internal") 

def google_sheet_conn():
    pass

def send_mail():
    pass

def gg_conn(rang_name, col_num):
    try:
        service = build('sheets', 'v4', credentials=creds)
        
        # Call the Sheets API
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=rang_name).execute()
        values = result.get('values', [])
       
        if not values:
            print('No data found.')
            return
        
    except HttpError as err:
        print(err)
    return values

def insert_master():
    try:
        lst = gg_conn('Master Data!A1:H', 8)
        dic_master = pd.DataFrame(lst[1:], columns=lst[0])
        df_master = pd.DataFrame.from_dict(dic_master)
        df_master = df_master.replace(to_replace=[''], value=np.nan)
        df_master = df_master.dropna(subset=["Key"])
        connection = engine.connect()
        metadata = MetaData()

        tb = Table("tb_master", metadata, autoload=True, autoload_with=engine)

        for index, row in df_master.iterrows():
            stmt = insert(tb).values(ticket_id=row["Key"]
                                    , assignee=row["Assignee"]
                                    , issue_type=row["Issue Type"]
                                    , summary=row["Summary"]
                                    , status=row["Status"]
                                    , sprint=["Sprint"]
                                    , est_story_point=row["Est. Story Points"]
                                    , delayed_task_flag=row["Delayed Task Flag"]
                                    )
            result_proxy = connection.execute(stmt)
    except HttpError as err:
        print(err)

def insert_sprint():
    try:
        lst = gg_conn('Sprint Data!A1:D', 4)
        dic_sprint=pd.DataFrame(lst[1:], columns=lst[0])
        df_sprint = pd.DataFrame.from_dict(dic_sprint)
        df_sprint = df_sprint.replace(to_replace=[None, 'None', '|'], value=np.nan)
        #df_sprint = df_sprint.dropna(subset=['S'])
        df_sprint.rename(columns={'S':'Key'}, inplace=True)

        connection = engine.connect()
        metadata = MetaData()

        tb = Table("tb_sprint", metadata, autoload=True, autoload_with=engine)

        for index, row in df_sprint.iterrows():
            stmt = insert(tb).values(ticket_id=row["Key"]
                                    , sprint=row["Sprint"]
                                    , pi=row["PI"]
                                    , iteration=row["Iteration"]
                                    )
            result_proxy = connection.execute(stmt)
    except HttpError as err:
        print(err)

def insert_issue():
    try:
        lst = gg_conn('Issue Data!A1:B', 2)
        dic_issue=pd.DataFrame(lst[1:], columns=lst[0])
        df_issue = pd.DataFrame.from_dict(dic_issue)
        df_issue.rename(columns={"Log Work.comment":"Issue"}, inplace=True)

        connection = engine.connect()
        metadata = MetaData()

        tb = Table("tb_issue", metadata, autoload=True, autoload_with=engine)

        for index, row in df_issue.iterrows():
            stmt = insert(tb).values(ticket_id=row["Key"]
                                    , issue=row["Issue"]
                                    )
            result_proxy = connection.execute(stmt)
    except HttpError as err:
        print(err)