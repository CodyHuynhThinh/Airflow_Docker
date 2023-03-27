from __future__ import print_function

import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import pandas as pd

from sqlalchemy import create_engine, Table, MetaData, select, insert, delete
import psycopg2

import os


# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = '1lhMOvIhZMlcUhmlD1murK_m3AepzREZz8cJR5P7ldd4'
SAMPLE_RANGE_NAME = 'Task!A1:L'

def main():
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                '/app/credentials_web.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    try:
        service = build('sheets', 'v4', credentials=creds)

        # Call the Sheets API
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                    range=SAMPLE_RANGE_NAME).execute()
        values = result.get('values', [])
       

        if not values:
            print('No data found.')
            return

        # Add data to dataframe
        dic = {}

        for i in range(1, 12):
            lst = []
            for row in values:
                lst.append(row[i])
                dic[lst[0]] = lst[1:]
        
    except HttpError as err:
        print(err)
    return dic

# if __name__ == '__main__':
#     dic = main()

# # Turn dict into DataFrame
# df = pd.DataFrame.from_dict(dic)
# print(df.head())


# # Load into Postgre
# engine = create_engine("postgresql://postgres:123456@localhost") 
# #engine = create_engine("postgresql://fdobijxb:P4e-KqTZVBR6FhiME3GflCNzn4vIPVng@tiny.db.elephantsql.com:5432/fdobijxb")
# #engine = create_engine("postgresql://kitpdzdt:123456@tiny.db.elephantsql.com:5432/kitpdzdt")


# connection = engine.connect()
# metadata = MetaData()

# columns = ["job_type", "status"]
# tb = Table("tb_type", metadata, autoload=True, autoload_with=engine)
# #lst = df.to_dict("records")
# # stmt = insert(tb)
# # result_proxy = connection.execute(stmt, lst=columns)

# for index, row in df.iterrows():
#     stmt = insert(tb).values(job_type=row["Job Type"], status=row["Status"])
#     result_proxy = connection.execute(stmt)

#     #tmt = "INSERT INTO job_type (job_type) VALUES (%s)"
#     #val = row["Job Type"]

#     #result_proxy = connection.execute(stmt, val)

