import os
import glob

from datetime import datetime
import psycopg2
import pandas as pd
from datetime import date

#from sql_queries import *

import psycopg2
from psycopg2 import OperationalError

def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection

def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except OperationalError as e:
        print(f"The error '{e}' occurred")

def execute_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    

def update_load_id_table(connection):

    connection.autocommit = False
    select_query = "SELECT \"LoadID\" FROM \"ETL\".\"LoadControl\" where \"CurrentIndicator\" = True"
    actual_load_id=execute_read_query(connection, select_query)[0][0]+1
    today = date.today()

    update_query="UPDATE \"ETL\".\"LoadControl\" SET \"CurrentIndicator\" = False WHERE \"CurrentIndicator\" = True"
    execute_query(connection,  update_query)

    
    insert_query = f"INSERT INTO \"ETL\".\"LoadControl\" (\"LoadID\", \"LoadDate\", \"CurrentIndicator\") VALUES ({actual_load_id}, '{today}', True)"
    cursor = connection.cursor()
    cursor.execute(insert_query)
    connection.commit()


def get_files(folder_path):
    
    all_files=[]
    
    for root, dir, files in os.walk(folder_path):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files:
            all_files.append({os.path.abspath(f),datetime.now().strftime("%Y%m%d")})
    return all_files    