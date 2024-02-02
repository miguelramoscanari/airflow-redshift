from datetime import datetime, timedelta
import time
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import pandas as pd
import requests
from datetime import datetime, timedelta
import psycopg2
import psycopg2.extras as extras 
import os
from twilio.rest import Client

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
import utils as util

def etl(**context):
    # Load dataframe    
    df = util.load_api_dataframe()
    csv_filename = f"./{context['ds']}crypto.csv"
    df.to_csv(csv_filename, index = False)
    print('CSV ETL ',csv_filename)
    
    return csv_filename


def load_datawarehouse(**context):
    csv_filename = context['ti'].xcom_pull(task_ids='etl')
    print('CSV LOAD ', csv_filename)
    
    fecha = pd.to_datetime('today').strftime("%Y-%m-%d")
    
    conn = util.conectar_bd()
    # Load CSV en dataframe
    df = pd.read_csv(csv_filename)
    print(df.head(5))
    
    # CARGA INCREMENTAL

    # Eliminando registros de esa "fecha"
    query=f""" DELETE FROM cryptomoneda WHERE fecha = '{fecha}'; """
    util.runExec(conn, query)

    # Añadiendo el dataframe a la tabla de la BD
    util.runExecMany(conn, df, 'cryptomoneda')    
    
    return 'Carga incremental exitoso'

def send_message(**context):
    csv_filename = context['ti'].xcom_pull(task_ids='etl')
    print('CSV LOAD ', csv_filename)
    
    # Load CSV en dataframe
    df = pd.read_csv(csv_filename)
    df = df.head(5)
    
    account_sid= Variable.get('TWILIO_ACCOUNT_SID')
    auth_token= Variable.get('TWILIO_AUTH_TOKEN')
    phone_number = Variable.get('PHONE_NUMBER')

    client = Client(account_sid, auth_token)

    message = client.messages \
                    .create(
                        body='\nHola! Resumen del cryptomoneas: \n\n\n ' + str(df),
                        from_=phone_number,
                        to='+51952351404'
                    )
    return message.sid


DAG = DAG(
  dag_id='DAG_Criptomoneda',
  start_date=datetime.now(),
  schedule_interval='@once'
)

etl = PythonOperator(
    task_id='etl', 
    python_callable=etl,
    provide_context=True,
    dag=DAG)

load_datawarehouse = PythonOperator(
    task_id='load_datawarehouse', 
    python_callable=load_datawarehouse,
    provide_context=True,
    dag=DAG)

send_message = PythonOperator(
    task_id='send_message', 
    python_callable=send_message,
    provide_context=True,
    dag=DAG)

etl >> load_datawarehouse
etl >> send_message