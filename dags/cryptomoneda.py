#miguel ramos 22

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
import utils as util

email = Variable.get("_EMAIL")

default_arguments = {   'owner': 'MiguelRamos',
                        'email': email,
                        'retries':1 ,
                        'retry_delay':timedelta(minutes=5)}

with DAG('DAG_Criptomoneda',
         default_args=default_arguments,
         description='Cryptomoneda' ,
         start_date = datetime(2022, 9, 21),
         schedule_interval = None,
         tags=['crypto'],
         catchup=False) as dag :
    
    # Definir la tarea
    etl = PythonOperator(
        task_id='elt',
        python_callable=util.etl,
        provide_context=True,
        retries=1,
        retry_delay=timedelta(seconds=15))

    # Definir la tarea
    load_datawarehouse = PythonOperator(
        task_id='load_datawarehouse',
        python_callable=util.load_datawarehouse,
        provide_context=True,
        retries=1,
        retry_delay=timedelta(seconds=15))
    

    # Definir dependencias entre tareas
    etl >> load_datawarehouse