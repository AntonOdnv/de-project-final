import boto3
from io import BytesIO
import pandas as pd
import logging
import vertica_python
import pendulum

from airflow.decorators import dag, task
from airflow.models.variable import Variable
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator

from py import from_s3_to_vertica

log = logging.getLogger(__name__)

AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
endpoint_url = Variable.get("S3_ENDPOINT")
bucket_name = Variable.get("BUCKET")

@dag(
    schedule_interval=None,  
    start_date=pendulum.datetime(2022, 10, 1, tz="UTC"),  # Дата начала
    catchup=False,  
    tags=['final_project', 'stage_layer'], 
    is_paused_upon_creation=True  # Сразу запущен.
)

def create_vertica_stg():

    # Создем подключение к S3
    session = boto3.session.Session()
    s3_client = session.client(
        service_name = 's3',
        endpoint_url = endpoint_url,
        aws_access_key_id = AWS_ACCESS_KEY_ID,
        aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
    )
    
    # Создаем подключение к Vertica.
    vertica_connection = BaseHook.get_connection("vertica_connection")

    autocommit = "True"
    if "autocommit" in vertica_connection.extra_dejson:
        autocommit = vertica_connection.extra_dejson["autocommit"]

    vertica_str = { 'host': f'{vertica_connection.host}', 
                    'port': f'{vertica_connection.port}',
                    'user': f'{vertica_connection.login}',       
                    'password': f'{vertica_connection.password}',
                    'database': f'{vertica_connection.schema}',
                    'autocommit': f'{autocommit}'
                  }
    
    # Инициализируем объект класса
    try:
        s3 = from_s3_to_vertica(s3_client, bucket_name, vertica_str)
    except Exception as e:
        log.error(f"An error occurred: {str(e)}")
        raise 
    
    # Перекладываем данные transactions в Vertica.
    @task()
    def fill_transactions(s3, dt, table_name):
        s3.insert_to_vertica(dt, table_name)
    
    fill_transactions_task = fill_transactions(s3, '{{ ds }}', 'STV2024012236__STAGING.transactions')
    
    # Перекладываем данные transactions в Vertica.
    @task()
    def fill_currencies(s3, dt, table_name):
        s3.insert_to_vertica(dt, table_name)
    
    fill_currencies_task = fill_currencies(s3, '{{ ds }}', 'STV2024012236__STAGING.currencies')

    # Формируем очередность исполнения задач
    fill_currencies_task >> fill_transactions_task

create_vertica_stg_dag = create_vertica_stg()
