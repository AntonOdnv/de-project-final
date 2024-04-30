import pendulum
import logging
import vertica_python

from airflow.decorators import dag, task
from airflow.models.variable import Variable
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator

log = logging.getLogger(__name__)


@dag(
    schedule_interval='@daily',  
    start_date=pendulum.datetime(2022, 10, 1, tz="UTC"),  # Дата начала
    end_date=pendulum.datetime(2022, 10, 31, tz="UTC"), # Дата завершения
    catchup=True,  
    tags=['final_project'], 
    is_paused_upon_creation=True  # Сразу запущен.
)

def build_cdm():
    
    # Создаем подключение к Vertica.
    vertica_connection = BaseHook.get_connection("vertica_connection")
    # Получем путь до файла с запросом на создание/обновление витрины
    sql_file_path = Variable.get('sql_cdm_file_path')
    #sql_file_path = "/lessons/cdm_global_metrics.sql"

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
    
    # Перекладываем данные transactions в Vertica.
    @task()
    def global_metrics_processor(date, sql_file_path, vertica_str):
        f = open(f"{sql_file_path}", encoding='utf8')
        sql_query = f.read()
        sql_query = sql_query.format(dt = str(date))
        with vertica_python.connect(**vertica_str) as conn:
            cur = conn.cursor()
            # Для сохранения идемпотентности удаляем строки, которые есть в загружаемом файле
            try:
                cur.execute(sql_query)
                log.info("global_metrics cdm created saccessfully")
                cur.connection.commit()
                cur.close()  
            except Exception as e:
                log.error(f"An error occurred: {str(e)}")

    
    global_metrics_dm_task = global_metrics_processor('{{ ds }}', sql_file_path, vertica_str)

    # Формируем очередность исполнения задач
    global_metrics_dm_task

build_cdm_dag = build_cdm()