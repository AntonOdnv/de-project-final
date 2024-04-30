import boto3
from io import BytesIO
import pandas as pd
import logging
import vertica_python
import pendulum

log = logging.getLogger(__name__)

class from_s3_to_vertica:
    
    def __init__(self, s3_client, bucket_name, vertica_str):
        self.s3_client = s3_client
        self.vertica_str = vertica_str
        self.bucket_name = bucket_name

    def filter_objects(self, table_relation, dt_treshold=None):
        objects = self.s3_client.list_objects_v2(Bucket=self.bucket_name)['Contents']
        # filtered_objects = [obj for obj in objects if ((obj['LastModified'].date() == dt_treshold.date())
        #                                                and (table_relation in str(obj['Key'])))]
        filtered_objects = [obj for obj in objects if (table_relation in str(obj['Key']))]
        log.info("Objects were filtered")
        return filtered_objects
       
        raise 
    def insert_to_vertica(self, str_date_treshold, table_name):
        dt_treshold = pendulum.parse(str_date_treshold, tz='UTC')
        table_relation = str(table_name).split('.')[1] 
        # Отбираем только те объекты, которые имеют нужную нам дату и принадлежность к таблице
        #filtered_object_list = self.filter_objects(dt_treshold, table_relation)
        filtered_object_list = self.filter_objects(table_relation)
        # Проход по каждому объекту в S3
        for obj in filtered_object_list:
            # Чтение данных из объекта S3
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=obj['Key'])
            log.info(F"Processing the file: {obj['Key']}")
            data = response['Body'].read()  # Предполагается, что данные закодированы в UTF-8
            df = pd.read_csv(BytesIO(data))
            log.info(F"Data from {obj['Key']} was written to df")
                 
            # Вставляем данные в Vertica
            with vertica_python.connect(**self.vertica_str) as conn:
                cur = conn.cursor()

                if "transactions" in table_relation:
                    log.info("Cleaning process for transactions type started")
                    operation_ids_str = ', '.join([f"'{id}'" for id in df['operation_id']])
                    status_ids_str = ', '.join([f"'{id}'" for id in df['status']])
                    # Для сохранения идемпотентности удаляем строки, которые есть в загружаемом файле
                    try:
                        cur.execute(f"""
                                    DELETE FROM {table_name} 
                                    WHERE transaction_dt between '{df['transaction_dt'].min()}' and '{df['transaction_dt'].max()}'
                                    AND operation_id IN ({operation_ids_str})
                                      AND status IN ({status_ids_str});
                                    """
                                   )
                        log.info("Cleaning process for transactions type completed")
                    except Exception as e:
                        log.error(f"An error occurred: {str(e)}")
                        
                if "currencies" in table_relation:
                    log.info("Cleaning process for currencies type started")
                    # Эту таблицу мы перезаписываем полностью
                    try:
                        cur.execute(f"""
                                    DELETE FROM {table_name};
                                    """
                                   )
                        log.info("Cleaning process for currencies type completed")
                    except Exception as e:
                        log.error(f"An error occurred: {str(e)}")
                        
                # Записываем прочитанные данные в Vertica через COPY
                log.info("Insert to Vertica process started")
                try:
                    cur.copy(f"""
                                COPY {table_name} 
                                FROM stdin 
                                DELIMITER ','
                                ENCLOSED BY '"'
                                REJECTED DATA AS TABLE {table_name}_rej;
                              """, 
                            df.to_csv(index=False, header=False), stdin=True
                            )
                    log.info("Insert to Vertica process completed")
                except Exception as e:
                    log.error(f"An error occurred: {str(e)}")
