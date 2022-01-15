from class_module.dag_factory import DagFactory
from class_module.task import Task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable
import datetime as dt
from class_module.scraper import Scraper
from class_module.operators import CustomMySqlOperator
import pandas as pd
from typing import List

def parse_data():
    config = Variable.get("config", deserialize_json=True)
    filter = {'area':config['area'],
              'type':config['type'],
              'max_price':config['max_price'],
              'min_year_built':config['min_year_built']}
    headers = {'User-Agent':
                'Mozilla/5.0 (Windows NT 6.1) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/41.0.2228.0 Safari/537.36'}
    scraper = Scraper(headers=headers, filter=filter)
    return scraper.get_content()

def store_data(**kwargs):
    ti = kwargs['ti']
    data: List[tuple] = ti.xcom_pull(key=None, task_ids='parse_data')
    parsedDt = kwargs.get('execution_date')
    #logging.info('paredDt: {}'.format(parsedDt))
    new_data = []
    for row in data:
        new_data.append(row + (parsedDt,))

    connection = MySqlHook(mysql_conn_id='mysql_propertydb')
    conn = connection.get_conn()
    conn.autocommit = True
    cursor = conn.cursor()
    sql = "INSERT INTO property(price,location,size,parsedDt) VALUES(%s,%s,%s,%s)"
    cursor.executemany(sql, new_data)
    conn.commit()
    return 'store_data'

tasks = []
tasks.append(Task(type='python', func=parse_data, dependencies=[]))
tasks.append(Task(type='python', func=store_data, dependencies=[parse_data]))

DAG_NAME = 'load_property_data_w_factory'

override_args = {
    'owner': 'Awesome Data Engineer',
    'retries': 2
}

dag = DagFactory().get_airflow_dag(DAG_NAME, tasks, default_args=override_args, cron='@daily')