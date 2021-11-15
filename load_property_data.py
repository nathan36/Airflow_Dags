from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable
import datetime as dt
from class_module.scraper import Scrper
from class_module.operators import CustomMySqlOperator
from pandas import DataFrame

def parse_data() -> DataFrame:
    config = Variable.get("config", deserialize_json=True)
    filter = {'area':config['area'],
              'type':config['type'],
              'max_price':config['max_price'],
              'min_year_built':config['min_year_built']}
    headers = {'User-Agent':
                'Mozilla/5.0 (Windows NT 6.1) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/41.0.2228.0 Safari/537.36'}
    scraper = Scrper(headers=headers, filter=filter)
    return scraper.get_content()

def store_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key=None, task_ids='parse_data')
    parsedDt = kwargs.get('execution_date')
    #logging.info('paredDt: {}'.format(parsedDt))
    new_data = []
    for row in data:
        row['paresd_dt'] = parsedDt
        new_data.append(tuple(row.values()))

    connection = MySqlHook(mysql_conn_id='mysql_propertydb')
    conn = connection.get_conn()
    conn.autocommit = True
    cursor = conn.cursor()
    sql = "INSERT INTO property(price,location,size,parse_dt) VALUES(%s,%s,%s,%s)"
    cursor.executemany(sql, new_data)
    conn.commit()
    return 'store_data'

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2021,9,8,00,00,00),
    'concurrency': 1,
    'retries': 2
}

with DAG('load_property_data',
         catchup=False,
         default_args=default_args,
         schedule_interval='0 9 * * *',
) as dag:
    opr_parse_propertyData = PythonOperator(task_id='parse_data',
                                      python_callable=parse_data)
    opr_store_data = PythonOperator(task_id='store_data',
                                    python_callable=store_data)
    partition = CustomMySqlOperator(task_id='partition',
                                    mysql_conn_id='mysql_propertydb',
                                    sql='sql/partition.sql')

opr_parse_propertyData >> partition >> opr_store_data