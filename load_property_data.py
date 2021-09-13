from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable
import datetime as dt
from class_module.property_parser import parser

def parse_data():
    config = Variable.get("config", deserialize_json=True)
    area = config['area']
    type = config['type']
    max_price = config['max_price']
    min_year_built = config['min_year_built']

    session = parser(area, type, max_price, min_year_built)
    session.get_request()
    data = session.parse_data()
    return data

def store_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key=None, task_ids='parse_data')
    connection = MySqlHook(mysql_conn_id='mysql_default')
    for row in data:
        sql = 'INSERT INTO property(price,location,size) VALUES(%s,%s,%s)'
        connection.run(sql, autocommit=True, parameters=(row['price'],
                                                         row['location'],
                                                         row['size']))
    return 'store_data'

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2021,9,11,00,00,00),
    'concurrency': 1,
    'retries': 2
}

with DAG('load_property_data',
         catchup=True,
         default_args=default_args,
         schedule_interval='0 6 * * *',
) as dag:
    opr_parse_propertyData = PythonOperator(task_id='parse_data',
                                      python_callable=parse_data)
    opr_store_data = PythonOperator(task_id='store_data',
                                    python_callable=store_data)

opr_parse_propertyData >> opr_store_data