from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable
import datetime as dt
from class_module.property_parser import parser
from class_module.operators import CustomMySqlOperator
import logging

def fetch_area_list(**kwargs):
    dag_run_conf = kwargs['dag_run'].conf
    area_list = dag_run_conf.get('area_list')
    # check if list parameter is passed in as string
    if isinstance(area_list, str):
        area_list = eval(area_list)

    config = Variable.get("config", deserialize_json=True)
    type = config['type']
    max_price = config['max_price']
    min_year_built = config['min_year_built']

    session = parser(area_list, type, max_price, min_year_built)
    session.check_area()

    return  {'area_list': area_list,
             'area_list_with_error': session.area_list_with_error,
             'status': session.response_status}

def check_api_response(**kwargs):
    ti = kwargs['ti']
    response = ti.xcom_pull(task_ids='fetch_area_list')
    response_status = response.get("status")
    if response_status == 'success':
        return_value = 'success'
    elif response_status == 'error':
        return_value = 'refresh_area_list'
    else:
        return_value = ''

    return return_value

def refresh_area_list(**kwargs):
    dag_run_conf = kwargs['dag_run'].conf
    old_area_list = dag_run_conf.get('area_list')

    task_instance = kwargs['task_instance']
    response = task_instance.xcom_pull(task_ids='fetch_area_list')
    area_list_with_error = response.get("area_list_with_error")
    new_area_list = [x for x in old_area_list if x not in area_list_with_error]

    return new_area_list

def fetch_property_data(**kwargs):
    ti = kwargs['ti']
    area_list = ti.xcom_pull(task_ids='fetch_area_list')['area_list']
    config = Variable.get("config", deserialize_json=True)
    type = config['type']
    max_price = config['max_price']
    min_year_built = config['min_year_built']

    session = parser(area_list, type, max_price, min_year_built)
    session.get_data()

    return session.parse_data()

def store_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key=None, task_ids='fetch_property_data')
    parsedDt = kwargs.get('execution_date')
    #logging.info('paredDt: {}'.format(parsedDt))
    new_data = []
    for row in data:
        row['parsed_dt'] = parsedDt
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
    fetch_area_list = PythonOperator(task_id='fetch_area_list',
                                     python_callable=fetch_area_list)
    check_api_response = BranchPythonOperator(task_id='chcek_api_response',
                                              python_callable=check_api_response)
    success = DummyOperator(task_id='success')
    fetch_property_data = PythonOperator(task_id='fetch_property_data',
                                         python_callable=fetch_property_data)
    partition = CustomMySqlOperator(task_id='partition',
                                    mysql_conn_id='mysql_propertydb',
                                    sql='sql/partition.sql')
    store_data = PythonOperator(task_id='store_data',
                                python_callable=store_data)
    refresh_area_list = PythonOperator(task_id='refresh_area_list',
                                       python_callable=refresh_area_list)
    retry_api = TriggerDagRunOperator(task_id='retry_api',
                                      trigger_dag_id='load_property_data',
                                      conf={'area_list': "{{ ti.xcom_pull(task_ids='refresh_area_list',key='return_value')}}"})

fetch_area_list >> check_api_response >> [refresh_area_list, success]
refresh_area_list >> retry_api
success >> fetch_property_data >> partition >> store_data