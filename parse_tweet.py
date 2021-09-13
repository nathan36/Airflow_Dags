from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable
import datetime as dt
import twint

# pycharm debug server
import pydevd_pycharm
#pydevd_pycharm.settrace('localhost', port=9292, stdoutToServer=True, stderrToServer=True)

def parse_tweets():
    c = twint.Config()
    settings = Variable.get("settings", deserialize_json=True)
    c.Since = settings['start_date']
    c.Until = settings['end_date']
    c.Search = settings['search_key_word']
    c.Limit = settings['limit']
    c.Pandas = True
    twint.run.Search(c)
    tweets_df = twint.storage.panda.Tweets_df
    tweets_df = tweets_df.loc[tweets_df.retweet == False, ['date', 'tweet']]
    tweets_dic = tweets_df.set_index('date')['tweet'].to_dict()
    return tweets_dic

def store_data(**kwargs):
    ti = kwargs['ti']
    tweets = ti.xcom_pull(key=None, task_ids='parse_tweets')
    connection = MySqlHook(mysql_conn_id='mysql_default')
    settings = Variable.get("settings", deserialize_json=True)
    keyword = settings['search_key_word']
    for k, v in tweets.items():
        data = v.encode('latin-1', 'ignore')
        sql = 'INSERT INTO tweet_data(key_word,data,date) VALUES(%s,%s,%s)'
        connection.run(sql, autocommit=True, parameters=(keyword, data, k))
    return 'store_data'

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2021,9,7,00,00,00),
    'concurrency': 1,
    'retries': 1
}

with DAG('parsing_tweets',
         catchup=False,
         default_args=default_args,
         schedule_interval=None,
) as dag:
    opr_parse_tweets = PythonOperator(task_id='parse_tweets',
                                      python_callable=parse_tweets)
    opr_store_data = PythonOperator(task_id='store_data',
                                    python_callable=store_data)

opr_parse_tweets >> opr_store_data
