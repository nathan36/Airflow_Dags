from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable
import datetime as dt
import twint
from airflow.decorators import task, dag

@task
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

@task
def store_data(records: dict):
    connection = MySqlHook(mysql_conn_id='mysql_default')
    settings = Variable.get("settings", deserialize_json=True)
    keyword = settings['search_key_word']
    for k, v in records.items():
        data = v.encode('latin-1', 'ignore')
        sql = 'INSERT INTO tweet_data(key_word,data,date) VALUES(%s,%s,%s)'
        connection.run(sql, autocommit=True, parameters=(keyword, data, k))

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2021,9,8,00,00,00),
    'concurrency': 1,
    'retries': 0
}

@dag(schedule_interval=None, default_args=default_args, catchup=False)
def parsing_tweets_taskflow():
    tweets_df = parse_tweets()
    store_data(tweets_df)

dag = parsing_tweets_taskflow()

