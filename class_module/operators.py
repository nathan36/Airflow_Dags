from airflow.providers.mysql.operators.mysql import MySqlOperator

class CustomMySqlOperator(MySqlOperator):
    template_fields = ('sql', 'params')