from dataclasses import dataclass, field
from typing import Optional, Any, List
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator

@dataclass
class Task:
    type: str
    func: Optional[Any] = None
    conn: Optional[str] = None
    path: Optional[str] = None
    dependencies: List = field(default_factory=list)

    def create_pyhonOperator(self, dag) -> dict:
        task_dict = {}
        task_id = self.func.__name__
        task = PythonOperator(
            task_id=task_id,
            python_callable=self.func,
            dag=dag
        )
        task_dict[task_id] = task
        return task_dict

    def create_MySqlOpperator(self, dag) -> dict:
        task_dict = {}
        task_id = self.func.__name__
        task = MySqlOperator(
            task_id=task_id,
            mysql_conn_id=self.conn,
            sql=self.path,
            dag=dag
        )
        task_dict[task_id] = task
        return task_dict

    def create_tasks(self, dag) -> dict:
        if self.type == 'mysql':
            return self.create_MySqlOpperator(dag)
        if self.type == 'python':
            return self.create_pyhonOperator(dag)