from airflow import DAG
from datetime import datetime, timedelta
from class_module.task import Task
from typing import List

class DagFactory:

    @classmethod
    def create_dag(cls, dagname, default_args={}, catchup=False, concurrency=5, cron=None) -> DAG:
        """
        :param dagname(str): the name of the dag
        :param default_args(dict): a dict with the specific keys you want to edit from the original DEFAULT_ARGS
        :param catchup(bool): Perform scheduler catchup (or only run latest)? Defaults to True
        :param concurrency(int): the number of task instances allowed to run concurrently
        :param cron(str): the cron expression or the schedule
        :return: DAG object
        """
        DEFAULT_ARGS={
            'owner': 'airflow',
            'depends_on_past': False,
            'start_date': datetime(2021, 1, 1),
            'email': ['airflow@company.com'],
            'email_on_failure': True,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        }

        DEFAULT_ARGS.update(default_args)
        dagargs = {
            'default_args': DEFAULT_ARGS,
            'schedule_interval': cron,
            'catchup': catchup,
            'concurrency': concurrency
        }

        dag = DAG(dagname, **dagargs)
        return dag

    @classmethod
    def add_task_to_dag(cls, dag, tasks: List[Task]) -> DAG:
        """
        :param dag(DAG)
        :param tasks(dict): dictionary in which each key is a callback. The value of that key is the task's dependencies.
        If a task has no dependencies (it's the first task), set an empty list [] as the value.
        IMPORTANT: all tasks have to be there even if they don't have dependencies
        :return: dag(DAG) with tasks
        """
        with dag as dag:
            aux_dict = {}

            for task in tasks:
                task_dict = task.create_tasks(dag)

                for task_id, task in task_dict.items():
                    aux_dict[task_id] = task

            for task in tasks:
                task_id = task.func.__name__
                for dep in task.dependencies:
                    aux_dict[dep.__name__] >> aux_dict[task_id]
        return dag

    @classmethod
    def get_airflow_dag(cls, dagname, tasks, default_args={}, catchup=False, concurrency=5, cron=None) -> DAG:
        dag = cls.create_dag(dagname, default_args=default_args, catchup=catchup, concurrency=concurrency, cron=cron)
        dag = cls.add_task_to_dag(dag, tasks)
        return dag