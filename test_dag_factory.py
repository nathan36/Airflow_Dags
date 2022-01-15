from class_module.dag_factory import DagFactory
import datetime
from class_module.task import Task


# first task to run
def say_hi():
    print('hi')


# next task in parallel with say_bye
def print_date():
    print(f'Today is {datetime.date.today()}')


# next task in parallel with print_date
def say_bye():
    print('bye')


tasks = []
# say_hi has no dependencies, set to []
tasks.append(Task(type='python', func=say_hi, dependencies=[]))
# the other 2 tasks depend on say_hi
tasks.append(Task(type='python', func=print_date, dependencies=[say_hi]))
tasks.append(Task(type='python', func=say_bye, dependencies=[say_hi]))

DAG_NAME = 'example_with_dag_factory'

override_args = {
    'owner': 'Awesome Data Engineer',
    'retries': 2
}

dag = DagFactory().get_airflow_dag(DAG_NAME, tasks, default_args=override_args, cron='0 9 * * *')