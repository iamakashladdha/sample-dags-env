from asyncio import Task
from airflow.decorators import task, task_group
from airflow.utils.task_group import TaskGroup

@task.python
def process_dataA(partner_name,partner_path):
    print(partner_name)
    print(partner_path)

@task.python
def process_dataB(partner_name,partner_path):
    print(partner_name)
    print(partner_path)

@task.python
def process_dataC(partner_name,partner_path):
    print(partner_name)
    print(partner_path)

@task.python
def check_dataA():
    print("Checking Data")

@task.python
def check_dataB():
    print("Checking Data")

@task.python
def check_dataC():
    print("Checking Data")

def process_tasks(partner_setting):
    with TaskGroup(group_id='process_tasks', add_suffix_on_collision=True) as process_tasks:
        with TaskGroup(group_id='check_tasks') as check_tasks:
            check_dataA()
            check_dataB()
            check_dataC()
        process_dataA(partner_setting['partner_name'],partner_setting['partner_path']) >> check_tasks
        process_dataB(partner_setting['partner_name'],partner_setting['partner_path']) >> check_tasks
        process_dataC(partner_setting['partner_name'],partner_setting['partner_path']) >> check_tasks
    return process_tasks