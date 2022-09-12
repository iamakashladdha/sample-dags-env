from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable


default_args = {
        'owner': 'customer success',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    }

with DAG(dag_id='dynamic_task_mapping_known_values',
         start_date=datetime(2022, 5, 1),
         schedule_interval='@once',
         default_args=default_args,
         tags=['dynamic task mapping'],
         description='''
            This DAG demonstrates dynamic task mapping with a constant parameter based on the known values, 
         ''',
         ) as dag:

    @task
    def add(x, y):
        print("Total Sum is: " + str(x+y))
        return x + y

    # partial(): allows you to pass parameters that remain constant for all tasks
    # expand(): allows you to pass parameters to map over
    added_values = add.partial(x=int(Variable.get("valuex_for_addition_var"))).expand(y=[0, 1, 2])
    # Variable comes from the AWS Parameter Store