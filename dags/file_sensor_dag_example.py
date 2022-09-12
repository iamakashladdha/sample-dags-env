from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.models.baseoperator import chain, cross_downstream
from airflow.models import Variable

default_args = {
    'retry':5,
    'retry_delay':timedelta(minutes=5),
    'owner': 'customer_success'
}

def _downloading_data(my_param):
    with open('hello.txt','w') as f:
        data_for_file_var= Variable.get("data_for_file_var")
        print(data_for_file_var)
        f.write(data_for_file_var)
    return 42

def _checking_data(ti):
    my_xcom = ti.xcom_pull(key='return_value',task_ids=['downloading_data'])
    print(my_xcom)
    print("check data")

with DAG(
    dag_id='file_sensor_dag_example',
    tags=['example','file-sensor'],
    default_args=default_args,
    schedule_interval="@daily", 
    start_date=days_ago(3),
    catchup=True,
    max_active_runs=3) as dag: 

    downloading_data = PythonOperator(
        task_id = 'downloading_data',
        python_callable=_downloading_data,
        op_kwargs={'my_param':42}
    )
    checking_data= PythonOperator(
        task_id = 'checking_data',
        python_callable= _checking_data
    )
    waiting_for_data = FileSensor(
        task_id='waiting_for_the_data',
        fs_conn_id='fs_default',
        filepath='hello.txt',
        poke_interval=30
    )
    processing_data = BashOperator(
        task_id='processing_data',
        bash_command='exit 0'
    )

    #downloading_data.set_downstream(waiting_for_data)
    #waiting_for_data.set_downstream(processing_data)
    chain(downloading_data, checking_data, waiting_for_data, processing_data)
