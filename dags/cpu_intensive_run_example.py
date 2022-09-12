from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.sensors.date_time import DateTimeSensorAsync
from airflow.operators.dummy import DummyOperator
from multiprocessing import cpu_count
import time, random, signal, threading


'''
    Steps to run CPU intensive DAG Run

    In the pop-up, select Trigger DAG w/ config to enter a configuration for the DAG run. 
    This DAG run's default running time is set to 60 seconds, but in many cases, this may be a bit short. 
    Therefore, in the config JSON, add a parameter duration and set it to 600 seconds i.e.

    { "duration": 600}

'''

# example DAG to promote CPU load during the DAG run
stop_loop = 0

def exit_load():
    global stop_loop
    stop_loop = 1

def induce_cpu_load():
    global stop_loop
    print(f'starting cpu load')
    while not stop_loop:
        pr = 213123  # generates some load
        pr * pr
        pr = pr + 1

default_args = {
    'owner': 'customer success'
}

with DAG(
    f"cpu_intensive_run_example",
    start_date=days_ago(0),
    schedule_interval=None,
    catchup=False,
    max_active_runs=32,
    max_active_tasks=32,
    default_args=default_args,
    tags=[
        'example',
        'cpu-run'
    ]
) as dag:

    processes = cpu_count()

    # simple init task that initializes this dag run
    @task(task_id="init")
    def init(**kwargs):
        dag_run = kwargs['dag_run']
        # try getting duration from dag run config, if any
        duration = dag_run.conf.get('duration', 60)
        return { 'duration': duration }

    complete = DummyOperator(task_id="complete")

    cpu_loads = []

    for i in range(processes):
        @task(task_id=f"cpu_intensive_{i}")
        def cpu_intensive(**kwargs):
            print(kwargs)
            ti = kwargs['ti']
            # looks like xcom_pull returns 'array', so choosing the first one.
            config = ti.xcom_pull(key='return_value', task_ids=['init'])[0]
            print(config)
            load = threading.Thread(target=induce_cpu_load)
            load.start()

            # start the sleep
            time.sleep(config['duration'])
            exit_load()
            print(f'finishing load after {config["duration"]} seconds.')
        
        cpu_loads.append(cpu_intensive())
    
    init() >> cpu_loads >> complete
