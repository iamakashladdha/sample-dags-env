Dags, Variables, Connections and Pools for Creating Sample Airflow Environment
======

**Variables**
- data_for_file_var - For Dag `dynamic_task_mapping_known_values`
- githubrepoendpoint - For Dag `extract_github_stars`.
- valuex_for_addition_var - For Dag `file_sensor_dag_example`.

**Connections**
- GitHub HTTP Connection `github_api` for Dag `extract_github_stars.py`.
- File Connection `fs_default` for Dag `file_sensor_dag_example.py`.

**Pools**
- default_pool - For Dags which doesn't explictly specify a pool.
- partner_pool - For Dag `dynamic_tasks_branch_pool_dag`.
- pool_example - For Dag `pool_dag_example`.

**include**
- `hello.txt` created by the Dag `file_sensor_dag_example.py`.

**requirement.txt**
- apache-airflow-providers-http==4.0.0 for the Dag `extract_github_stars.py`.
- apache-airflow-providers-github==2.1.0 for the Dag `extract_github_stars.py`.

**Dags**
branching_example.py
cpu_intensive_run_example.py
deferrable_operator_dag.py
deferrable_sleep_dag.py
dependency_dag_example.py
dynamic_tasks_dag_example.py
dynamic_tasks_dag_pool_example.py
dynamic_tasks_mapping_known_values.py
dynamic_tasks_mapping_mixed_operators.py
example-dag-advanced.py
example-dag-basic.py
example_bash_operator.py
example_branch_day_of_week_operator.py
example_branch_operator_dop.py
example_short_circuit_operator.py
example_skip_dags.py
example_sla_dag.py
example_time_delta_sensor_async.py
exmaple_nested_branch_dag.py
extract_github_stars.py
file_sensor_dag_example.py
mem_intensive_run_example.py
pool_dag_example.py
task_group_dag.py
xcom_dag_example.py
