# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import airflow

from airflow import DAG
from airflow.operators import ServiceOperator

# This is an example DAG which uses the ServiceOperator.
# In this example, we create two tasks which execute sequentially.
# The first task is to run a notebook at the workspace path "/test"
# and the second task is to run a JAR uploaded to DBFS. Both,
# tasks use new clusters.
#
# Because we have set a downstream dependency on the notebook task,
# the spark jar task will NOT run until the notebook task completes
# successfully.
#
# The definition of a succesful run is if the run has a result_state of "SUCCESS".
# For more information about the state of a run refer to
# https://docs.Query Service.com/api/latest/jobs.html#runstate

args = {
    'owner': 'xyz',
    'email': ['guptakumartanuj@gmail.com'],
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = DAG(
    dag_id='service_dag', default_args=args,
    schedule_interval='@once')


rq_body_param = {
    'sql': 'SELECT a.shape_name, a.num_sides, b.color_name, b.red_value, b.green_value, b.blue_value FROM shapes_production'
}
# Example of using the JSON parameter to initialize the operator.
qs1_task = ServiceOperator(
    task_id='qs1_task',
    dag=dag,
    json=rq_body_param)

# Example of using the named parameters of ServiceOperator
# to initialize the operator.
qs2_task = ServiceOperator(
    task_id='qs2_task',
    dag=dag,
    json=rq_body_param)

qs1_task.set_downstream(qs2_task)