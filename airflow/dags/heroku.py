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
"""
### Example HTTP operator and sensor
"""
import airflow
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import HttpSensor
from datetime import timedelta
import json


args = {
    'owner': 'xyz',
    'email': ['guptakumartanuj@gmail.com'],
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = DAG(
    dag_id='heroku_crypto', default_args=args,
    schedule_interval='@once')

dag.doc_md = __doc__

def response_check(response):
    """
    Dumps the http response and returns True when the http call status is 200/success
    """
    print(response)
    print(response.text)
    return response.status_code == 200  
    
t2 = SimpleHttpOperator(
    task_id='heroku_coin',
    http_conn_id='heroku_conn',
    method='GET',
    endpoint='',
    headers={"Content-Type": "application/json"},
    xcom_push=True,
    response_check=response_check,
    dag=dag)

def print_hello():
    return 'Hello world!'
        
hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

t2.set_upstream(hello_operator)