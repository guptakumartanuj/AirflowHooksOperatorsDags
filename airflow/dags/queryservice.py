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
import MySQLdb
import logging;
from airflow.hooks.http_hook import HttpHook
import os
from airflow import configuration

args = {
    'owner': 'xyz',
    'email': ['guptakumartanuj@gmail.com'],
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2)
}

task=json.dumps({"sql":"SELECT a.shape_name, a.num_sides, b.color_name, b.red_value, b.green_value, b.blue_value FROM shapes_production "})

dag = DAG(
    dag_id='Lookup_QS_xyz', default_args=args,
    schedule_interval='@once')

dag.doc_md = __doc__

def response_check(response):
    """
    Dumps the http response and returns True when the http call status is 200/success
    """
    print(response)
    print(response.text)
    return response.status_code == 201  
    
t2 = SimpleHttpOperator(
    task_id='PQS_details',
    #task_id='Lookup_Post',
    http_conn_id='qs_default',
    #http_conn_id='lookup_conn',
    method='POST',
    data=task,
    #data={"sql":"SELECT a.shape_name, a.num_sides, b.color_name, b.red_value, b.green_value, b.blue_value FROM shapes_production;"},
    #data=json.dumps({'ldap' : 'tangupta'}),
    endpoint='query/12345@xyzOrg/test',
    #endpoint='/lookup/dataSets/testCollection/keys/ldap?imsOrg=testDb',
    headers={"Content-Type": "application/json", "Authorization" : "Bearer token"},
    #headers={"Content-Type": "application/json"},
    xcom_push=True,
    response_check=response_check,
    dag=dag)

def print_hello():
    task_id='PQS_details'
    #task_id='Lookup_Post',
    http_conn_id='qs_default'
    #http_conn_id='lookup_conn',
    method='POST'
    data=task
    #data={"sql":"SELECT a.shape_name, a.num_sides, b.color_name, b.red_value, b.green_value, b.blue_value FROM shapes_production a, colors_production b WHERE a.color_name = b.color_name LIMIT 5;"},
    #data=json.dumps({'ldap' : 'tangupta'}),
    endpoint='query/12345@xyzOrg/test'
    #endpoint='/lookup/dataSets/testCollection/keys/ldap?imsOrg=testDb',
    headers={"Content-Type": "application/json", "Authorization" : "Bearer eyJ4NXUiOiJpbXMta2V5LTEuY2VyIiwiYWxnIjoiUlMyNTYifQ.eyJpZCI6IjE1MTcyNDUzNzgzNDVfYjg2ZGU4ZDgtZTAzZi00OGNmLThmZTAtN2VmNDcxYjFhYzRhX3VlMSIsImNsaWVudF9pZCI6Ik1DRFBDYXRhbG9nU2VydmljZVFhMiIsInVzZXJfaWQiOiJNQ0RQQ2F0YWxvZ1NlcnZpY2VRYTJAQWRvYmVJRCIsInR5cGUiOiJhY2Nlc3NfdG9rZW4iLCJhcyI6Imltcy1uYTEtcWEyIiwicGFjIjoiTUNEUENhdGFsb2dTZXJ2aWNlUWEyX2RldnFhIiwicnRpZCI6IjE1MTcyNDUzNzgzNTBfZjNkOTExMTEtZjFjNi00OTIzLThjNWEtYThmN2RjNGJkNjhhX3VlMSIsInJ0ZWEiOiIxNTE4NDU0OTc4MzUwIiwibW9pIjoiMzI5YjFmNDciLCJjIjoiTnd3VmE4UG9ubDZuWkZpQi8yZjdkUT09IiwiZXhwaXJlc19pbiI6Ijg2NDAwMDAwIiwic2NvcGUiOiJzeXN0ZW0sQWRvYmVJRCxvcGVuaWQiLCJjcmVhdGVkX2F0IjoiMTUxNzI0NTM3ODM0NSJ9.MTuH1VI-b4PcLlr1BHB4a_CXxwn8TbsyQVk3pJi2uoNpng9jhH1W-lfg0pzbhLy0Ji81HlIopbTEGYNYlfXjzE9OtKM-BX7-VQOSXmS7tgAkfec0OheZariZUIdpXUa4YY-BHLsSijkBWs-AUTzZ9SG91M-8cWn_4U7uPSRn2aqEqT92kZgRRl-u5Zq6YteSFXUJCpY64IOO2c-sZfLQ6yQb425qUyn468ECPElZ5h00483xs81ZFF-r8LFhQtQpQ7m9pfNFZWqUexM_RrQNUL6XZmiq7mZ1_wxw1RSIDDMTvt35JU2MUjs6B0c9GwMTL814ej0ewrDV1qhAIliLlA"}
    extra_options={}
    http = HttpHook(method, http_conn_id='qs_default')

    logging.info('Calling HTTP method')
    print(os.environ['PATH'])
    print(os.environ['TEST_PATH'])
    response = http.run('query/12345@xyzOrg/visual',
                            data,
                            headers,
                            extra_options)
    print(response)
    print(response.text)
    print(configuration.get('testing', 'tanuj').encode('utf-8'))
    return 'Hello world!'

#from IPython import embed; embed()

hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

t2.set_upstream(hello_operator)