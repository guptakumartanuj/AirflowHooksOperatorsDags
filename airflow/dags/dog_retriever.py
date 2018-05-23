import airflow
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.sensors import HttpSensor
from datetime import datetime, timedelta
import json



default_args = {
    'owner': 'Loftium',
    'depends_on_past': False,
    'start_date': datetime(2017, 10, 9),
    'email': 'guptakumartanuj@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG('dog_retriever',
    schedule_interval='@once',
    default_args=default_args)
    
def response_check(response):
    """
    Dumps the http response and returns True when the http call status is 200/success
    """
    print(response)
    print(response.text)
    return response.status_code == 200    

t1 = SimpleHttpOperator(
    task_id='get_labrador',
    method='GET',
    http_conn_id='http_default',
    endpoint='api/breed/labrador/images',
    headers={"Content-Type": "application/json"},
    xcom_push=True,
    response_check=response_check,
    dag=dag)

t2 = SimpleHttpOperator(
    task_id='get_breeds',
    method='GET',
    http_conn_id='http_default',
    endpoint='api/breeds/list',
    headers={"Content-Type": "application/json"},
    dag=dag)

t2.set_upstream(t1)