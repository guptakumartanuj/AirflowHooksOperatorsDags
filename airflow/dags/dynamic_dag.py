from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators import MyFirstOperator, MyFirstSensor

## Create JSON Var if it doesn't exist
## Get JSON Var
CUSTOMERS = [
    {
        'name': 'Tanuj Gupta',
        'ldap': 'guptakumartanuj',
        'email': ['guptakumartanuj@gmail.com'],
        'schedule_interval': None,
        'flag': True
    },
    {
        'name': 'Raman Gupta',
        'ldap': 'guptakumartanuj',
        'email': ['rguptakumartanuj@gmail.com'],
        'schedule_interval': None,
        'flag': True
    }
]

CUSTOMERS = Variable.get(
    "customer_list", default_var=CUSTOMERS, deserialize_json=True)


def create_dag(customer):
    """
    Takes a cust parameters dict, uses that to override default args creates DAG object
    
    Returns: DAG() Object
    """
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'email': 'guptakumartanuj@gmail.com',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'start_date': datetime(2018, 1, 1, 0, 0),
        'end_date': None
    }

    # This allows DAG parameters to be passed in from the Variable if a customer needs something specific overridden in their DAG
    # Consider how email being passed in from the customer object overrides email in the resulting replaced_args object
    replaced_args = {k: default_args[k] if customer.get(
        k, None) is None else customer[k] for k in default_args}


    dag_id = '{base_name}_{id}'.format(
        base_name='xyz_data_platform', id=customer['ldap'])

    return DAG(dag_id=dag_id, default_args=replaced_args, schedule_interval=customer['schedule_interval'])

for cust in CUSTOMERS:  # Loop customers array of containing customer objects
    if cust['flag']:
        dag = create_dag(cust)
        globals()[dag.dag_id] = dag

        extract = DummyOperator(
            task_id='extract_data',
            dag=dag
        )

        transform = DummyOperator(
            task_id='transform_data',
            dag=dag
        )

        load = DummyOperator(
            task_id='load_data',
            dag=dag
        )
        
        templated_command = """
           {% for i in range(5) %}
           echo "{{ dag_run.conf["key"] }}"
           echo "{{ ds }}"
           echo "{{ dag_run.conf.test }}"
           echo "{{ macros.ds_add(ds, 7)}}"
           echo "{{ params.my_param }}"
           {% endfor %}
        """

        custom = BashOperator(
            task_id='templated',
            bash_command=templated_command,
            params={'my_param': cust['name']},
            dag=dag)
    
        operator_task = MyFirstOperator(my_operator_param='This is a test.',
                                task_id='my_first_operator_task', dag=dag)
                                
        extract.set_downstream(transform)
        transform.set_downstream(load)
        load.set_downstream(custom)
        custom.set_downstream(operator_task)