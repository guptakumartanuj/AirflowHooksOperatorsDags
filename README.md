# AirflowHooksOperatorsDags
The purpose of this repo is to learn how to write DAG, custom hooks and operators.


Introduction: Apache Airflow is the open source project started by Airbnb written in python, works as a workflow engine to schedule and monitor workflows. Airflow is based on the concept called DAG (Dynamic Acyclic Graph) in which vertices are represented as tasks (Operator/Sensor) and edges are represented as a sequence of pipeline (which defines the order in which task has to complete).


Installation Steps on MAC machine:

     Need to setup a home for airflow directory using the below command -

 $ mkdir ~/Airflow export AIRFLOW_HOME=~/Airflow

    As airflow is written in python. So first make sure that python is installed on the machine. If not, use the below command to install the python -

           $ cd Airflow brew install python python3

     Now install airflow using pip (package management system used to install and manage software packages written in Python).

 $ pip install airflow

             Most probably, you would be getting some installation error which is given below using the above command -

             "Found existing installation: six 1.4.1 DEPRECATION: Uninstalling a distutils installed project (six) has been deprecated and will be removed in a future version. This is due to                 the fact that uninstalling a distutils   project will only partially uninstall the project.  Uninstalling six-1.4.1:"

    So to avoid this, use the below command to install the airflow successfully -

 $ pip install --ignore-installed six airflow 

    # To install required packages based on the need 
 $ pip install--ignore-installed six airflow[crypto] # For connection credentials security
 $ pip install--ignore-installed six airflow[postgres] # For PostgreSQL Database
 $ pip install--ignore-installed six airflow[celery] # For distributed mode: celery executor
 $ pip install--ignore-installed six airflow[rabbitmq] # For message queuing and passing between airflow server and workers

    Even after executing the above command, you would be getting some permission errors like "error: [Errno 13] Permission denied: '/usr/local/bin/mako-render". So give permission to all those folders which are getting executed in the above command -

 $ sudo chown -R $USER /Library/Python/2.7
           $ sudo chown -R $USER /usr/local/bin/

    Airflow uses a sqlite database which will be installed in parallel and create the necessary tables to check the status of DAG (Directed Acyclic Graph – is a collection of all the tasks you want to run, organised in a way that reflects their relationships and dependencies.) and other information related to this.

     Now as a last step we need to initialise the sqlite database using the below command-

 $ airflow initdb

     Finally, everything is done and it's time to start the web server to play with Airflow UI using the below command -

 $ airflow webserver -p 8080

    You can now visit the Airflow UI by navigating your browser to port 8080(Chane the IP and port in airflow.cfg file if required) on the host where Airflow was started, for example: http://localhost:8080/admin/


Airflow Directory Structure:

AIRFLOW_HOME
├── airflow.cfg (This file contains Airflow’s default configuration. We can edit it to any setting related to executor, brokers etc)
├── airflow.db (This file contains information about database (SQLite DB by default) once airflow initialize the db)
├── dags        <-  DAGs directory (All the dags are kept inside this folders.)
│   └── test.py     <- (test DAG python file. Ensure to compile the same before running the DAG)
│   └── test_first_operators.py  <- (test first operator DAG python file. Ensure to compile the same before running the DAG)                                                                                       
├── plugins
│    └── first_operators.py    <- (First Operator python file. Ensure to compile the same before running it)

│    └──  first_sensor.py  <- (First Sensor python file. Ensure to compile the same before running it)
└── unittests.cfg (This file contains the default configuration related to junit tests)


Steps to run the DAG and task:

    As per the above directory structure, we just need to place the DAG file inside dags folder of AIRFLOW_HOME. Just make sure to compile the file successfully.
    Now start the Airflow Scheduler by issuing the following command - $ airflow scheduler
    Once the scheduler is started, it will send the task for execution based on defined executor in airflow config file. By default, tasks are scheduled by SequentialExecutor(This has nothing to do with concurrency). To achieve parallelism, one should either go with CeleryExecutor or MesosExecutor for robustness.
    In order to start the DAG, go to Admin UI and turn on the DAG. Now either trigger the DAG by UI or use the below command to run the DAG -

                             # run your first task instance airflow run test task1 2018-01-20

                             # run a backfill over 2 days airflow backfill test -s 2018-01-21 -e 2018-01-22


Steps to write your own Plugin:

    Airflow has a simple plugin manager built-in that can integrate external features to its core by simply dropping files in your $AIRFLOW_HOME/plugins folder.
    The python modules in the plugins folder get imported, and hooks, operators, macros, executors and web views get integrated to Airflow’s main collections and become available for use.
    To create a plugin you will need to derive the airflow.plugins_manager.AirflowPlugin class.
    Extend with SuperClass BaseOperator, BaseHook, BaseExecutor, BaseSensorOperator and BaseView to write your own operator, hook, executor, sensor and view respectively as a part of plugin.

Custom Airflow Operator:

    An Operator is an atomic block of workflow logic, which performs a single action.
    To create a custom Operator class, we define a sub class of BaseOperator.
    Use the _init_()  function to initialize the settting for the given task.
    Use execute() function to execute the desired task. Any value that the execute method returns is saved as an Xcom message under the key return_value.
    To debug an operator install IPython library ($ pip install ipython) by placing IPython’s embed()command in your execute() method of an operator and Ariflow comes with "airflow test" command which you can use to manually start a single operator in the context of a specific DAG run.

                               $ airflow test test task1 2018-01-21     

Custom Airflow Sensor:

    It is a special type of Operator, typically used to monitor a long running task on another system.
    Sonsor class is created by extending BaseSensorOperator
    Use the _init_()  function to initialize the settting for the given task.
    Use poke() function to execute the desired task over and over every poke_interval seconds until it returns True and if it returns False it will be called again.

XCom (Cross-Communication):

    let tasks exchange messages, allowing more nuanced forms of control and shared state. 
    XComs are principally defined by a key, value, and timestamp.
    XComs can be “pushed” (sent) using xcom_push() functionor “pulled” (received) using xcom_pull() function. The information passed using Xcoms will be pickled and stored in the Airflow database (xcom table), so it’s better to save only small bits of information, rather then large objects.
                               task_instance.xcom_push('key1', value1)
                               value = task_instance.xcom_pull('task', key='key1)


Passing and Accessing run time arguments to Airflow through CLI:

    One can pass run time arguments at the time of triggering the DAG using below command -
                   $ airflow trigger_dag dag_id --conf '{"key":"value" }'
    Now, There are two ways in which one can access the parameters passed in airflow trigger_dag command -

    In the callable method defined in Operator, one can access the params as kwargs['dag_run'].conf.get('key')
    Given the field where you are using this thing is templatable field, one can use {{ dag_run.conf['key'] }}

Note* : The schedule_interval for the externally trigger-able DAG is set as None for the above approaches to work


Working with Local Executor:

LocalExecutor is widely used by the users in case they have moderate amounts of jobs to be executed. In this, worker picks the job and run locally via multiprocessing.

    Need to install PostgreSQL or MySql to support parallelism using any executor other then Sequential. Use the following command to do so - $ brew install postgresql.
    Modify the configuration in AIRFLOW_HOME/airflow.cfg -


                        # Change the executor to Local Executor

                         executor = LocalExecutor

                         # Change the meta db configuration 

                         sql_alchemy_conn = postgresql+psycopg2://user_name:password@host_name/database_name

       3. Restart airflow to test your dags

                         $ airflow initdb $ airflow webserver $ airflow scheduler

      4. Establish the db connections via the Airflow admin UI -

    Go to the Airflow Admin UI: Admin -> Connection -> Create

Connection Type	

Postgres(Database/AWS)

Host	Database server IP/localhost
Scheme	Database_Name
Username	User_Name
Password	Password


    Encrypt your credentials

            # Generate a valid Fernet key and place it into airflow.cfg

             FERNET_KEY=$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print FERNET_KEY")


Working with Celery Executor:

CeleryExecutor is the best choice for the users in production when they have heavy amounts of jobs to be executed. In this, remote worker picks the job and runs as scheduled and load balanced.

    Install and configure the message queuing/passing engine on the airflow server: RabbitMQ/Reddis/etc. -

                  1. Install RabbitMQ using $ brew install rabbitmq

                  2.  Add the following path to your .bash_profile or .profile - PATH=$PATH:/usr/local/sbin

                 3. Start the RabbitMQ server using the following commands -
                                    $ sudo rabbitmq-server # run in foreground; or
                                    $ sudo rabbitmq-server -detached # run in background

                  4. Configure RabbitMQ: create user and grant privileges
                                      $ rabbitmqctl add_user rabbitmq_user_name rabbitmq_password
                                      $ rabbitmqctl add_vhost rabbitmq_virtual_host_name
                                      $ rabbitmqctl set_user_tags rabbitmq_user_name rabbitmq_tag_name
                                      $ rabbitmqctl set_permissions -p rabbitmq_virtual_host_name rabbitmq_user_name ".*" ".*" ".*"

                  5. Make the RabbitMQ server open to remote connections -
                  Go to /usr/local/etc/rabbitmq/rabbitmq-env.conf, and change NODE_IP_ADDRESS from 127.0.0.1 to 0.0.0.0

    Modify the configuration in AIRFLOW_HOME/airflow.cfg - 

           1. Change the executor to Celery Executor
                               executor = CeleryExecutor

            2. Set up the RabbitMQ broker url and celery result backend
                              broker_url = amqp://rabbitmq_user_name:rabbitmq_password@host_name/rabbitmq_virtual_host_name # host_name=localhost on server
                              celery_result_backend = meta db url (as configured in step 2 of Phase 2), or RabbitMQ broker url (same as above), or any other eligible result backend
    Open the meta DB (PostgreSQL) to remote connections
             1. Modify /usr/local/var/postgres/pg_hba.conf to add Client Authentication Record                   

    host    all         all         0.0.0.0/0          md5 # 0.0.0.0/0 stands for all ips; use CIDR address to restrict access; md5 for pwd authentication

              2. Change the Listen Address in /usr/local/var/postgres/postgresql.conf
                             listen_addresses = '*'
              3. Create a user and grant privileges (run the commands below under superuser of postgres)
                          $ CREATE USER your_postgres_user_name WITH ENCRYPTED PASSWORD 'your_postgres_pwd';

                           $ GRANT ALL PRIVILEGES ON DATABASE your_database_name TO your_postgres_user_name;
                           $ GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO your_postgres_user_name;

                4. Restart the PostgreSQL server and test it out.
                          $ brew services restart postgresql

                           $ psql -U [postgres_user_name] -h [postgres_host_name] -d [postgres_database_name]
     IMPORTANT: update your sql_alchemy_conn string in airflow.cfg

    Start your airflow workers, on each worker, run: $ airflow worker.

    Your airflow workers should be now picking up and running jobs from the airflow server.