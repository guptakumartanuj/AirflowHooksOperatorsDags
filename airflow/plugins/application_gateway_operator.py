

import six
import time
import logging
import json
from airflow.exceptions import AirflowException
from airflow.plugins_manager import AirflowPlugin
from Application_gateway_hook import ApplicationGatewayHook
from service_token_hook import ServiceToken
from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook

class ApplicationGatewayOperator(BaseOperator):

    def __init__(
            self,
            json=None,
            imsJson=None,
           
            run_name=None,
            timeout_seconds=None,
            qs_conn_id='default_connection',
            polling_period_seconds=30,
            qs_retry_limit=3,
            **kwargs):
       
        super(ApplicationGatewayOperator, self).__init__(**kwargs)
        self.json = json or {}
        self.imsJson = imsJson or {}
        self.qs_conn_id = qs_conn_id
        self.polling_period_seconds = polling_period_seconds
        self.qs_retry_limit = qs_retry_limit
        if run_name is not None:
            self.json['run_name'] = run_name
        if timeout_seconds is not None:
            self.json['timeout_seconds'] = timeout_seconds
        if 'run_name' not in self.json:
            self.json['run_name'] = run_name or kwargs['task_id']
            
        self.json = self.deep_string_coerce(self.json)
        self.run_id = None
        self.token = None

    def deep_string_coerce(self, content, json_path='json'):
        
        c = self.deep_string_coerce
        if isinstance(content, six.string_types):
            return content
        elif isinstance(content, six.integer_types+(float,)):
            # Application Gateway can tolerate either numeric or string types in the API backend.
            return str(content)
        elif isinstance(content, (list, tuple)):
            return [c(e, '{0}[{1}]'.format(json_path, i)) for i, e in enumerate(content)]
        elif isinstance(content, dict):
            return {k: c(v, '{0}[{1}]'.format(json_path, k))
                    for k, v in list(content.items())}
        else:
            param_type = type(content)
            msg = 'Type {0} used for parameter {1} is not a number or a string' \
                    .format(param_type, json_path)
            raise AirflowException(msg)


    def get_hook(self):
        return APPLICATIONGatewayHook(
            self.qs_conn_id,
            retry_limit=self.qs_retry_limit)
    
    def get_service_token_hook(self):
        return ServiceToken(
            'ims_default',
            retry_limit=self.qs_retry_limit)
                          

    def execute(self, context, **kwargs):
        
        service_token_hook = self.get_service_token_hook()
        self.imsJson={}
        token = service_token_hook.execute_ims_job(self.imsJson)
       
        logging.info('IMS Run submitted with token: %s', token)
           
        Application_hook = self.get_hook()
        self.run_id = Application_hook.execute_spark_job(token, self.json)
       
        logging.info('Application Gateway Job Run submitted with run_id: %s', self.run_id);
        while True:
            run_state = Application_hook.spark_job_state(self.run_id, token, self.imsJson)

            if run_state.is_finished:
                    logging.info('%s completed successfully.', self.task_id)
                    return
            elif run_state.is_terminated:
                    error_message = '{t} failed with terminal state: {s}'.format(
                        t=self.task_id,
                        s=run_state.result_state)
                    logging.info('error_message : %s ', error_message)
                    raise AirflowException(error_message)
            else:
                logging.info('%s in run state: %s', self.task_id, run_state.result_state)
                
                logging.info('Sleeping for %s seconds.', self.polling_period_seconds)
                time.sleep(self.polling_period_seconds)

        logging.info('CG Run submitted with run_id: %s', self.run_id)



                
class ApplicationGatewayPlugin(AirflowPlugin):
    name = "Application_gateway_plugin"
    operators = [ApplicationGatewayOperator]