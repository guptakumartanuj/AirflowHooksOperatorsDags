

import requests
import logging
import json

from airflow import __version__
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from requests import exceptions as requests_exceptions
from requests.auth import AuthBase


try:
    from urllib import parse as urlparse
except ImportError:
    import urlparse


Submit_Batch_Job_POST_EndPoint = ('POST', 'abc/test/execute/jobs')
Submit_Batch_Job_GET_EndPoint = ('GET', 'abc/test/execute/jobs/')
User_Headers = {'user-agent': 'airflow-{v}'.format(v=__version__), 'accept': 'application/json', 'Content-Type': 'application/json'}

class APPLICATIONGatewayHook(BaseHook):
    """
    Interact with APPLICATION Gateway.
    """
    def __init__(
            self,
            APPLICATION_connection_id='default_connection',
            timeout_seconds=180,
            retry_limit=3):

        self.APPLICATION_connection_id = APPLICATION_connection_id
        self.APPLICATION_connection = self.get_connection(APPLICATION_connection_id)
        self.timeout_seconds = timeout_seconds
        assert retry_limit >= 1, 'Retry limit must be greater than equal to 1'
        self.retry_limit = retry_limit

    def parse_host(self, host):

        urlparse_host = urlparse.urlparse(host).hostname
        if urlparse_host:
            # In this case, host = https://xx.cloud.APPLICATION Gateway.com
            return urlparse_host
        else:
            # In this case, host = xx.cloud.APPLICATION Gateway.com
            return host

    def submit_batch_job(self, method, token, json, run_id):

        logging.info('Token in submit_batch_job is : %s',token);
        if method == 'GET':
            request_func = requests.get
            endpoint = self.APPLICATION_connection.extra_dejson.get('GET_END_POINT', None)
            if endpoint is  None:
                         endpoint = Submit_Batch_Job_GET_EndPoint
        elif method == 'POST':
            request_func = requests.post
            endpoint = self.APPLICATION_connection.extra_dejson.get('POST_END_POINT', None)
            if endpoint is  None:
                         endpoint = Submit_Batch_Job_POST_EndPoint
        else:
            raise AirflowException('Unexpected HTTP Method: ' + method)

        url = 'https://{host}/{endpoint}'.format(
            host=self.parse_host(self.APPLICATION_connection.host),
            endpoint=endpoint)
        logging.info('URL :: '+url)
        logging.info(json)

        logging.info('Using basic auth.')
        auth = ServiceToken(token);

        if method == 'GET':
            request_func = requests.get
            url = url + run_id
        elif method == 'POST':
            request_func = requests.post
        else:
            raise AirflowException('Unexpected HTTP Method: ' + method)

        for attempt_num in range(1, self.retry_limit+1):
            try:
                logging.info('Final URL :: '+url)

                response = request_func(
                    url,
                    json=json,
                    auth=auth,
                    headers=User_Headers,
                    timeout=self.timeout_seconds)
                if response.status_code == 201:
                    return response.json()
                elif response.status_code == 200:
                    return response.json()
                else:
                    raise AirflowException('Response: {0}, Status Code: {1}'.format(
                        response.content, response.status_code))
            except (requests_exceptions.ConnectionError,
                    requests_exceptions.Timeout) as e:
                logging.info(
                    'Attempt %s API Request to APPLICATION Gateway failed with reason: %s',
                    attempt_num, e
                )
        raise AirflowException(('API requests to APPLICATION Gateway failed {} times. ' +
                               'Giving up.').format(self.retry_limit))

    def execute_spark_job(self, token, json):
        run_id = None
        method = 'POST'
        response = self.submit_batch_job(method, token, json, run_id)
        logging.info(response)
        return response['id']

    def spark_job_state(self, run_id, token, json):
        json = {'json': json}
        method = 'GET'
        response = self.submit_batch_job(method, token, json, run_id)

        logging.info('Spark Job Response : %s', response);
        result_state = response['status']
        logging.info('Spark Job status : %s', result_state);

        return CGJobRunState(result_state)

APPLICATION_GATEWAY_JOB_STATES = [
    'TASK_STARTING',
    'TASK_RUNNING',
    'TASK_STAGING',
    'TASK_FINISHED',
    'TASK_FAILED',
    'TASK_KILLED'
]

class CGJobRunState:
    """
    Utility class for the run state concept of APPLICATION Gateway Job runs.
    """
    def __init__(self, result_state):
        self.result_state = result_state

    @property
    def is_terminated(self):
        if self.result_state not in APPLICATION_GATEWAY_JOB_STATES:
            raise AirflowException(('Some problem happened while running the spark job. Please check with APPLICATION Gateway Team').format(
                                self.result_state))
        return self.result_state in ('TASK_KILLED', 'TASK_FAILED')

    @property
    def is_finished(self):
        return self.result_state == 'TASK_FINISHED'

    def __eq__(self, other):
        return self.result_state == other.result_state

class ServiceToken(AuthBase):

    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        logging.info('Token in Auth is : %s',self.token);
        r.headers['Authorization'] = 'Bearer ' + self.token
        return r
