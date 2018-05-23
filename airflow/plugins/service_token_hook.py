

import requests
import logging
import os
from airflow import __version__
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from requests import exceptions as requests_exceptions
from requests.auth import AuthBase
from airflow import configuration
from os import environ

try:
    from urllib import parse as urlparse
except ImportError:
    import urlparse


Service_Token_EndPoint = ('POST', 'abc/test/v1/')
User_Headers = {'user-agent': 'airflow-{v}'.format(v=__version__), 'accept': 'application/json','Content-Type': 'application/x-www-form-urlencoded'}


class ServiceToken(BaseHook):
    """
    Interact with IMS Gateway.
    """
    def __init__(
            self,
            service_connection_id='default_connection',
            timeout_seconds=180,
            retry_limit=3):
        self.service_connection_id = service_connection_id
        self.service_connection = self.get_connection(service_connection_id)
        self.timeout_seconds = timeout_seconds
        assert retry_limit >= 1, 'Retry limit must be greater than equal to 1'
        self.retry_limit = retry_limit

    def parse_host(self, host):

        urlparse_host = urlparse.urlparse(host).hostname
        if urlparse_host:
            # In this case, host = https://platform-dev.XYZ.io
            return urlparse_host
        else:
            # In this case, host = platform-dev.XYZ.io
            return host

    def get_service_token(self, method, json):

        if method == 'POST':
            request_func = requests.post
            endpoint = self.service_connection.extra_dejson.get('POST_END_POINT', None)
            if endpoint is  None:
                         endpoint = Service_Token_EndPoint
        else:
            raise AirflowException('Unexpected HTTP Method: ' + method)

        url = 'https://{host}/{endpoint}'.format(
            host=self.parse_host(self.service_connection.host),
            endpoint=endpoint)
        logging.info('URL :: '+url)
        logging.info(json)

        for attempt_num in range(1, self.retry_limit+1):
            try:
                if os.getenv("id") is not None:
                     id = os.environ['id']
                else:
                     id = configuration.get('service', 'id')
                if os.getenv("service") is not None:
                     service = os.environ['service']
                else:
                     service = configuration.get('service', 'service')
                if os.getenv("code") is not None:
                     code = os.environ['code']
                else:
                     code = configuration.get('service', 'code')
                if os.getenv("type") is not None:
                     type = os.environ['type']
                else:
                     type = configuration.get('service', 'type')
                logging.info('URL :: '+url)
                logging.info(' id :: '+id)
                logging.info(' service :: '+service)
                logging.info(' code :: '+code)
                logging.info(' type :: '+type)
                query_params= '?type=%s&id=%s&service=%s&code=%s' % (type, id, service, code)
                logging.info('Final query_params :: '+query_params)
                url = url + query_params
                logging.info('Final Appended URL :: '+url)

                response = request_func(
                    url,
                    json=json,
                    headers=User_Headers,
                    timeout=self.timeout_seconds)
                if response.status_code == 200:
                    return response.json()
                else:
                    raise AirflowException('Response: {0}, Status Code: {1}'.format(
                        response.content, response.status_code))
            except (requests_exceptions.ConnectionError,
                    requests_exceptions.Timeout) as e:
                logging.info(
                    'Attempt %s API Request to Query Service failed with reason: %s',
                    attempt_num, e
                )
        raise AirflowException(('API requests to IMS Gateway Service failed {} times. ' +
                               'Giving up.').format(self.retry_limit))

    def execute_service_job(self, json):
        method = 'POST'
        response = self.get_service_token(method, json)
        logging.info(response)
        return response['token']
