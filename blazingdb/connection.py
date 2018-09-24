# coding=utf-8

from collections import namedtuple
import requests


ConnectionStatus = namedtuple(
    'ConnectionStatus', ['valid', 'message', 'access_token']
)


class Connection:
    def __init__(self, url, username, password, database=''):
        # mandatory parameters
        self.url = url
        self.username = username
        self.password = password

        # optional parameters
        self.database = database


class Connector:
    def __init__(self, connection):
        self.connection = connection

    def connect(self):
        url = self.connection.url
        username = self.connection.username
        password = self.connection.password
        database = self.connection.database

        try:
            access_token = _http_register(url, username, password)

            if access_token == 'fail':
                error_message = 'Your username or password is incorrect'
                return ConnectionStatus(False, error_message, '')
        except Exception as e:
            error_message = 'The host you entered is unreachable or your credentials are incorrect'
            return ConnectionStatus(False, error_message, '')

        if database:
            try:
                query = 'use database ' + database
                result_key = _http_query(query, url, username, access_token)

                if result_key == 'fail':
                    return ConnectionStatus(False, result_key, access_token)
            except Exception as e:
                error_message = 'The server could not execute use database ' + database
                return ConnectionStatus(False, error_message, access_token)

        return ConnectionStatus(True, '', access_token)

    def run(self, sql, connection_status):
        """" arrow bytes """

        url = self.connection.url
        username = self.connection.username

        is_invalid_status = (connection_status.valid == False)
        has_invalid_access_token = (not connection_status.access_token)

        if is_invalid_status or has_invalid_access_token:
            error = 'Invalid connection status: ' + str(connection_status)
            raise ValueError(error)

        access_token = connection_status.access_token

        result_key = _http_query(sql, url, username, access_token)

        if result_key == 'fail':
            # TODO better error message ...
            raise RuntimeError(result_key)

        arrow_bytes = _http_get_results(
            url, username, access_token, result_key)

        return arrow_bytes


def _http_register(url, username, password):
    final_url = url + '/blazing-jdbc/register'
    request_data = {'username': username, 'password': password}
    response = requests.post(final_url, data=request_data, verify=False)
    access_token = response.text
    return access_token


def _http_query(query, url, username, access_token):
    final_url = url + '/blazing-jdbc/query'
    request_data = {
        'username': username,
        'token': access_token,
        'query': query
    }
    response = requests.post(final_url, data=request_data, verify=False)
    result_key = response.text
    return result_key


def _http_get_results(url, username, access_token, result_key):
    final_url = url + '/blazing-jdbc/get-results'

    request_data = {
        'username': username,
        'token': access_token,
        'resultSetToken': result_key
    }

    request_headers = {'Content-Type': 'application/octet-stream'}

    response = requests.get(
        final_url, headers=request_headers, params=request_data, stream=True, verify=False)

    arrow_data = response.raw.data

    return arrow_data
