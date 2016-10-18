import logging.config

import requests

from daemo.auth import Auth
from daemo.utils import log_exit

log = logging.getLogger("daemo.client")

TOKEN = "Bearer %s"
AUTHORIZATION = "Authorization"
CONTENT_JSON = "application/json"
CONTENT_TYPE = "Content-Type"


class RestClient:
    auth = None

    def __init__(self, credentials_path, host, http_proto):
        self.auth = Auth(credentials_path, host, http_proto, self)
        self.auth.initialize()

    def get_headers(self, headers, authorization, is_json, access_token):
        if headers is None:
            headers = dict()

        if authorization:
            headers.update({
                AUTHORIZATION: TOKEN % access_token,
            })

        if is_json:
            headers.update({
                CONTENT_TYPE: CONTENT_JSON
            })

        return headers

    def get(self, relative_url, data=None, headers=None, is_json=True, authorization=True):
        session = requests.session()
        base_url = self.auth.http_proto + self.auth.host

        headers = self.get_headers(
            headers=headers,
            authorization=authorization,
            is_json=is_json,
            access_token=self.auth.access_token)

        response = None
        try:
            response = session.get(base_url + relative_url, data=data, headers=headers)
        except Exception, e:
            log_exit(e)

        if self.auth.is_auth_error(response):
            self.auth.refresh_tokens()

            if authorization:
                headers.update({
                    AUTHORIZATION: TOKEN % self.auth.access_token
                })

            response = session.get(base_url + relative_url, data=data, headers=headers)

        return response

    def post(self, relative_url, data, headers=None, is_json=True, authorization=True):
        session = requests.session()
        base_url = self.auth.http_proto + self.auth.host

        headers = self.get_headers(
            headers=headers,
            authorization=authorization,
            is_json=is_json,
            access_token=self.auth.access_token)

        response = None
        try:
            response = session.post(base_url + relative_url, data=data, headers=headers)
        except Exception, e:
            log_exit(e)

        if self.auth.is_auth_error(response):
            self.auth.refresh_tokens()

            if authorization:
                headers.update({
                    AUTHORIZATION: TOKEN % self.auth.access_token
                })

            response = session.post(base_url + relative_url, data=data, headers=headers)

        return response

    def put(self, relative_url, data, headers=None, is_json=True, authorization=True):
        session = requests.session()

        base_url = self.auth.http_proto + self.auth.host

        headers = self.get_headers(
            headers=headers,
            authorization=authorization,
            is_json=is_json,
            access_token=self.auth.access_token)

        response = None
        try:
            response = session.put(base_url + relative_url, data=data, headers=headers)
        except Exception, e:
            log_exit(e)

        if self.auth.is_auth_error(response):
            self.auth.refresh_tokens()

            if authorization:
                headers.update({
                    AUTHORIZATION: TOKEN % self.auth.access_token
                })

            response = session.put(base_url + relative_url, data=data, headers=headers)

        return response
