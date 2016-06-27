import logging
from inspect import isfunction

import requests
from autobahn.twisted.websocket import WebSocketClientFactory, connectWS
from twisted.internet import reactor

import daemo
from daemo.errors import Error
from daemo.exceptions import AuthException
from daemo.protocol import ClientProtocol

STREAM = 'stream'
WRITE_ONLY = 'w'
READ_ONLY = 'r'
CALLBACK = "completed"
APPROVE = "approve"
CREDENTIALS = '.credentials'
GRANT_TYPE = "grant_type"
PROJECT_ID = "project_id"
REFRESH_TOKEN = "refresh_token"
ACCESS_TOKEN = "access_token"
CLIENT_ID = "client_id"
CREDENTIALS_NOT_PROVIDED = "Authentication credentials were not provided."
CONTENT_JSON = "application/json"
CONTENT_FORM_URLENCODED = "application/x-www-form-urlencoded"
TOKEN = "Bearer %s"
AUTHORIZATION = "Authorization"
CONTENT_TYPE = "Content-Type"
STATUS_ACCEPTED = 3
STATUS_REJECTED = 4

__version__ = daemo.__version__
logger = logging.getLogger(__name__)
fh = logging.FileHandler('.log')
fh.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)
logger.addHandler(fh)


class Client:
    def __init__(self, client_id, access_token, refresh_token):
        assert client_id is not None and len(client_id) > 0, Error.required(CLIENT_ID)
        assert access_token is not None and len(access_token) > 0, Error.required(ACCESS_TOKEN)
        assert refresh_token is not None and len(refresh_token) > 0, Error.required(REFRESH_TOKEN)

        self.host = daemo.HOST

        self.client_id = client_id
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.project_id = None
        self.stream = False

        if self._credentials_exist():
            self._load_tokens()
        else:
            self._persist_tokens()

        self.session = requests.session()
        self.authenticate()

    def authenticate(self):
        self._refresh_token()

    def publish(self, project_id, approve, completed, stream):
        assert project_id is not None and project_id > 0, Error.required(PROJECT_ID)
        assert isfunction(approve), Error.func_def_undefined(APPROVE)
        assert isfunction(completed), Error.func_def_undefined(CALLBACK)
        assert stream is not None, Error.required(STREAM)

        self.project_id = project_id
        self.stream = stream
        self._launch(project_id, approve, completed, stream)

    def add_data(self, project_id, data):
        response = self._post('/api/project/%d/add-data/' % project_id, data=data)
        response.raise_for_status()
        return response

    def fetch_task(self, taskworker_id):
        response = self._get('/api/task-worker/%d/' % taskworker_id, data={})
        response.raise_for_status()
        return response

    def update_status(self, task):
        data = {
            'status': STATUS_ACCEPTED if task['accept'] else STATUS_REJECTED,
            'workers': [task['id']]
        }

        response = self._post('/api/task-worker/%d/bulk-update-status', data)
        response.raise_for_status()

        return response

    def fetch_status(self, project_id):
        # todo: get project status on pending and completed tasks
        response = self._post()
        response.raise_for_status()
        return response

    def is_auth_error(self, response):
        try:
            response = response.json()
        except Exception as e:
            logger.error(e.message)

        auth_error = CREDENTIALS_NOT_PROVIDED
        return response is not None and response.get("detail", "") == auth_error

    def _credentials_exist(self):
        import os
        return os.path.isfile(CREDENTIALS)

    def _load_tokens(self):
        import json
        with open(CREDENTIALS, READ_ONLY) as infile:
            data = json.load(infile)

            self.client_id = data[CLIENT_ID]
            self.access_token = data[ACCESS_TOKEN]
            self.refresh_token = data[REFRESH_TOKEN]
        infile.close()

    def _persist_tokens(self):
        import json
        with open(CREDENTIALS, WRITE_ONLY) as outfile:
            data = {
                CLIENT_ID: self.client_id,
                ACCESS_TOKEN: self.access_token,
                REFRESH_TOKEN: self.refresh_token
            }
            json.dump(data, outfile)

        outfile.close()

    def _refresh_token(self):
        data = {
            CLIENT_ID: self.client_id,
            GRANT_TYPE: REFRESH_TOKEN,
            REFRESH_TOKEN: self.refresh_token
        }

        response = self._post(daemo.OAUTH_TOKEN_URL, data=data, is_json=False, authorization=False)

        if "error" in response.json():
            raise AuthException("Error refreshing access token. Please retry again.")
        else:
            response = response.json()
            self.access_token = response.get(ACCESS_TOKEN)
            self.refresh_token = response.get(REFRESH_TOKEN)

            self._persist_tokens()

    def _launch(self, project_id, approve, completed, stream):
        self._create_websocket(project_id, approve, completed, stream)

    def _create_websocket(self, project_id, approve, completed, stream):
        headers = {
            AUTHORIZATION: TOKEN % self.access_token
        }

        self.ws = WebSocketClientFactory(daemo.WEBSOCKET + self.host + daemo.WS_BOT_SUBSCRIBE_URL, headers=headers)
        self.ws.protocol = ClientProtocol

        self.ws.project_id = project_id
        self.ws.approve = approve
        self.ws.completed = completed
        self.ws.stream = stream
        self.ws.client = self

        self._wait_for_results()

    def _wait_for_results(self):
        assert self.ws is not None, Error.missing_connection()
        connectWS(self.ws)
        reactor.run()

    def _get(self, relative_url, data, headers=None, is_json=True, authorization=True):
        if headers is None:
            headers = dict()

        if authorization:
            headers.update({
                AUTHORIZATION: TOKEN % self.access_token,
            })

        if is_json:
            headers.update({
                CONTENT_TYPE: CONTENT_JSON
            })

        response = self.session.get(daemo.HTTP + self.host + relative_url, data=data, headers=headers)

        if self.is_auth_error(response):
            self._refresh_token()

            if authorization:
                headers.update({
                    AUTHORIZATION: TOKEN % self.access_token
                })

            response = self.session.get(daemo.HTTP + self.host + relative_url, data=data, headers=headers)

        return response

    def _post(self, relative_url, data, headers=None, is_json=True, authorization=True):
        if headers is None:
            headers = dict()

        if authorization:
            headers.update({
                AUTHORIZATION: TOKEN % self.access_token,
            })

        if is_json:
            headers.update({
                CONTENT_TYPE: CONTENT_JSON
            })

        response = self.session.post(daemo.HTTP + self.host + relative_url, data=data, headers=headers)

        if self.is_auth_error(response):
            self._refresh_token()

            if authorization:
                headers.update({
                    AUTHORIZATION: TOKEN % self.access_token
                })

            response = self.session.post(daemo.HTTP + self.host + relative_url, data=data, headers=headers)

        return response

    def _put(self, relative_url, data, headers=None, is_json=True, authorization=True):
        if headers is None:
            headers = dict()

        if authorization:
            headers.update({
                AUTHORIZATION: TOKEN % self.access_token,
            })

        if is_json:
            headers.update({
                CONTENT_TYPE: CONTENT_JSON
            })

        response = self.session.put(daemo.HTTP + self.host + relative_url, data=data, headers=headers)

        if self.is_auth_error(response):
            self._refresh_token()

            if authorization:
                headers.update({
                    AUTHORIZATION: TOKEN % self.access_token
                })

            response = self.session.put(daemo.HTTP + self.host + relative_url, data=data, headers=headers)

        return response
