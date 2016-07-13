import fcntl
import json
import logging
import multiprocessing
import os
import signal
import sys
import threading
import time
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
    client_id = None
    access_token = None
    refresh_token = None
    projects = []
    cache = []
    aggregated_data = []

    def __init__(self, credentials_path):
        assert credentials_path is not None and len(credentials_path) > 0, Error.required("credentials path")
        self.credentials_path = credentials_path

        self.host = daemo.HOST

        self.ws_process = None
        self.project_id = None
        self.stream = False

        if self._credentials_exist():
            self._load_tokens()
        else:
            self._persist_tokens()

        self.session = requests.session()
        self._refresh_token()

        self.register_signals()

        self.connect()

    def register_signals(self):
        thread = threading.Thread(target=signal.pause)
        thread.start()

    def connect(self):
        signal.signal(signal.SIGINT, self.handler)

        self.ws_process = multiprocessing.Process(
            target=self._create_websocket,
            kwargs=dict(access_token=self.access_token, host=self.host, client=self)
        )
        self.ws_process.start()

    def publish(self, project_id, tasks, approve, completed, stream):
        assert project_id is not None and project_id > 0, Error.required(PROJECT_ID)
        assert isfunction(approve), Error.func_def_undefined(APPROVE)
        assert isfunction(completed), Error.func_def_undefined(CALLBACK)
        assert stream is not None, Error.required(STREAM)

        thread = threading.Thread(
            target=self._publish,
            kwargs=dict(
                project_id=project_id,
                tasks=tasks,
                approve=approve, completed=completed,
                stream=stream
            )
        )
        thread.start()

    def _publish(self, project_id, tasks, approve, completed, stream):
        self.project_id = project_id
        self.stream = stream

        self._publish_project(project_id)

        for task in tasks:
            task['identifier'] = int(time.time())
            self.add_data(project_id=project_id, data=task, approve=approve, completed=completed, stream=stream)

    def _publish_project(self, project_id):
        response = self._post('/api/project/%d/publish/' % project_id, data=json.dumps({}))
        response.raise_for_status()

        # todo: should update not append
        self.projects.append(project_id)

    def add_data(self, project_id, data, approve, completed, stream):
        response = self._post('/api/project/%d/add-data/' % project_id, data=json.dumps({"tasks": [data]}))
        response.raise_for_status()

        tasks = response.json()

        for task in tasks:
            # todo: should update not append
            self.cache.append({
                'project_id': task['project'],
                'task_id': task['id'],
                'approve': approve,
                'completed': completed,
                'stream': stream
            })

        return response

    def aggregate(self, project_id,task_id, task_data):
        self.aggregated_data.append({
            'project_id': project_id,
            'task_id': task_id,
            'task_data': task_data
        })

    def remove_project(self, project_id):
        self.projects.remove(project_id)

    def is_complete(self):
        return len(self.projects) == 0

    def get_cached_task_detail(self, project_id, task_id):
        matched = [x for x in self.cache if x['project_id'] == project_id]  # and x['task_id'] == task_id]
        return matched

    def fetch_task(self, taskworker_id):
        response = self._get('/api/task-worker/%d/' % taskworker_id, data={})
        response.raise_for_status()
        return response

    def update_status(self, task):
        data = {
            'status': STATUS_ACCEPTED if task['accept'] else STATUS_REJECTED,
            'workers': [task['id']]
        }

        response = self._post('/api/task-worker/bulk-update-status/', data=json.dumps(data))
        return response

    def fetch_status(self, project_id):
        response = self._get('/api/project/%d/is-done/' % project_id, data={})
        response.raise_for_status()

        project_data = response.json()
        is_done = project_data.get('is_done')
        return is_done

    def is_auth_error(self, response):
        try:
            response = response.json()
        except Exception as e:
            pass

        auth_error = CREDENTIALS_NOT_PROVIDED

        return response is not None and isinstance(response, dict) and response.get("detail", "") == auth_error

    def _credentials_exist(self):
        return os.path.isfile(self.credentials_path)

    def _load_tokens(self):
        with open(self.credentials_path, READ_ONLY) as infile:
            fcntl.flock(infile.fileno(), fcntl.LOCK_EX)

            data = json.load(infile)

            assert data[CLIENT_ID] is not None and len(data[CLIENT_ID]) > 0, Error.required(CLIENT_ID)
            assert data[ACCESS_TOKEN] is not None and len(data[ACCESS_TOKEN]) > 0, Error.required(ACCESS_TOKEN)
            assert data[REFRESH_TOKEN] is not None and len(data[REFRESH_TOKEN]) > 0, Error.required(REFRESH_TOKEN)

            self.client_id = data[CLIENT_ID]
            self.access_token = data[ACCESS_TOKEN]
            self.refresh_token = data[REFRESH_TOKEN]

    def _persist_tokens(self):
        with open(self.credentials_path, WRITE_ONLY) as outfile:
            fcntl.flock(outfile.fileno(), fcntl.LOCK_EX)

            data = {
                CLIENT_ID: self.client_id,
                ACCESS_TOKEN: self.access_token,
                REFRESH_TOKEN: self.refresh_token
            }
            json.dump(data, outfile)

    def _refresh_token(self):
        self._load_tokens()

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

    def _create_websocket(self, access_token, host, client):
        headers = {
            AUTHORIZATION: TOKEN % access_token
        }

        self.ws = WebSocketClientFactory(daemo.WEBSOCKET + host + daemo.WS_BOT_SUBSCRIBE_URL, headers=headers)
        self.ws.protocol = ClientProtocol
        self.ws.client = client
        connectWS(self.ws)
        reactor.run()

    def mark_completed(self):
        reactor.callFromThread(reactor.stop)

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

    def handler(self, signum, frame):
        if signum in [signal.SIGINT, signal.SIGTERM, signal.SIGABRT]:
            if reactor.running:
                reactor.callFromThread(reactor.stop)
            else:
                sys.exit(0)
