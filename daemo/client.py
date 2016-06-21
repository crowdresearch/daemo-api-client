from inspect import isfunction

import requests
import websocket

import daemo
from daemo.errors import Error

__version__ = daemo.__version__
error = Error()


class Client:
    def __init__(self, access_token, secret, refresh_token, project_id):
        assert access_token is not None and len(access_token) > 0, error.required('access_token')
        assert refresh_token is not None and len(refresh_token) > 0, error.required('refresh_token')
        assert project_id is not None and len(project_id) > 0, error.required('project_id')

        self.host = daemo.HOST
        self.access_token = access_token
        self.secret = secret
        self.refresh_token = refresh_token
        self.project_id = project_id
        self.channel = None
        self.stream = True

        self.session = requests.session()

        self.authenticate()

    def authenticate(self):
        self.channel = 'XXXXXXXXXX'

    def publish(self, approve, completed, stream=True):
        assert isfunction(approve), error.func_def_undefined('approve')
        assert isfunction(completed), error.func_def_undefined('callback')
        assert self.channel is not None, error.unauthenticated()

        self.stream = stream

        if self.channel is not None:
            self._launch(self.project_id, self.stream)

    def status(self, project_id):
        pass

    def _get(self, relative_url, data, headers):
        headers = {} if headers is None else headers
        headers.update({
            daemo.AUTHORIZATION: daemo.TOKEN % self.access_token,
        })
        response = self.session.get(self.host + relative_url, data=data, headers=headers)
        return response

    def _post(self, relative_url, data, headers):
        headers = {} if headers is None else headers
        headers.update({
            daemo.AUTHORIZATION: daemo.TOKEN % self.access_token,
            daemo.CONTENT_TYPE: daemo.CONTENT_JSON
        })
        response = self.session.post(self.host + relative_url, data=data, headers=headers)
        return response

    def _put(self, relative_url, data, headers):
        headers = {} if headers is None else headers
        headers.update({
            daemo.AUTHORIZATION: daemo.TOKEN % self.access_token,
            daemo.CONTENT_TYPE: daemo.CONTENT_JSON
        })
        response = self.session.put(self.host + relative_url, data=data, headers=headers)
        return response

    def _launch(self, project_id, stream):
        self._create_websocket(project_id)

    def _create_websocket(self, project_id):
        websocket.enableTrace(True)

        self.ws = websocket.WebSocketApp(
            self.host,
            on_open=self.on_open,
            on_message=self.on_message,
            on_data=self.on_data,
            on_error=self.on_error,
            on_close=self.on_close
        )
        self._wait_for_results()

    def _wait_for_results(self):
        assert self.ws is not None, error.missing_connection()
        self.ws.run_forever()

    def on_open(self, ws):
        print "### channel opened ###"

    def on_message(self, ws, message):
        print message

    def on_data(self, ws, data):
        print data

    def on_error(self, ws, error):
        print error

    def on_close(self, ws):
        print "### channel closed ###"
