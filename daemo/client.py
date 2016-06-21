import logging
from inspect import isfunction

import requests
import sys
import websocket

import daemo
from daemo.errors import Error
from daemo.exceptions import AuthException

__version__ = daemo.__version__
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.DEBUG)


class Client:
    def __init__(self, client_id, access_token, refresh_token, project_id):
        # assert username is not None and len(username) > 0, Error.required('username')
        # assert password is not None and len(password) > 0, Error.required('password')
        assert client_id is not None and len(client_id) > 0, Error.required('client_id')
        assert access_token is not None and len(access_token) > 0, Error.required('access_token')
        assert refresh_token is not None and len(refresh_token) > 0, Error.required('refresh_token')
        assert project_id is not None and project_id > 0, Error.required('project_id')

        self.host = daemo.HOST

        # self.username = username
        # self.password = password

        self.client_id = client_id
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.project_id = project_id

        self.channel = None
        self.stream = True

        self.session = requests.session()

        self.authenticate()

    def authenticate(self):

        # self.session_login()
        self.refresh_oauth_token()
        self.channel = 'XXXXXXXXXX'

    # def session_login(self):
    #     data = {
    #         'username': self.username,
    #         'password': self.password
    #     }
    #
    #     response = self._post('/api/auth/login/', data=data)
    #
    #     if 'error' in response:
    #         raise AuthException("Error refreshing access token. Please retry again.")
    #     else:
    #         print response
    #         print response.json()
    #         self.access_token = response.get('access_token')
    #         self.refresh_token = response.get('refresh_token')

    def refresh_oauth_token(self):
        print self.access_token
        print self.refresh_token

        data = {
            'client_id': self.client_id,
            'grant_type': 'refresh_token',
            'refresh_token': self.refresh_token
        }

        response = self._post('/api/oauth2-ng/token/', data=data)

        if 'error' in response:
            raise AuthException("Error refreshing access token. Please retry again.")
        else:
            self.access_token = response.get('access_token')
            self.refresh_token = response.get('refresh_token')

            print self.access_token
            print self.refresh_token

    def publish(self, approve, completed, stream=True):
        assert isfunction(approve), Error.func_def_undefined('approve')
        assert isfunction(completed), Error.func_def_undefined('callback')
        assert self.channel is not None, Error.unauthenticated()

        self.stream = stream

        if self.channel is not None:
            self._launch(self.project_id, self.stream)

    def status(self, project_id):
        pass

    def is_auth_error(self, response):
        auth_error = "Authentication credentials were not provided."
        return response.get('detail', '') == auth_error

    def _get(self, relative_url, data, headers=None):
        if headers is None:
            headers = dict()
        headers.update({
            daemo.AUTHORIZATION: daemo.TOKEN % self.access_token,
        })

        response = self.session.get(self.host + relative_url, data=data, headers=headers)

        try:
            response = response.json()
            print response
        except Exception as e:
            logging.error(e.message)
            response = response.text

        if self.is_auth_error(response):
            self.refresh_oauth_token()

            headers.update({
                daemo.AUTHORIZATION: daemo.TOKEN % self.access_token
            })

            response = self.session.get(self.host + relative_url, data=data, headers=headers)

            try:
                response = response.json()
                print response
            except Exception as e:
                logging.error(e.message)
                response = response.text

        return response

    def _post(self, relative_url, data, headers=None, is_json=True):
        if headers is None:
            headers = dict()
        headers.update({
            daemo.AUTHORIZATION: daemo.TOKEN % self.access_token,
        })

        if is_json:
            headers.update({
                daemo.CONTENT_TYPE: daemo.CONTENT_JSON
            })
        else:
            headers.update({
                daemo.CONTENT_TYPE: daemo.CONTENT_FORM_URLENCODED
            })

        response = self.session.post(self.host + relative_url, data=data, headers=headers)

        try:
            print response
            response = response.json()
            print response
        except Exception as e:
            logger.error(e.message)
            response = response.text

        if self.is_auth_error(response):
            self.refresh_oauth_token()

            headers.update({
                daemo.AUTHORIZATION: daemo.TOKEN % self.access_token
            })

            response = self.session.post(self.host + relative_url, data=data, headers=headers)

            try:
                print response
                response = response.json()
                print response
            except Exception as e:
                logging.error(e.message)
                response = response.text

        return response

    def _put(self, relative_url, data, headers=None):
        if headers is None:
            headers = dict()
        headers.update({
            daemo.AUTHORIZATION: daemo.TOKEN % self.access_token,
            daemo.CONTENT_TYPE: daemo.CONTENT_JSON
        })

        response = self.session.put(self.host + relative_url, data=data, headers=headers)

        try:
            response = response.json()
        except Exception as e:
            logging.error(e.message)
            response = response.text

        if self.is_auth_error(response):
            self.refresh_oauth_token()

            headers.update({
                daemo.AUTHORIZATION: daemo.TOKEN % self.access_token
            })

            response = self.session.put(self.host + relative_url, data=data, headers=headers)

            try:
                response = response.json()
                print response
            except Exception as e:
                logging.error(e.message)
                response = response.text

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
        assert self.ws is not None, Error.missing_connection()
        self.ws.run_forever()

    def on_open(self, ws):
        print "### channel opened ###"

    def on_message(self, ws, message):
        print message

    def on_data(self, ws, data):
        print data

    def on_error(self, ws, error):
        print error

    def launch_task(self, project_id, data, accept, completion, stream=False):
        self.projects[project_id] = {'data': data, 'accept': accept, 'completion': completion, 'stream': stream}
        if stream:
            r = self.post_request(path='api/project/create-full/', data=data, stream=stream)
            for result in r: #as they come -- spawn subprocess to monitor events
                if result: #check to see if its actually something
                    action = accept(result)
                    if action:
                        completion(result)
                    #post action to take on the task
        else:
            while True:
                results = self.post_request(path='api/project/create-full/', data=data, stream=stream)
                #spawn and monitor as before except here we wait until server closes connection
                for result in results:
                    if result:
                        action = accept(result)
                        #post action to take on the task
            map(results, completion) #something like this

    def on_close(self, ws):
        print "### channel closed ###"
