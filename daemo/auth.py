import fcntl
import json
import logging.config
import os

from daemo.errors import Error
from daemo.exceptions import ServerException
from daemo.utils import check_dependency

log = logging.getLogger("daemo.client")

AUTHORIZATION = "Authorization"
TOKEN = "Bearer %s"
GRANT_TYPE = "grant_type"
REFRESH_TOKEN = "refresh_token"
ACCESS_TOKEN = "access_token"
CLIENT_ID = "client_id"
CREDENTIALS_NOT_PROVIDED = "Authentication credentials were not provided."
REFRESH_TOKEN_FAILED = "Error refreshing access token. Please retry again."
AUTH_TOKEN_URL = "/api/oauth2-ng/token/"


class Auth:
    client = None
    client_id = None
    access_token = None
    refresh_token = None
    credentials_path = None
    host = None
    http_proto = None

    def __init__(self, credentials_path, host, http_proto, client):
        self.client = client
        self.credentials_path = credentials_path
        self.host = host
        self.http_proto = http_proto

    def initialize(self):
        if self.credentials_exist():
            self.load_tokens()
        else:
            self.persist_tokens()

        self.refresh_tokens()

    def credentials_exist(self):
        return os.path.isfile(self.credentials_path)

    def load_tokens(self):
        with open(self.credentials_path, "r") as infile:
            data = json.load(infile)

            check_dependency(data[CLIENT_ID] is not None and len(data[CLIENT_ID]) > 0, Error.required(CLIENT_ID))
            check_dependency(data[ACCESS_TOKEN] is not None and len(data[ACCESS_TOKEN]) > 0,
                             Error.required(ACCESS_TOKEN))
            check_dependency(data[REFRESH_TOKEN] is not None and len(data[REFRESH_TOKEN]) > 0,
                             Error.required(REFRESH_TOKEN))

            self.client_id = data[CLIENT_ID]
            self.access_token = data[ACCESS_TOKEN]
            self.refresh_token = data[REFRESH_TOKEN]

    def persist_tokens(self):
        with open(self.credentials_path, "w") as outfile:
            fcntl.flock(outfile.fileno(), fcntl.LOCK_EX)

            data = {
                CLIENT_ID: self.client_id,
                ACCESS_TOKEN: self.access_token,
                REFRESH_TOKEN: self.refresh_token
            }

            json.dump(data, outfile)

    def refresh_tokens(self):
        self.load_tokens()

        data = {
            CLIENT_ID: self.client_id,
            GRANT_TYPE: REFRESH_TOKEN,
            REFRESH_TOKEN: self.refresh_token
        }

        auth_response = self.client.post(AUTH_TOKEN_URL, data=data, is_json=False, authorization=False)
        response = auth_response.json()

        if "error" in response:
            raise ServerException("auth", REFRESH_TOKEN_FAILED, 400)

        check_dependency(response[ACCESS_TOKEN] is not None and len(response[ACCESS_TOKEN]) > 0,
                         Error.required(ACCESS_TOKEN))
        check_dependency(response[REFRESH_TOKEN] is not None and len(response[REFRESH_TOKEN]) > 0,
                         Error.required(
                             REFRESH_TOKEN))

        self.access_token = response.get(ACCESS_TOKEN)
        self.refresh_token = response.get(REFRESH_TOKEN)

        self.persist_tokens()

    def get_auth_token(self):
        return self.access_token

    def is_auth_error(self, response):
        try:
            response = response.json()
        except Exception as e:
            pass

        return response is not None \
               and isinstance(response, dict) \
               and response.get("detail", "") == CREDENTIALS_NOT_PROVIDED
