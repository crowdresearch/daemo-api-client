import requests


class DaemoClient():
    def __init__(self, host, access_token):
        self.host = host
        self.access_token = access_token
        self.session = requests.session()

    def request(self, path, data):
        headers = {"Authorization": "Bearer " + self.access_token, "Content-Type": 'application/json'}
        response = self.session.post(self.host + path, data=data, headers=headers)
        return response

    def create_project(self, data):
        return self.request(path='api/project/create-full/', data=data)