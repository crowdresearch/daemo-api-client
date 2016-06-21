import requests

from twisted.internet import reactor
from autobahn.twisted.websocket import WebSocketClientFactory, \
    WebSocketClientProtocol, connectWS


class DaemoClientProtocol(WebSocketClientProtocol):
    def onConnect(self, response):
        print(response)

    def send_data(self, data):
        self.sendMessage(data.encode('utf8'))

    def onOpen(self):
        pass

    def onMessage(self, payload, isBinary):
        if not isBinary:
            print("Recv: {}".format(payload.decode('utf8')))


class DaemoClient:
    def __init__(self, host, access_token):
        self.host = host
        self.access_token = access_token
        self.session = requests.session()
        headers = {'Authorization': 'Bearer {}'.format(self.access_token)}
        factory = WebSocketClientFactory('ws://' + self.host + '/ws/api-client?subscribe-user', headers=headers)
        factory.protocol = DaemoClientProtocol
        connectWS(factory)
        reactor.run()

    def post_request(self, path, data):
        headers = {"Authorization": "Bearer " + self.access_token, "Content-Type": 'application/json'}
        response = self.session.post(self.host + path, data=data, headers=headers)
        return response

    def get_request(self, path):
        self.session.headers.update({"Authorization": "Bearer " + self.access_token})
        response = self.session.get(url=self.host + path)
        return response

    def create_project(self, data):
        return self.post_request(path='api/project/create-full/', data=data)

    def get_projects(self):
        return self.get_request('api/project/requester_projects/')

    def get_worker(self, daemo_id):
        return self.get_request('api/worker/list-using-daemo-id/?daemo_id=' + str(daemo_id))
