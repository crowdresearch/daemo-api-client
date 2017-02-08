import logging
import threading
import time
from multiprocessing import Process

from autobahn.twisted.websocket import connectWS
from twisted.internet import defer

from daemo.client_factory import ClientFactory
from daemo.protocol import ClientProtocol

log = logging.getLogger("daemo.client")


class Channel(Process):
    factory = None
    connector = None
    queue = None
    url = None
    api_client = None
    state = 0

    def __init__(self, queue, api_client, url):
        super(Channel, self).__init__()
        log.debug(msg="initiating channel...")

        self.queue = queue
        self.url = url
        self.api_client = api_client
        self.lock = threading.Lock()
        # defer.setDebugging(True)

        self.clientDisconnected = defer.Deferred()

        access_token = self.api_client.get_auth_token()

        headers = {
            "Authorization": "Bearer %s" % access_token
        }

        self.factory = ClientFactory(self.url, headers=headers)

        self.factory.force_close = False
        self.factory.protocol = ClientProtocol
        self.factory.queue = self.queue

    def return_name(self):
        return "%s" % self.name

    def run(self):
        log.debug(msg="opening channel...")

        if self.state == 0:
            self.state = 1

            self.factory.onConnectionLost = self.clientDisconnected
            self.clientDisconnected.addCallback(self.on_client_disconnected)

            self.connector = connectWS(self.factory)
            self.connector.reactor.run()

    def stop(self, forced_closure):
        if forced_closure:
            self.factory.force_close = forced_closure
            self.factory.stopTrying()
            self.connector.disconnect()

        return defer.gatherResults([self.clientDisconnected])

    def on_client_disconnected(self, protocol):
        if not (self.factory.continueTrying > 0):
            time.sleep(2)

            with self.lock:
                if self.state > 0:
                    self.state = 0
                    self.connector.reactor.stop()
        # else:
        #     log.warning("continue trying")
