import logging
import threading
import time
from multiprocessing import Process

from autobahn.twisted.websocket import connectWS

from daemo.client_factory import ClientFactory
from daemo.protocol import ClientProtocol

log = logging.getLogger("daemo.client")


class Channel(Process):
    factory = None
    connector = None
    queue = None
    state = 0

    def __init__(self, queue, access_token, url):
        super(Channel, self).__init__()
        self.queue = queue

        headers = {
            "Authorization": "Bearer %s" % access_token
        }

        self.factory = ClientFactory(url, headers=headers)
        self.factory.protocol = ClientProtocol
        self.factory.queue = queue
        self.lock = threading.Lock()

    def get_pid(self):
        return self.pid

    def return_name(self):
        return "%s" % self.name

    def run(self):
        log.debug(msg="opening channel...")

        self.connector = connectWS(self.factory)
        self.state = 1

        self.connector.reactor.run()

    def stop(self):
        log.info(msg="closing channel...")

        if self.connector.reactor.running:
            self.connector.transport.protocol.sendClose(None)

        time.sleep(1)

        with self.lock:
            if self.state > 0:
                self.state = 0
                self.connector.reactor.stop()
