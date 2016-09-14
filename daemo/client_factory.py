import logging

from autobahn.twisted import WebSocketClientFactory
from twisted.internet.protocol import ReconnectingClientFactory

from daemo.websockets import ClientProtocol

log = logging.getLogger("daemo.client")


class ClientFactory(WebSocketClientFactory, ReconnectingClientFactory):

    protocol = ClientProtocol

    def clientConnectionFailed(self, connector, reason):
        log.warning("websocket connection failed.")
        log.info("connecting again...")
        self.retry(connector)

    def clientConnectionLost(self, connector, reason):
        log.warning("websocket connection lost.")
        log.info("connecting again...")
        self.retry(connector)
