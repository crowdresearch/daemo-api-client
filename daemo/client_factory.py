import logging

from autobahn.twisted import WebSocketClientFactory
from twisted.internet import defer
from twisted.internet.protocol import ReconnectingClientFactory

log = logging.getLogger("daemo.client")


class ClientFactory(WebSocketClientFactory, ReconnectingClientFactory):
    maxRetries = 5

    def clientConnectionFailed(self, connector, reason):
        log.warning("websocket connection failed.")
        log.warning(reason)

        if self.continueTrying > 0:
            log.info("connecting again...")
            self.retry(connector)

    def clientConnectionLost(self, connector, reason):
        super(ClientFactory, self).clientConnectionLost(connector, reason)
        log.warning("websocket connection lost.")
        log.warning(reason)

        if self.continueTrying > 0:
            log.info("connecting again...")
            self.retry(connector)
