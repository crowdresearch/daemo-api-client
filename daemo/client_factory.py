import logging

from autobahn.twisted import WebSocketClientFactory
from twisted.internet.protocol import ReconnectingClientFactory

log = logging.getLogger("daemo.client")


class ClientFactory(WebSocketClientFactory, ReconnectingClientFactory):
    maxRetries = 5
    maxDelay = 300  # 5 min

    def clientConnectionFailed(self, connector, reason):
        log.warning("websocket connection failed.")
        # log.warning(reason.value)

        self.retryConnection(connector, reason, True)

    def clientConnectionLost(self, connector, reason):
        log.warning("websocket connection lost.")

        self.retryConnection(connector, reason, False)

    def retryConnection(self, connector, reason, fail=False):
        if self.continueTrying and self.retries < self.maxRetries:
            self.connector = connector
            log.info("connecting again (%d/%d)..." % (self.retries + 1, self.maxRetries))
            self.retry(self.connector)
        else:
            self.stop(connector, fail)

    def stop(self, connector, fail):
        connector.disconnect()

        if self.queue is not None:
            self.queue.put(None)
            self.queue = None

        if fail:
            connector.reactor.stop()
