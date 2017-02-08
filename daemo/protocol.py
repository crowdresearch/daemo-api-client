import logging

from autobahn.twisted.websocket import WebSocketClientProtocol

from daemo.errors import Error

log = logging.getLogger("daemo.client")


class ClientProtocol(WebSocketClientProtocol):
    lost = False

    def connectionMade(self):
        log.info("channel connected")
        self.factory.resetDelay()

    # def onConnect(self, response):
    #     log.info("channel connected")

    def onOpen(self):
        log.info("channel opened")

        assert hasattr(self.factory, "queue") and self.factory.queue is not None, \
            Error.required("queue")

    def onMessage(self, payload, isBinary):
        self.factory.queue.put({
            "payload": payload,
            "isBinary": isBinary
        })

    def onSend(self, data):
        self.sendMessage(data.encode("utf8"))
        log.debug("<<<{}>>>".format(data))

    def onClose(self, wasClean, code, reason):
        log.warning("channel closed")

        if not wasClean:
            log.error(reason.value)

    def connectionLost(self, reason):
        if self.factory.force_close:
            onConnectionLost = self.factory.onConnectionLost

            # do not let callback fire again
            if onConnectionLost is not None:
                self.factory.onConnectionLost = None
                onConnectionLost.callback(self)
