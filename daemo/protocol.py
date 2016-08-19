import logging

from autobahn.twisted.websocket import WebSocketClientProtocol

from daemo.errors import Error


class ClientProtocol(WebSocketClientProtocol):
    def onConnect(self, response):
        logging.debug("### channel connected ###")

    def onOpen(self):
        logging.debug("### channel opened ###")

        assert hasattr(self.factory, "queue") and self.factory.queue is not None, \
            Error.required("queue")

    def onMessage(self, payload, isBinary):
        self.factory.queue.put({
            "payload": payload,
            "isBinary": isBinary
        })

    def onSend(self, data):
        self.sendMessage(data.encode("utf8"))
        logging.debug("<<<{}>>>".format(data))

    def onClose(self, wasClean, code, reason):
        logging.debug("### channel closed ###")
        logging.debug(reason)
