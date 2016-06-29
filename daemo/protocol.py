import json
import logging

from autobahn.twisted.websocket import WebSocketClientProtocol

from daemo.errors import Error


class ClientProtocol(WebSocketClientProtocol):
    def onConnect(self, response):
        logging.debug("### channel connected ###")

    def onOpen(self):
        logging.debug("### channel opened ###")

        assert hasattr(self.factory, 'client') and self.factory.client is not None, \
            Error.required('client')
        assert hasattr(self.factory, 'approve') and self.factory.approve is not None, \
            Error.required('approve')
        assert hasattr(self.factory, 'completed') and self.factory.completed is not None, \
            Error.required('completed')

    def onMessage(self, payload, isBinary):
        if not isBinary:
            logging.debug("<: {}".format(payload.decode("utf8")))
        self.processMessage(payload, isBinary)

    def processMessage(self, payload, isBinary):
        if not isBinary:
            response = json.loads(payload.decode('utf8'))

            taskworker_id = int(response.get('taskworker_id', 0))
            project_id = int(response.get('project_id', 0))

            assert taskworker_id > 0, Error.required('taskworker_id')
            assert project_id > 0, Error.required('project_id')

            if project_id == self.factory.client.project_id:
                task = self.factory.client.fetch_task(taskworker_id)
                task_data = task.json()

                if task is not None:
                    task_data['accept'] = False

                    if self.factory.approve(task_data):
                        task_data['accept'] = True
                        self.factory.completed(task_data)

                    self.factory.client.update_status(task_data)

    def onSend(self, data):
        self.sendMessage(data.encode("utf8"))
        logging.debug(">: {}".format(data))

    def onClose(self, wasClean, code, reason):
        logging.debug("### channel closed ###")
        logging.debug(reason)
