import json
import logging
import threading

from autobahn.twisted.websocket import WebSocketClientProtocol

from daemo.errors import Error


class ClientProtocol(WebSocketClientProtocol):
    def onConnect(self, response):
        logging.debug("### channel connected ###")

    def onOpen(self):
        logging.debug("### channel opened ###")

        assert hasattr(self.factory, 'client') and self.factory.client is not None, \
            Error.required('client')

    def onMessage(self, payload, isBinary):
        if not isBinary:
            logging.debug("<: {}".format(payload.decode("utf8")))

        thread = threading.Thread(target=self.processMessage, kwargs=dict(payload=payload, isBinary=isBinary))
        thread.start()

    def processMessage(self, payload, isBinary):
        if not isBinary:
            response = json.loads(payload.decode('utf8'))

            taskworker_id = int(response.get('taskworker_id', 0))
            task_id = int(response.get('task_id', 0))
            project_id = int(response.get('project_id', 0))
            # task_identifier = int(response.get('task_identifier', 0))

            assert taskworker_id > 0, Error.required('taskworker_id')
            assert task_id > 0, Error.required('task_id')
            assert project_id > 0, Error.required('project_id')
            # assert task_identifier > 0, Error.required('task_identifier')

            task_configs = self.factory.client.get_cached_task_detail(project_id, task_id)

            if task_configs is not None and len(task_configs) > 0:
                task = self.factory.client.fetch_task(taskworker_id)
                task.raise_for_status()

                task_data = task.json()

                if task is not None:
                    task_data['accept'] = False

                    for config in task_configs:
                        approve = config['approve']
                        completed = config['completed']
                        stream = config['stream']

                        if stream:
                            if approve([task_data]):
                                task_data['accept'] = True

                            task_status = self.factory.client.update_status(task_data)
                            task_status.raise_for_status()

                            if task_data['accept']:
                                completed([task_data])

                            is_done = self.factory.client.fetch_status(project_id)

                            if is_done:
                                # remove it from global list of projects
                                self.factory.client.remove_project(project_id)

                        else:
                            # store it for aggregation
                            self.factory.client.aggregate(project_id, task_id, task_data)

                            is_done = self.factory.client.fetch_status(project_id)

                            if is_done:
                                tasks_data = self.factory.client.fetch_aggregated(project_id)

                                approvals = approve(tasks_data)

                                for approval in approvals:
                                    task_data['accept'] = approval

                                    task_status = self.factory.client.update_status(task_data)
                                    task_status.raise_for_status()

                                approved_tasks = [x for x in zip(tasks_data, approvals) if x[1]]
                                completed([approved_tasks])

                                # remove it from global list of projects
                                self.factory.client.remove_project(project_id)

                    if self.factory.client.is_complete():
                        self.factory.client.mark_completed()

    def onSend(self, data):
        self.sendMessage(data.encode("utf8"))
        logging.debug(">: {}".format(data))

    def onClose(self, wasClean, code, reason):
        logging.debug("### channel closed ###")
        logging.debug(reason)
