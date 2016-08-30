import fcntl
import json
import logging
import multiprocessing
import os
import signal
import sys
import threading
from inspect import isfunction

import requests
from autobahn.twisted.websocket import WebSocketClientFactory, connectWS
from twisted.internet import reactor

import daemo
from daemo.errors import Error
from daemo.exceptions import AuthException
from daemo.protocol import ClientProtocol

GRANT_TYPE = "grant_type"
REFRESH_TOKEN = "refresh_token"
ACCESS_TOKEN = "access_token"
CLIENT_ID = "client_id"
CREDENTIALS_NOT_PROVIDED = "Authentication credentials were not provided."
CONTENT_JSON = "application/json"
TOKEN = "Bearer %s"
AUTHORIZATION = "Authorization"
CONTENT_TYPE = "Content-Type"
STATUS_ACCEPTED = 3
STATUS_REJECTED = 4

__version__ = daemo.__version__


class DaemoClient:
    """
    Initializes Daemo Client
        - Authentication with Daemo host server
        - Connect to the host
        - Fetch any submitted worker responses using the rerun_key

    First download the credentials file from your Daemo User Profile. Fill in the RERUN_KEY which is considered incremental number here for each run.
    ::
        CREDENTIALS_FILE = 'credentials.json'
        RERUN_KEY = '0001'

        daemo = DaemoClient(credentials_path=CREDENTIALS_FILE, rerun_key=RERUN_KEY)

    :param credentials_path: path of the daemo credentials file which can be downloaded from daemo user profile (**Menu** >> **Get Credentials**)
    :param host: daemo server to connect to - uses a default server if not defined
    :param rerun_key: a string used to differentiate each script run. If this key is same, it replays the last results from worker responses and brings you to the last point when script stopped execution.
    :param multi_threading: False by default, bool value to enable multi-threaded response handling
    """

    def __init__(self, credentials_path, host=daemo.HOST, rerun_key=None, multi_threading=False):
        logging.debug(msg="initializing client...")
        assert credentials_path is not None and len(credentials_path) > 0, Error.required("credentials_path")

        self.host = host
        self.credentials_path = credentials_path
        self.rerun_key = rerun_key
        self.multi_threading = multi_threading

        self.client_id = None
        self.access_token = None
        self.refresh_token = None

        self.ws_process = None

        self.batches = []
        self.tasks = {}
        self.cache = {}

        self.queue = multiprocessing.Queue()

        self._initialize()

    def publish(self, project_key, tasks, approve, completed, mock_workers=None, stream=False):
        """
        Publishes the project if not already published and creates new tasks based on the tasks list provided

        A typical usage is given below and each of the callbacks are explained further:
        ::
            daemo.publish(
                project_key='k0BXZxVz4P3w',
                tasks=[{
                    "id": id,
                    "tweet": text
                }],
                approve=approve_tweet,
                completed=post_to_twitter
            )

        :param project_key: string key for the project as shown in Daemo's Project Authoring Interface. It is a unique for each project.
        :param tasks: list object with data for each task in a key-value pair where each key is used in Daemo's Project Authoring Interface as replaceable value

        A typical tasks list object is given below which passes an id and tweet text as input for each task.
        Remember these keys -- id, tweet -- have been used while creating task fields on Daemo task authoring inteface.
        ::
            tasks=[{
                "id": id,
                "tweet": text
            }]

        :param approve: a callback function which process worker responses to produce boolean value indicating if each worker response should be accepted and thus, paid or not.

        A typical approve callback function is given below which checks if tweet text in worker response is not empty.
        ::
            def approve_tweet(worker_responses):
                approvals = [len(get_tweet_text(response)) > 0 for response in worker_responses]
                return approvals

        :param completed: a callback function similiar to approve callback but process only the approved worker responses. It doesn't return any value.

        A typical completed callback function is given below which posts all the approved worker responses to twitter.
        ::
            def post_to_twitter(worker_responses):
                for worker_response in worker_responses:
                    twitter.post(worker_response)

        :param mock_workers: a callback function which simulates workers passing responses to different tasks

        A typical mock_workers callback function is given below which provides some text for tweet on behalf of *count* number of workers.
        ::
            def mock_workers(task, count):
                results = [
                    [{
                        "name": "tweet",
                        "value": "%d. Trump Trump everywhere not a Hillary to see." % num
                    }] for num in range(count)]
                return results

        :param stream: a boolean value which controls whether worker response should be received as soon as each worker has submitted or wait for all of them to complete.
        """
        logging.debug(msg="publish function called...")

        assert project_key is not None and len(project_key) > 0, Error.required("project_key")
        assert tasks is not None and len(tasks) >= 0, Error.required("tasks")
        assert isfunction(approve), Error.func_def_undefined("approve")
        assert isfunction(completed), Error.func_def_undefined("completed")

        if mock_workers is not None:
            assert isfunction(mock_workers), Error.func_def_undefined("mock_workers")

        thread = threading.Thread(
            target=self._publish,
            kwargs=dict(
                project_key=project_key,
                tasks=tasks,
                approve=approve, completed=completed,
                stream=stream,
                mock_workers=mock_workers,
                rerun_key=self.rerun_key
            )
        )
        thread.start()

    def rate(self, project_key, ratings, ignore_history=False):
        """
        Rate a worker submission

        :param project_key: string key for the project as shown in Daemo's Project Authoring Interface. It is a unique for each project.
        :param ratings: list object which provides ratings for one or more worker responses.
        Below, a single rating object is shown which must have three parameters - *task_id*, *worker_id* and *weight*.
        ::
            rating = {
                    "task_id": unique ID for the task (is available from the worker response),
                    "worker_id": unique ID for the worker (is available from the worker response),
                    "weight": rating value (can be integer or float)
            }

            ratings = [rating]
        :param ignore_history: boolean value that determines whether historical ratings should be considered for updating this new rating.
        If true, a worker's score will be set to the score that is provided to rate.
        If peer review is being used, this value should be set to True.

        :return: rating response
        """
        logging.debug(msg="rate function called")
        data = {
            "project_id": project_key,
            "ratings": ratings,
            "ignore_history": ignore_history
        }

        response = self._post("/api/worker-requester-rating/boomerang-feedback/", data=json.dumps(data))
        return response

    def peer_review(self, worker_responses, review_completed, inter_task_review=False):
        """
        Performs peer review for all the worker responses and when all ratings from peer feedback are received, review_completed callback is triggered.

        :param worker_responses: list of worker responses to the given task
        :param review_completed: a callback function to process all the ratings received from peer feedback on the worker responses
        :param inter_task_review: a boolean value to control if peer feedback should be allowed across workers on same task or not.
        If True, it will allow peer feedback for workers for any task they completed in the past irrespective of their similiarity
        If False, it only allows peer feedback among workers for the same task they completed

        :return: review response
        """

        if not callable(review_completed):
            pass

        thread = threading.Thread(
            target=self._peer_review,
            kwargs=dict(
                worker_responses=worker_responses,
                inter_task_review=inter_task_review,
                review_completed=review_completed,
                project_key=None
            )
        )
        thread.start()

    def peer_review_and_rate(self, worker_responses, project_key, inter_task_review=False):
        """
        Performs peer review for all the worker responses and when all ratings from peer feedback are received, these ratings are fed back to the platform to update worker ratings.

        :param worker_responses: list of worker responses to the given task
        :param review_completed: a callback function to process all the ratings received from peer feedback on the worker responses
        :param inter_task_review: a boolean value to control if peer feedback should be allowed across workers on same task or not.
        If True, it will allow peer feedback for workers for any task they completed in the past irrespective of their similiarity
        If False, it only allows peer feedback among workers for the same task they completed

        :return: review response
        """

        thread = threading.Thread(
            target=self._peer_review,
            kwargs=dict(
                worker_responses=worker_responses,
                inter_task_review=inter_task_review,
                review_completed=None,
                project_key=project_key
            )
        )
        thread.start()

    def _initialize(self):
        if self._credentials_exist():
            self._load_tokens()
        else:
            self._persist_tokens()

        self.session = requests.session()
        self._refresh_token()

        self._register_signals()

        self._monitor_messages()

        self._connect()

    def _publish(self, project_key, tasks, approve, completed, stream, mock_workers, rerun_key):
        # change status of project to published if not already set
        project = self._publish_project(project_key)

        new_tasks = self._create_tasks(project_key=project_key,
                                       tasks=tasks,
                                       approve=approve, completed=completed,
                                       stream=stream,
                                       rerun_key=rerun_key,
                                       count=project["repetition"])

        if new_tasks is not None and len(new_tasks) > 0 and mock_workers is not None:
            for new_task in new_tasks:
                thread = threading.Thread(
                    target=self._mock_task,
                    kwargs=dict(
                        task_id=new_task["id"],
                        mock_workers=mock_workers
                    )
                )
                thread.start()

    def _create_tasks(self, project_key, tasks, approve, completed, stream, rerun_key, count):
        logging.debug(msg="find and create missing tasks...")

        tasks = self._add_data(project_key, tasks, rerun_key)

        self._create_batch(project_key, tasks, approve, completed, stream, count)

        return tasks

    def _create_batch(self, project_key, data, approve, completed, stream, count):
        tasks = data["tasks"]

        self.batches.append({
            "project_key": project_key,
            "tasks": tasks,
            "approve": approve,
            "completed": completed,
            "stream": stream,
            "count": count,
            "status": {},
            "submissions": {},
            "is_complete": False,
            "aggregated_data": []
        })

        batch_index = len(self.batches) - 1

        for task in tasks:
            self._map_task(task, batch_index)

            task_id = task["id"]

            if "task_workers" in task and len(task["task_workers"]) > 0:
                for taskworker in task["task_workers"]:
                    taskworker_id = taskworker["id"]

                    self._replay_task(project_key, task_id, taskworker_id, taskworker)

    def _replay_task(self, project_key, task_id, taskworker_id, taskworker):
        # re-queue submitted results
        payload = json.dumps({
            "taskworker_id": taskworker_id,
            "task_id": task_id,
            "project_key": project_key,
            "taskworker": taskworker
        })

        self.queue.put({
            "payload": payload,
            "isBinary": False
        })

    def _map_task(self, task, batch_index):
        assert "id" in task and task["id"] is not None, "Invalid task"

        task_id = task["id"]

        self.batches[batch_index]["status"][task_id] = False

        if task_id not in self.batches[batch_index]["submissions"]:
            self.batches[batch_index]["submissions"][task_id] = 0

        if task_id in self.tasks:
            self.tasks[task_id]["batches"].append(batch_index)
        else:
            self.tasks[task_id] = {
                "batches": [batch_index],
                "task_id": task_id,
            }

    def _mock_task(self, task_id, mock_workers):
        logging.debug(msg="mocking workers...")

        task = self._fetch_task(task_id)

        if task is not None:
            num_workers = task["project"]["num_workers"]

            responses = mock_workers(task, num_workers)

            assert responses is not None and len(
                responses) == num_workers, "Incorrect number of responses. Result=%d. Expected=%d" % (
                len(responses), num_workers)

            results = [{
                           "items": [{
                                         "result": field["value"],
                                         "template_item": self._get_template_item_id(field["name"],
                                                                                     task["template"]["fields"])
                                     } for field in response]
                       } for response in responses]

            self._submit_results(
                task["id"],
                results
            )

    def _peer_review(self, worker_responses, review_completed, project_key, inter_task_review=False):
        task_workers = [response['id'] for response in worker_responses]

        response = self._launch_peer_review(task_workers, inter_task_review)

        if review_completed is not None:
            match_group_id = response['match_group_id']
            self.cache[match_group_id] = review_completed

        if project_key is not None:
            self.cache['project_key'] = project_key

    def _get_template_item_id(self, template_item_name, template_items):
        """
        Find template item by name from list of template items in a task

        :param template_item_name:
        :param template_items:
        :return:
        """
        fields = filter(lambda x: x["name"] == template_item_name, template_items)

        if fields is not None and len(fields) > 0:
            return fields[0]["id"]
        return -1

    def _is_task_complete(self, batch_index, task_id):
        task_status = self._fetch_task_status(task_id)

        is_done = task_status["is_done"]
        expected = int(task_status["expected"])

        # compare result counts too
        return is_done and expected == self.batches[batch_index]["submissions"][task_id]

    def _mark_task_completed(self, batch_index, task_id):
        if task_id in self.batches[batch_index]["status"]:
            self.batches[batch_index]["status"][task_id] = True

    def _is_batch_complete(self, batch_index):
        return all(self.batches[batch_index]["status"].values())

    def _mark_batch_completed(self, batch_index):
        self.batches[batch_index]["is_complete"] = True

    def _all_batches_complete(self):
        return all([batch["is_complete"] for batch in self.batches])

    def _stop(self):
        logging.debug(msg="stop everything")
        self.queue.put(None)
        reactor.callFromThread(reactor.stop)

    def _handler(self, signum, frame):
        if signum in [signal.SIGINT, signal.SIGTERM, signal.SIGABRT]:
            self.queue.put(None)

            if reactor.running:
                reactor.callFromThread(reactor.stop)
            else:
                sys.exit(0)

    def _register_signals(self):
        thread = threading.Thread(target=signal.pause)
        thread.start()

    def _aggregate(self, batch_index, task_id, task_data):
        self.batches[batch_index]["aggregated_data"].append({
            "task_id": task_id,
            "data": task_data
        })

    def _get_aggregated(self, batch_index):
        return [x["data"] for x in self.batches[batch_index]["aggregated_data"]]

    # Web-socket Communication =========================================================================================

    def _connect(self):
        signal.signal(signal.SIGINT, self._handler)

        self.ws_process = multiprocessing.Process(
            target=self._create_websocket,
            args=(self.queue,),
            kwargs=dict(access_token=self.access_token, host=self.host)
        )
        self.ws_process.start()

    def _create_websocket(self, queue, access_token, host):
        logging.debug(msg="open websocket connection")

        headers = {
            AUTHORIZATION: TOKEN % access_token
        }

        self.ws = WebSocketClientFactory(daemo.WEBSOCKET + host + daemo.WS_BOT_SUBSCRIBE_URL, headers=headers)
        self.ws.protocol = ClientProtocol
        self.ws.queue = queue
        connectWS(self.ws)
        reactor.run()

    def _monitor_messages(self):
        threading.Thread(
            target=self._read_message
        ).start()

    def _read_message(self):
        while True:
            data = self.queue.get(block=True)

            # to stop reading, None is passed to the queue by server/client
            if data is None:
                break

            logging.debug(msg="got new message")

            if not data["isBinary"]:
                logging.debug("<<<{}>>>".format(data["payload"].decode("utf8")))

            thread = threading.Thread(
                target=self._process_message,
                kwargs=dict(
                    payload=data["payload"],
                    isBinary=data["isBinary"]
                )
            )
            thread.start()

            if not self.multi_threading:
                thread.join()

    def _process_message(self, payload, isBinary):
        logging.debug(msg="processing message...")

        if not isBinary:
            response = json.loads(payload.decode("utf8"))

            type = response.get("type", "REGULAR")
            payload = response.get('payload')

            if type == "REVIEW":
                return self._process_review_message(payload)

            taskworker_id = int(payload.get("taskworker_id", 0))
            task_id = int(payload.get("task_id", 0))
            project_key = payload.get("project_key", None)
            taskworker = payload.get("taskworker", None)

            # ignore data pushed via GUI (has no batch info)
            if task_id in self.tasks:
                assert taskworker_id > 0, Error.required("taskworker_id")
                assert task_id > 0, Error.required("task_id")
                assert project_key is not None, Error.required("project_key")

                batch_indices = self.tasks[task_id]["batches"]

                if taskworker is None:
                    task_data = self._get_task_results_by_taskworker_id(taskworker_id)
                else:
                    task_data = self._transform_task_results(taskworker)

                for batch_index in batch_indices:
                    assert batch_index < len(self.batches) \
                           and self.batches[batch_index] is not None, "Missing batch for task"

                    assert task_data is not None, "No worker responses for the task found"

                    config = self.batches[batch_index]

                    task_data["accept"] = False
                    approve = config["approve"]
                    completed = config["completed"]
                    stream = config["stream"]

                    # increment count to track completion
                    self.batches[batch_index]["submissions"][task_id] += 1

                    if stream:
                        self._stream_response(batch_index, task_id, task_data, approve, completed)
                    else:
                        self._aggregate_responses(batch_index, task_id, task_data, approve, completed)

                    if self._all_batches_complete():
                        logging.debug(msg="are all batches done? yes")
                        self._stop()
                    else:
                        logging.debug("are all batches done? no")
            else:
                logging.debug("No corresponding task found. Message ignored.")

    def _process_review_message(self, payload):
        if payload['is_done']:
            match_group_id = payload["match_group_id"]
            ratings = self._get_trueskill_scores(match_group_id)

            if 'project_key' in self.cache:
                project_key = self.cache['project_key']
                self.rate(project_key, ratings, ignore_history=True)
            elif match_group_id in self.cache:
                callback = self.cache[match_group_id]
                callback(ratings)

    def _stream_response(self, batch_index, task_id, task_data, approve, completed):
        logging.debug(msg="streaming responses...")

        logging.debug(msg="calling approved callback...")

        if approve([task_data]):
            task_data["accept"] = True
            logging.debug(msg="task approved.")
        else:
            logging.debug(msg="task rejected.")

        task_status = self._update_approval_status(task_data)
        task_status.raise_for_status()

        if task_data["accept"]:
            logging.debug(msg="calling completed callback")
            completed([task_data])

        is_done = self._is_task_complete(batch_index, task_id)
        logging.debug(msg="is task %d done? %s" % (task_id, is_done))

        if is_done:
            self._mark_task_completed(batch_index, task_id)

    def _aggregate_responses(self, batch_index, task_id, task_data, approve, completed):
        logging.debug(msg="aggregating responses...")

        # store it for aggregation (stream = False)
        self._aggregate(batch_index, task_id, task_data)

        is_done = self._is_task_complete(batch_index, task_id)
        logging.debug(msg="is task %d done? %s" % (task_id, is_done))

        if is_done:
            self._mark_task_completed(batch_index, task_id)

        is_done = self._is_batch_complete(batch_index)

        if is_done:
            self._mark_batch_completed(batch_index)

            logging.debug(msg="is batch done? True")
            tasks_data = self._get_aggregated(batch_index)

            logging.debug(msg="calling approved callback...")
            approvals = approve(tasks_data)

            for approval in approvals:
                task_data["accept"] = approval

                task_status = self._update_approval_status(task_data)
                task_status.raise_for_status()

            approved_tasks = [x[0] for x in zip(tasks_data, approvals) if x[1]]

            logging.debug(msg="calling completed callback...")
            completed(approved_tasks)
        else:
            logging.debug(msg="is batch done? False")

    # Backend API ======================================================================================================

    def _fetch_task(self, task_id):
        response = self._get("/api/task/%d/" % task_id, data=json.dumps({}))
        response.raise_for_status()

        data = response.json()

        fields = []

        if "items" in data["template"]:
            for item in data["template"]["items"]:
                if item["role"] == "input":
                    options = []

                    if "choices" in item["aux_attributes"]:
                        for option in item["aux_attributes"]["choices"]:
                            options.append({
                                "position": option["position"],
                                "value": option["value"],
                            })

                    fields.append({
                        "id": item["id"],
                        "name": item["name"],
                        "type": item["type"],
                        "position": item["position"],
                        "question": item["aux_attributes"]["question"]["value"],
                        "options": options
                    })

        task = {
            "id": data["id"],
            "project": {
                "id": data["project_data"]["id"],
                "key": data["project_data"]["hash_id"],
                "name": data["project_data"]["name"],
                "num_workers": data["project_data"]["repetition"],
            },
            "template": {
                "id": data["template"]["id"],
                "fields": fields
            }
        }

        return task

    def _fetch_config(self, rerun_key):
        response = self._get("/api/task/?filter_by=rerun_key&rerun_key=%s" % rerun_key, data=json.dumps({}))
        response.raise_for_status()

        return response.json()

    def _publish_project(self, project_id):
        response = self._post("/api/project/%s/publish/" % project_id, data=json.dumps({}))
        response.raise_for_status()

        return response.json()

    def _add_data(self, project_key, tasks, rerun_key):
        response = self._post("/api/project/%s/add-data/" % project_key, data=json.dumps({
            "tasks": tasks,
            "rerun_key": rerun_key
        }))

        response.raise_for_status()

        return response.json()

    def _get_task_results_by_task_id(self, task_id):
        response = self._get("/api/task-worker/list-submissions/?task_id=%d" % task_id, data=json.dumps({}))
        response.raise_for_status()

        return response.json()

    def _get_task_results_by_taskworker_id(self, taskworker_id):
        try:
            response = self._get("/api/task-worker/%d/" % taskworker_id, data={})
            response.raise_for_status()
            results = response.json()

            return self._transform_task_results(results)
        except Exception as e:
            print e.message
            return None

    def _transform_task_results(self, data):
        fields = {}
        for result in data.get("results"):
            fields[result["key"]] = result["result"]

        data["fields"] = fields
        del data["results"]

        return data

    def _update_approval_status(self, task):
        data = {
            "status": STATUS_ACCEPTED if task["accept"] else STATUS_REJECTED,
            "workers": [task["id"]]
        }

        response = self._post("/api/task-worker/bulk-update-status/", data=json.dumps(data))
        return response

    def _fetch_task_status(self, task_id):
        response = self._get("/api/task/%s/is-done/" % task_id, data={})
        response.raise_for_status()

        task_data = response.json()
        return task_data

    def _submit_results(self, task_id, results):
        data = {
            "task_id": task_id,
            "results": results
        }

        response = self._post("/api/task-worker-result/mock-results/", data=json.dumps(data))
        response.raise_for_status()
        return response.json()

    def _launch_peer_review(self, task_workers, inter_task_review):
        data = {
            "task_workers": task_workers,
            "inter_task_review": inter_task_review
        }

        response = self._post("/api/task/peer-review/", data=json.dumps(data))
        response.raise_for_status()
        return response.json()

    def _get_trueskill_scores(self, match_group_id):
        response = self._get("/api/worker-requester-rating/trueskill/?match_group_id={}".format(match_group_id))
        response.raise_for_status()

        return response.json()

    # Authentication ===================================================================================================

    def _is_auth_error(self, response):
        try:
            response = response.json()
        except Exception as e:
            pass

        return response is not None and isinstance(response, dict) and response.get("detail",
                                                                                    "") == CREDENTIALS_NOT_PROVIDED

    def _credentials_exist(self):
        return os.path.isfile(self.credentials_path)

    def _load_tokens(self):
        with open(self.credentials_path, "r") as infile:
            data = json.load(infile)

            assert data[CLIENT_ID] is not None and len(data[CLIENT_ID]) > 0, Error.required(CLIENT_ID)
            assert data[ACCESS_TOKEN] is not None and len(data[ACCESS_TOKEN]) > 0, Error.required(ACCESS_TOKEN)
            assert data[REFRESH_TOKEN] is not None and len(data[REFRESH_TOKEN]) > 0, Error.required(REFRESH_TOKEN)

            self.client_id = data[CLIENT_ID]
            self.access_token = data[ACCESS_TOKEN]
            self.refresh_token = data[REFRESH_TOKEN]

    def _persist_tokens(self):
        with open(self.credentials_path, "w") as outfile:
            fcntl.flock(outfile.fileno(), fcntl.LOCK_EX)

            data = {
                CLIENT_ID: self.client_id,
                ACCESS_TOKEN: self.access_token,
                REFRESH_TOKEN: self.refresh_token
            }
            json.dump(data, outfile)

    def _refresh_token(self):
        self._load_tokens()

        data = {
            CLIENT_ID: self.client_id,
            GRANT_TYPE: REFRESH_TOKEN,
            REFRESH_TOKEN: self.refresh_token
        }

        response = self._post(daemo.OAUTH_TOKEN_URL, data=data, is_json=False, authorization=False)

        if "error" in response.json():
            raise AuthException("Error refreshing access token. Please retry again.")
        else:
            response = response.json()

            assert response[ACCESS_TOKEN] is not None and len(response[ACCESS_TOKEN]) > 0, Error.required(ACCESS_TOKEN)
            assert response[REFRESH_TOKEN] is not None and len(response[REFRESH_TOKEN]) > 0, Error.required(
                REFRESH_TOKEN)

            self.access_token = response.get(ACCESS_TOKEN)
            self.refresh_token = response.get(REFRESH_TOKEN)

            self._persist_tokens()

    # REST API =========================================================================================================

    def _get(self, relative_url, data, headers=None, is_json=True, authorization=True):
        if headers is None:
            headers = dict()

        if authorization:
            headers.update({
                AUTHORIZATION: TOKEN % self.access_token,
            })

        if is_json:
            headers.update({
                CONTENT_TYPE: CONTENT_JSON
            })

        response = self.session.get(daemo.HTTP + self.host + relative_url, data=data, headers=headers)

        if self._is_auth_error(response):
            self._refresh_token()

            if authorization:
                headers.update({
                    AUTHORIZATION: TOKEN % self.access_token
                })

            response = self.session.get(daemo.HTTP + self.host + relative_url, data=data, headers=headers)

        return response

    def _post(self, relative_url, data, headers=None, is_json=True, authorization=True):
        if headers is None:
            headers = dict()

        if authorization:
            headers.update({
                AUTHORIZATION: TOKEN % self.access_token,
            })

        if is_json:
            headers.update({
                CONTENT_TYPE: CONTENT_JSON
            })

        response = self.session.post(daemo.HTTP + self.host + relative_url, data=data, headers=headers)

        if self._is_auth_error(response):
            self._refresh_token()

            if authorization:
                headers.update({
                    AUTHORIZATION: TOKEN % self.access_token
                })

            response = self.session.post(daemo.HTTP + self.host + relative_url, data=data, headers=headers)

        return response

    def _put(self, relative_url, data, headers=None, is_json=True, authorization=True):
        if headers is None:
            headers = dict()

        if authorization:
            headers.update({
                AUTHORIZATION: TOKEN % self.access_token,
            })

        if is_json:
            headers.update({
                CONTENT_TYPE: CONTENT_JSON
            })

        response = self.session.put(daemo.HTTP + self.host + relative_url, data=data, headers=headers)

        if self._is_auth_error(response):
            self._refresh_token()

            if authorization:
                headers.update({
                    AUTHORIZATION: TOKEN % self.access_token
                })

            response = self.session.put(daemo.HTTP + self.host + relative_url, data=data, headers=headers)

        return response
