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

STREAM = "stream"
WRITE_ONLY = "w"
READ_ONLY = "r"
CALLBACK = "completed"
APPROVE = "approve"
GRANT_TYPE = "grant_type"
PROJECT_ID = "project_id"
REFRESH_TOKEN = "refresh_token"
ACCESS_TOKEN = "access_token"
CLIENT_ID = "client_id"
CREDENTIALS_NOT_PROVIDED = "Authentication credentials were not provided."
CONTENT_JSON = "application/json"
CONTENT_FORM_URLENCODED = "application/x-www-form-urlencoded"
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

        daemo = DaemoClient(CREDENTIALS_FILE, rerun_key=RERUN_KEY)

    :param credentials_path: path of the daemo credentials file which can be downloaded from daemo user profile (**Menu** >> **Get Credentials**)
    :param host: daemo server to connect to - uses a default server if not defined
    :param rerun_key: a string used to differentiate each script run. If this key is same, it replays the last results from worker responses and brings you to the last point when script stopped execution.
    :param multi_threading: False by default, bool value to enable multi-threaded response handling
    """

    def __init__(self, credentials_path, host=daemo.HOST, rerun_key=None, multi_threading=False):
        logging.debug(msg="initializing client...")
        assert credentials_path is not None and len(credentials_path) > 0, Error.required("credentials_path")

        self.client_id = None
        self.access_token = None
        self.refresh_token = None

        self.ws_process = None

        self.projects = None
        self.batches = {}
        self.batches_in_progress = set()
        self.cache = []
        self.aggregated_data = []

        self.credentials_path = credentials_path
        self.rerun_key = rerun_key
        self.multi_threading = multi_threading

        self.host = host

        self.queue = multiprocessing.Queue()

        if self._credentials_exist():
            self._load_tokens()
        else:
            self._persist_tokens()

        self.session = requests.session()
        self._refresh_token()

        self._register_signals()

        self._monitor_messages()

        self._connect()

        if self.rerun_key is not None and len(self.rerun_key) > 0:
            self._fetch_batch_config(self.rerun_key)

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

        A typical tasks list object is given below which passes an id and tweet text as input for each task. Remember these keys -- id, tweet -- have been used while creating task fields on Daemo task authoring inteface.
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
        assert isfunction(approve), Error.func_def_undefined(APPROVE)
        assert isfunction(completed), Error.func_def_undefined(CALLBACK)
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

    def rate(self, project_key, ratings):
        """

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

        :return: rating response
        """
        logging.debug(msg="rate function called")
        data = {
            "project_id": project_key,
            "ratings": ratings
        }

        response = self._post("/api/worker-requester-rating/boomerang-feedback/", data=json.dumps(data))
        return response

    def peer_review(self, worker_responses, inter_task_review=False):
        """

        :param worker_responses: list of worker responses to the given task
        :param inter_task_review: a boolean value that controls whether or not review matchups should be setup between
        workers from different tasks. If true, matches can be setup between tasks. If false, all matchups will be
        between workers of the current task.
        :return: review response
        """

        task_workers = [w['id'] for w in worker_responses]
        data = {
            "task_workers": task_workers,
            "inter_task_review": inter_task_review
        }

        response = self._post("/api/task/peer-review/", data=json.dumps(data))


        return response

    def _fetch_batch_config(self, rerun_key):
        logging.debug(msg="fetching batch config...")

        data = self._fetch_batch(rerun_key)

        # data got : tasks -> project + batch
        # re-map to project -> batch -> tasks
        for task in data:
            project_key = task["project_data"]["hash_id"]

            if project_key not in self.batches:
                self.batches[project_key] = {}

            batch_id = task["batch"]["id"]

            if batch_id not in self.batches[project_key]:
                self.batches[project_key][batch_id] = {
                    "id": task["batch"]["id"],
                    "parent": task["batch"]["parent"],
                    "project_key": project_key,
                    "project_id": task["project_data"]["id"],
                    "tasks": []
                }

            self.batches[project_key][batch_id]["tasks"].append({
                "id": task["id"],
                "data": task["data"],
            })

    def _publish(self, project_key, tasks, approve, completed, stream, mock_workers, rerun_key):
        # change status of project to published if not already set
        project = self._publish_project(project_key)

        # get a relevant batch existing for this project with this data set (tasks)
        batch = self._find_batch(project_key, tasks)

        # if no batch found, push this as new dataset
        if batch is None:
            logging.debug(msg="no batch found.")
            new_tasks = self._create_task(project_key=project_key,
                                          batch=batch,
                                          data=tasks,
                                          approve=approve, completed=completed,
                                          stream=stream,
                                          rerun_key=rerun_key)
            if new_tasks is not None and len(new_tasks) > 0 and mock_workers is not None:
                thread = threading.Thread(
                    target=self._mock_task,
                    kwargs=dict(
                        task_id=new_tasks[0]["id"],
                        mock_workers=mock_workers
                    )
                )
                thread.start()
        else:
            logging.debug(msg="batch found.")
            logging.debug(msg="relay old task results again to the processing queue")
            old_tasks = [{
                             "project": project["id"],
                             "batch": batch,
                             "id": task["id"],
                             "data": task["data"]
                         } for task in batch["tasks"]]
            self._map_task(project_key, old_tasks, approve, completed, stream, rerun_key)

            for task in old_tasks:
                task_workers = self._get_task_results_by_task_id(task["id"])

                # re-queue submitted results
                for task_worker in task_workers:
                    payload = json.dumps({
                        "taskworker_id": task_worker["id"],
                        "task_id": task["id"],
                        "project_hash_id": project_key,
                        "batch": batch,
                    })

                    self.queue.put({
                        "payload": payload,
                        "isBinary": False
                    })

    def _peer_review(self, match_group_id):
        pass

    def _create_task(self, project_key, batch, data, approve, completed, stream, rerun_key):
        logging.debug(msg="creating tasks...")

        batch_id = None

        if batch is not None:
            batch_id = batch["id"]

        tasks = self._add_data(project_key, batch_id, data, rerun_key)
        self._map_task(project_key, tasks, approve, completed, stream, rerun_key)

        return tasks

    def _map_task(self, project_key, tasks, approve, completed, stream, rerun_key):
        for task in tasks:
            self.cache.append({
                "rerun_key": rerun_key,
                "project_id": task["project"],
                "project_key": project_key,
                "batch_id": task["batch"]["id"],
                "parent_batch_id": task["batch"]["parent"],
                "aggregation_id": task["batch"]["parent"] if task["batch"]["parent"] is not None else task["batch"][
                    "id"],
                "task_id": task["id"],
                "approve": approve,
                "completed": completed,
                "stream": stream
            })

            self.batches_in_progress.add(task["batch"]["id"])

    def _mock_task(self, task_id, mock_workers):
        logging.debug(msg="mocking workers")

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

    def _get_template_item_id(self, template_item_name, template_items):
        fields = filter(lambda x: x["name"] == template_item_name, template_items)

        if fields is not None and len(fields) > 0:
            return fields[0]["id"]
        return -1

    def _get_task_map(self, project_key, task_id, batch_id):
        matched = filter(
            lambda x: x["project_key"] == project_key and x["task_id"] == task_id and x["batch_id"] == batch_id,
            self.cache)
        return matched

    def _find_batch(self, project_key, tasks):
        if project_key in self.batches:
            project_batches = self.batches[project_key]

            for batch in project_batches.values():
                if self._tasks_match_score(tasks, batch["tasks"]):
                    # never use the same batch again
                    del self.batches[project_key][batch["id"]]
                    return batch

        return None

    def _remove_batch(self, batch_id):
        self.batches_in_progress.discard(batch_id)

    def _all_batches_complete(self):
        return len(self.batches_in_progress) == 0

    def _stop(self):
        logging.debug(msg="stop everything")
        self.queue.put(None)
        reactor.callFromThread(reactor.stop)

    def _tasks_match_score(self, new_tasks, orig_tasks):
        # pairwise instead of N x N comparison
        tasks = zip(new_tasks, orig_tasks)
        matching_tasks = filter(lambda task: self._match_task(task[0], task[1]["data"]), tasks)

        return len(matching_tasks) == len(orig_tasks)

    def _match_task(self, task1, task2):
        if task1 is None or task2 is None:
            return False

        return cmp(task1, task2) == 0

    def _doesnt_match_task(self, task1, task2):
        if task1 is None or task2 is None:
            return False

        return cmp(task1, task2) != 0

    def _get_task_diff(self, orig_tasks, new_tasks):
        if orig_tasks is None:
            if new_tasks is None:
                return []
            return new_tasks

        tasks = zip(new_tasks, orig_tasks)
        return filter(lambda task: self.doesnt_match_task(task[0], task[1]), tasks)

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

    def _aggregate(self, project_key, task_id, aggregation_id, task_data):
        logging.debug(msg="aggregating...")
        self.aggregated_data.append({
            "batch_id": aggregation_id,
            "project_key": project_key,
            "task_id": task_id,
            "data": task_data
        })

    def _get_aggregated(self, aggregation_id):
        matched = filter(lambda x: x["batch_id"] == aggregation_id, self.aggregated_data)
        return [x["data"] for x in matched]

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

            if data is None:
                break

            logging.debug(msg="got new message")

            thread = threading.Thread(
                target=self._processMessage,
                kwargs=dict(
                    payload=data["payload"],
                    isBinary=data["isBinary"]
                )
            )
            thread.start()

            if not self.multi_threading:
                thread.join()

    def _processMessage(self, payload, isBinary):
        logging.debug(msg="process message")

        if not isBinary:
            response = json.loads(payload.decode("utf8"))

            taskworker_id = int(response.get("taskworker_id", 0))
            task_id = int(response.get("task_id", 0))
            project_key = response.get("project_hash_id", None)
            batch = response.get("batch", None)

            # ignore data pushed via GUI (has no batch info)
            if batch is not None:
                assert taskworker_id > 0, Error.required("taskworker_id")
                assert task_id > 0, Error.required("task_id")
                assert project_key is not None, Error.required("project_hash_id")

                task_configs = self._get_task_map(project_key, task_id, batch["id"])

                if task_configs is not None and len(task_configs) > 0:
                    task_data = self._get_task_results_by_taskworker_id(taskworker_id)

                    if task_data is not None:
                        task_data["accept"] = False

                        for config in task_configs:
                            approve = config["approve"]
                            completed = config["completed"]
                            stream = config["stream"]
                            aggregation_id = config["aggregation_id"]

                            if stream:
                                logging.debug(msg="streaming responses...")

                                logging.debug(msg="calling approved callback...")
                                if approve([task_data]):
                                    task_data["accept"] = True
                                    logging.debug(msg="task approved.")
                                else:
                                    logging.debug(msg="task rejected.")

                                task_status = self._update_status(task_data)
                                task_status.raise_for_status()

                                if task_data["accept"]:
                                    logging.debug(msg="calling completed callback")
                                    completed([task_data])

                                is_done = self._fetch_batch_status(project_key, aggregation_id)
                                logging.debug(msg="is batch done? %s"%is_done)

                                if is_done:
                                    logging.debug(msg="removing batch...")
                                    # remove it from global list of projects
                                    self._remove_batch(aggregation_id)

                            else:
                                # store it for aggregation (stream = False)
                                self._aggregate(project_key, task_id, aggregation_id, task_data)

                                is_done = self._fetch_batch_status(project_key, aggregation_id)

                                if is_done:
                                    logging.debug(msg="is batch done? yes")
                                    tasks_data = self._get_aggregated(aggregation_id)

                                    logging.debug(msg="calling approved callback...")
                                    approvals = approve(tasks_data)

                                    for approval in approvals:
                                        task_data["accept"] = approval

                                        task_status = self._update_status(task_data)
                                        task_status.raise_for_status()

                                    approved_tasks = [x[0] for x in zip(tasks_data, approvals) if x[1]]

                                    logging.debug(msg="calling completed callback...")
                                    completed(approved_tasks)

                                    logging.debug(msg="removing batch...")
                                    self._remove_batch(aggregation_id)
                                else:
                                    logging.debug(msg="is batch done? no")

                        if self._all_batches_complete():
                            logging.debug(msg="is all done? yes")
                            self._stop()
                        else:
                            logging.debug("is all done? no")
                    else:
                        logging.debug(msg="no worker responses found yet.")
                else:
                    logging.debug(msg="no task mapping found.")


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

    def _fetch_batch(self, rerun_key):
        response = self._get("/api/task/?filter_by=rerun_key&rerun_key=%s" % rerun_key, data=json.dumps({}))
        response.raise_for_status()

        return response.json()

    def _publish_project(self, project_id):
        response = self._post("/api/project/%s/publish/" % project_id, data=json.dumps({}))

        response.raise_for_status()

        return response.json()

    def _add_data(self, project_key, batch_id, data, rerun_key):
        response = self._post("/api/project/%s/add-data/" % project_key, data=json.dumps({
            "tasks": data,
            "parent_batch_id": batch_id,
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

            data = response.json()

            fields = {}
            for result in data.get("results"):
                fields[result["key"]] = result["result"]

            data["fields"] = fields
            del data["results"]

            return data
        except Exception as e:
            print e.message
            return None

    def _update_status(self, task):
        data = {
            "status": STATUS_ACCEPTED if task["accept"] else STATUS_REJECTED,
            "workers": [task["id"]]
        }

        response = self._post("/api/task-worker/bulk-update-status/", data=json.dumps(data))
        return response

    def _fetch_batch_status(self, project_key, aggregation_id):
        response = self._get("/api/project/%s/is-done/?batch_id=%d" % (project_key, aggregation_id), data={})
        response.raise_for_status()

        project_data = response.json()
        is_done = project_data.get("is_done")
        return is_done

    def _submit_results(self, task_id, results):
        data = {
            "task_id": task_id,
            "results": results
        }

        response = self._post("/api/task-worker-result/mock-results/", data=json.dumps(data))
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
        with open(self.credentials_path, READ_ONLY) as infile:
            fcntl.flock(infile.fileno(), fcntl.LOCK_EX)
            data = json.load(infile)

            assert data[CLIENT_ID] is not None and len(data[CLIENT_ID]) > 0, Error.required(CLIENT_ID)
            assert data[ACCESS_TOKEN] is not None and len(data[ACCESS_TOKEN]) > 0, Error.required(ACCESS_TOKEN)
            assert data[REFRESH_TOKEN] is not None and len(data[REFRESH_TOKEN]) > 0, Error.required(REFRESH_TOKEN)

            self.client_id = data[CLIENT_ID]
            self.access_token = data[ACCESS_TOKEN]
            self.refresh_token = data[REFRESH_TOKEN]

    def _persist_tokens(self):
        with open(self.credentials_path, WRITE_ONLY) as outfile:
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

        with open("error.html", "w") as outfile:
            outfile.write(response.text)

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
