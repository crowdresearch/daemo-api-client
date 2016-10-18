import json
import logging.config
import multiprocessing
import os
import signal
from inspect import isfunction

import yaml

from daemo.api import ApiClient
from daemo.constants import *
from daemo.errors import Error
from daemo.storage import Store
from daemo.utils import callback_thread, check_dependency, get_template_item_id, transform_task, transform_task_results
from daemo.websockets import Channel

log = logging.getLogger(__name__)


class DaemoClient:
    """
    Initializes Daemo Client by authentication with Daemo host server.

    First download the credentials file from your Daemo User Profile. Fill in the RERUN_KEY which is considered incremental number here for each run.
    ::
        RERUN_KEY = '0001'

        daemo = DaemoClient(rerun_key=RERUN_KEY)

    :param credentials_path: path of the daemo credentials file which can be downloaded from daemo user profile (**Menu** >> **Get Credentials**)

    :param rerun_key: a string used to differentiate each script run. If this key is same, it replays the last results from worker responses and brings you to the last point when script stopped execution.
    :param multi_threading: False by default, bool value to enable multi-threaded response handling
    :param host: daemo server to connect to - uses a default server if not defined
    :param is_secure: boolean flag to control if connection happen via secure mode or not
    :param is_sandbox: boolean flag to control if tasks will be posted to sandbox instead of production system of Daemo
    :param log_config: standard python logging module based dictionary config to control logging

    """

    def __init__(self, credentials_path='credentials.json', rerun_key=None, multi_threading=False, host=None,
                 is_secure=True, is_sandbox=False, log_config=None):

        # log using default logging config if no config provided
        if log_config is None:
            logging_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logging.conf')

            with open(logging_path) as f:
                log_config = yaml.load(f)

        logging.config.dictConfig(log_config)

        log.info(msg="initializing client...")

        check_dependency(credentials_path is not None and len(credentials_path) > 0, Error.required("credentials_path"))
        self.credentials_path = credentials_path
        self.rerun_key = rerun_key
        self.multi_threading = multi_threading

        self.http_proto = "http://"
        self.websock_proto = "ws://"

        if is_secure:
            self.http_proto = "https://"
            self.websock_proto = "wss://"

        self.host = PRODUCTION

        if is_sandbox:
            self.host = SANDBOX

        if host is not None:
            self.host = host

        self.api_client = ApiClient(self.credentials_path, self.host, self.http_proto)

        self.store = Store()

        self._open_channel()

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

        :param project_key: string key for the project as shown in Daemo's Project Authoring Interface. It is unique for each project.
        :param tasks: list object with data for each task in a key-value pair where each key is used in Daemo's Project Authoring Interface as replaceable value

        A typical tasks list object is given below which passes an id and tweet text as input for each task.
        Remember these keys -- id, tweet -- have been used while creating task fields on Daemo task authoring interface.
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
        log.info(msg="publishing project...")

        check_dependency(project_key is not None and len(project_key) > 0, Error.required("project_key"))
        check_dependency(tasks is not None and len(tasks) >= 0, Error.required("tasks"))
        check_dependency(isfunction(approve), Error.func_def_undefined("approve"))
        check_dependency(isfunction(completed), Error.func_def_undefined("completed"))

        if mock_workers is not None:
            check_dependency(isfunction(mock_workers), Error.func_def_undefined("mock_workers"))

        thread = callback_thread(
            name='publish',
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

        :param project_key: string key for the project as shown in Daemo's Project Authoring Interface. It is unique for each project.

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
        log.info(msg="rating workers...")
        data = {
            "project_key": project_key,
            "ratings": ratings,
            "ignore_history": ignore_history
        }

        response = self.api_client.boomerang_feedback(data)
        return response

    def peer_review(self, project_key, worker_responses, review_completed, inter_task_review=False):
        """
        Performs peer review for all the worker responses and when all ratings from peer feedback are received, ``review_completed`` callback is triggered.

        :param project_key: string key for the project as shown in Daemo's Project Authoring Interface. It is a unique for each project
        :param worker_responses: list of worker responses to the given task
        :param review_completed: a callback function to process all the ratings received from peer feedback on the worker responses
        :param inter_task_review: a boolean value to control if peer feedback should be allowed across workers on same task or not. If True, it will allow peer feedback for workers for any task they completed in the past irrespective of their similiarity. If False, it only allows peer feedback among workers for the same task they completed

        :return: review response

        """
        log.info(msg="initiating peer review...")

        check_dependency(project_key is not None and len(project_key) > 0, Error.required("project_key"))
        check_dependency(worker_responses is not None and len(worker_responses) >= 0,
                         Error.required("worker_responses"))
        check_dependency(isfunction(review_completed), Error.func_def_undefined("review_completed"))

        self._peer_review(
            project_key=project_key,
            worker_responses=worker_responses,
            inter_task_review=inter_task_review,
            review_completed=review_completed
        )

        # thread = callback_thread(
        #     name='peer review',
        #     target=self._peer_review,
        #     kwargs=dict(
        #         project_key=project_key,
        #         worker_responses=worker_responses,
        #         inter_task_review=inter_task_review,
        #         review_completed=review_completed
        #     )
        # )
        # thread.start()

    def peer_review_and_rate(self, project_key, worker_responses, inter_task_review=False):
        """
        Performs peer review for all the worker responses and when all ratings from peer feedback are received, these ratings are fed back to the platform to update worker ratings.

        :param project_key: string key for the project as shown in Daemo's Project Authoring Interface. It is unique for each project
        :param worker_responses: list of worker responses to the given task
        :param review_completed: a callback function to process all the ratings received from peer feedback on the worker responses
        :param inter_task_review: a boolean value to control if peer feedback should be allowed across workers on same task or not. If True, it will allow peer feedback for workers for any task they completed in the past irrespective of their similiarity. If False, it only allows peer feedback among workers for the same task they completed

        :return: review response
        """

        log.debug(msg="initiating peer review and rating...")

        check_dependency(project_key is not None and len(project_key) > 0, Error.required("project_key"))
        check_dependency(worker_responses is not None and len(worker_responses) >= 0,
                         Error.required("worker_responses"))

        self._peer_review(
            project_key=project_key,
            worker_responses=worker_responses,
            inter_task_review=inter_task_review,
            review_completed=self._review_completed
        )

        # thread = callback_thread(
        #     name='peer review and rate',
        #     target=self._peer_review,
        #     kwargs=dict(
        #         project_key=project_key,
        #         worker_responses=worker_responses,
        #         inter_task_review=inter_task_review,
        #         review_completed=self._review_completed
        #     )
        # )
        # thread.start()

    def _publish(self, project_key, tasks, approve, completed, stream, mock_workers, rerun_key):

        # change status of project to published if not already set
        log.info(msg="publishing project...")
        project = self.api_client.publish_project(project_key)

        log.info(msg="open [ %s%s/project-review/%s ] to review project's progress" % (
            self.http_proto, self.host, project["id"]))

        log.info(msg="press [ Ctrl + c ] to terminate...")

        new_tasks = self._create_tasks(project_key=project_key,
                                       tasks=tasks,
                                       approve=approve, completed=completed,
                                       stream=stream,
                                       rerun_key=rerun_key,
                                       count=project["repetition"])

        if new_tasks is not None and len(new_tasks) > 0 and mock_workers is not None:
            for new_task in new_tasks:
                thread = callback_thread(
                    name='mock',
                    target=self._mock_task,
                    kwargs=dict(
                        task_id=new_task["id"],
                        mock_workers=mock_workers
                    )
                )
                thread.start()

    def _create_tasks(self, project_key, tasks, approve, completed, stream, rerun_key, count):
        log.info(msg="syncing tasks...")

        data = self.api_client.add_data(project_key, tasks, rerun_key)
        tasks = data["tasks"]

        self._create_batch(project_key, tasks, approve, completed, stream, count)

        return tasks

    def _create_batch(self, project_key, tasks, approve, completed, stream, count):
        self.store.batches.append({
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

        batch_index = self.store.batch_count() - 1

        for task in tasks:
            check_dependency("id" in task and task["id"] is not None, "Invalid task")
            self.store.map_task(task, batch_index)

            task_id = task["id"]

            if "task_workers" in task and len(task["task_workers"]) > 0:
                for taskworker in task["task_workers"]:
                    self._replay_task(project_key, task_id, taskworker)

    def _peer_review(self, project_key, worker_responses, review_completed, inter_task_review=False):
        task_workers = [response['id'] for response in worker_responses]

        tasks = {}
        for response in worker_responses:
            if not response["task_id"] in tasks:
                tasks[response["task_id"]] = []

            if not response["worker_id"] in tasks[response["task_id"]]:
                tasks[response["task_id"]].append(response["worker_id"])

        unique_workers = any([tasks[task_id] > 1 for task_id in tasks.keys()])

        check_dependency(len(task_workers) > 1, "Peer review requires more than 1 worker responses")
        check_dependency(unique_workers, "Peer review requires more than 1 worker to complete the tasks. All tasks were completed by same worker.")

        response = self.api_client.launch_peer_review(task_workers, inter_task_review, self.rerun_key)

        match_group_id = str(response['match_group_id'])

        if match_group_id not in self.store.cache:
            self.store.cache[match_group_id] = {}

        if review_completed is not None:
            self.store.cache[match_group_id]['project_key'] = project_key
            self.store.cache[match_group_id]['review_completed'] = review_completed
            self.store.cache[match_group_id]['is_complete'] = False

            if "scores" in response and len(response["scores"]) > 0:
                self._replay_review(project_key, match_group_id, response["scores"])

    def _replay_task(self, project_key, task_id, task_worker):
        # re-queue submitted results
        log.info(msg="syncing previous submissions...")

        payload = json.dumps({
            "type": "REGULAR",
            "payload": {
                "taskworker_id": task_worker["id"],
                "task_id": task_id,
                "worker_id": task_worker["worker"],
                "project_key": project_key,
                "project_id": task_worker["project_data"]["id"],
                "taskworker": task_worker
            }
        })

        self.queue.put({
            "payload": payload,
            "isBinary": False
        })

    def _replay_review(self, project_key, match_group_id, scores):
        # re-queue submitted results
        log.info(msg="syncing previous peer review submissions...")
        payload = json.dumps({
            "type": "REVIEW",
            "payload": {
                "project_key": project_key,
                "match_group_id": match_group_id,
                "scores": scores
            }
        })

        self.queue.put({
            "payload": payload,
            "isBinary": False
        })

    def _mock_task(self, task_id, mock_workers):
        log.info(msg="mocking workers...")

        task = self._fetch_task(task_id)

        if task is not None:
            num_workers = task["project"]["num_workers"]

            responses = mock_workers(task, num_workers)

            check_dependency(responses is not None and len(
                responses) == num_workers, "Incorrect number of responses. Result=%d. Expected=%d" % (
                                 len(responses), num_workers))

            results = [
                {
                    "items": [
                        {
                            "result": field["value"],
                            "template_item": get_template_item_id(field["name"],
                                                                  task["template"]["fields"])
                        } for field in response]
                } for response in responses]

            self.api_client.submit_results(
                task["id"],
                results
            )

    def _read_message(self):
        while self.queue is not None:
            data = self.queue.get(block=True)

            # to stop reading, None is passed to the queue by server/client
            if data is None:
                break

            log.debug(msg="got new message")

            if not data["isBinary"]:
                log.debug("<<<{}>>>".format(data["payload"].decode("utf8")))

            thread = callback_thread(
                name='message processor',
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
        log.debug(msg="processing message...")

        if not isBinary:
            response = json.loads(payload.decode("utf8"))

            type = response.get("type", "REGULAR")
            payload = response.get('payload')

            if type == "REVIEW":
                return self._process_review(payload)

            if type == "REGULAR":
                return self._process_task(payload)

    def _process_task(self, payload):
        taskworker_id = int(payload.get("taskworker_id", 0))
        task_id = int(payload.get("task_id", 0))
        worker_id = int(payload.get("worker_id", 0))
        project_key = payload.get("project_key", None)
        taskworker = payload.get("taskworker", None)

        if task_id in self.store.tasks:
            check_dependency(taskworker_id > 0, Error.required("taskworker_id"))
            check_dependency(task_id > 0, Error.required("task_id"))
            check_dependency(project_key is not None, Error.required("project_key"))

            batch_indices = self.store.tasks[task_id]["batches"]

            if taskworker is None:
                task_data = self.api_client.get_task_results_by_taskworker_id(taskworker_id)
                task_data = transform_task_results(task_data)
            else:
                task_data = transform_task_results(taskworker)

            for batch_index in batch_indices:
                check_dependency(batch_index < len(self.store.batches) \
                                 and self.store.batches[batch_index] is not None, "Missing batch for task")

                check_dependency(task_data is not None, "No worker responses for task %d found" % task_id)

                config = self.store.batches[batch_index]

                task_data["accept"] = False
                approve = config["approve"]
                completed = config["completed"]
                stream = config["stream"]

                # increment count to track completion
                self.store.batches[batch_index]["submissions"][task_id] += 1

                if stream:
                    self._stream_response(batch_index, task_id, task_data, approve, completed)
                else:
                    self._aggregate_responses(batch_index, task_id, task_data, approve, completed)

                self.check_for_pending_tasks_reviews()
        else:
            log.debug("No corresponding task found. Worker response ignored.")

    def _process_review(self, payload):
        match_group_id = str(payload["match_group_id"])

        if "scores" in payload and len(payload["scores"]) > 0:
            ratings = payload["scores"]
        else:
            ratings = self.api_client.get_trueskill_scores(match_group_id)

        if match_group_id in self.store.cache:
            project_key = self.store.cache[match_group_id]['project_key']
            review_completed = self.store.cache[match_group_id]['review_completed']
            review_completed(project_key, ratings)
            self.store.cache[match_group_id]['is_complete'] = True

            self.check_for_pending_tasks_reviews()

    def check_for_pending_tasks_reviews(self):
        if all([self.store.all_batches_complete(), self.store.all_reviews_complete()]):
            log.debug(msg="all tasks and reviews complete.")
            self._stop()

    def _review_completed(self, project_key, ratings, ignore_history=True):
        self.rate(project_key, ratings, ignore_history=ignore_history)

    def _stream_response(self, batch_index, task_id, task_data, approve, completed):
        log.info(msg="streaming responses...")

        log.info(msg="calling approve callback...")

        if approve([task_data]):
            task_data["accept"] = True
            log.info(msg="task %d approved" % task_id)
        else:
            log.info(msg="task %d rejected" % task_id)

        self.api_client.update_approval_status(task_data)

        if task_data["accept"]:
            log.info(msg="calling completed callback")
            completed([task_data])

        is_done = self.store.is_task_complete(batch_index, task_id)

        if is_done:
            self.store.mark_task_completed(batch_index, task_id)

    def _aggregate_responses(self, batch_index, task_id, task_data, approve, completed):
        log.info(msg="aggregating responses...")

        # store it for aggregation (stream = False)
        self.store.aggregate(batch_index, task_id, task_data)

        is_done = self.store.is_task_complete(batch_index, task_id)

        if is_done:
            self.store.mark_task_completed(batch_index, task_id)

            is_done = self.store.is_batch_complete(batch_index)

            if is_done:
                self.store.mark_batch_completed(batch_index)

                tasks_data = self.store.get_aggregated(batch_index)

                log.info(msg="calling approve callback...")
                approvals = approve(tasks_data)

                tasks_approvals = zip(tasks_data, approvals)

                for task_approval in tasks_approvals:
                    task_data = task_approval[0]
                    approval = task_approval[1]

                    task_data["accept"] = approval

                    if approval:
                        log.info(msg="task %d approved" % task_data.get("id"))
                    else:
                        log.info(msg="task %d rejected" % task_data.get("id"))

                    self.api_client.update_approval_status(task_data)

                approved_tasks = [x[0] for x in zip(tasks_data, approvals) if x[1]]

                log.info(msg="calling completed callback...")
                completed(approved_tasks)

    def _fetch_task(self, task_id):
        data = self.api_client.fetch_task(task_id)
        return transform_task(data)

    def _open_channel(self):
        # shared queue between main process and channel for message passing
        self.queue = multiprocessing.Queue()

        # this thread reads message received in the queue via channel
        thread = callback_thread(name='message reader', target=self._read_message)
        thread.start()

        # this thread waits for kill signal for channel
        thread = callback_thread(name='signal monitor', target=signal.pause)
        thread.start()

        signal.signal(signal.SIGINT, self._handler)

        access_token = self.api_client.get_auth_token()
        subscribe_url = self.websock_proto + self.host + self.api_client.route.subscribe

        self.channel = Channel(self.queue, access_token, subscribe_url)
        self.channel.start()

    def _handler(self, signum, frame):
        if signum in [signal.SIGINT, signal.SIGTERM, signal.SIGABRT] and os.getpid() == self.channel.get_pid():
            self.channel.stop()

            if self.queue is not None:
                self.queue.put(None)
                self.queue = None

    def _stop(self):
        log.info(msg="disconnecting...")
        os.kill(self.channel.get_pid(), signal.SIGINT)
