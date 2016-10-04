import json
import logging

import sys
from rainbow_logging_handler import RainbowLoggingHandler

from daemo.client import DaemoClient

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
formatter = logging.Formatter("[%(asctime)s] %(name)s: %(message)s")  # same as default

handler = RainbowLoggingHandler(sys.stderr, color_asctime=('yellow', None, False))
handler.setFormatter(formatter)
log.addHandler(handler)

# Remember any task launched under this rerun key, so you can debug or resume them by re-running
RERUN_KEY = ''
BATCH = RERUN_KEY

PROJECT_KEY = ''

# Create the client
client = DaemoClient(rerun_key=RERUN_KEY)


def load_gold_data():
    gold_data = None

    filename = "data/gold_data.json"
    with open(filename, "r") as source:
        gold_data = json.load(source)

    return gold_data


def load_batch_tasks():
    tasks = None

    filename = "data/batch%s.json" % BATCH
    with open(filename, "r") as source:
        tasks = json.load(source)

    return tasks


def get_task_id(worker_response):
    """
    Find out the unique ID from the task input in a worker's submission

    :param worker_response: submission made by a worker for a task
    :return: ID value from task_data for the submission
    """
    return worker_response.get('task_data').get('id')


def get_article_stance(worker_response):
    """
    Filter out image classification from a worker's complete submission

    :param worker_response: submission made by a worker for a task
    :return: image category/label
    """
    return worker_response.get('fields').get('stance')


def classify_articles():
    """
    Post images to Daemo server for classification
    """
    tasks = load_batch_tasks()

    if tasks is not None:
        client.publish(
            project_key=PROJECT_KEY,
            tasks=tasks,
            approve=approve,
            completed=completed
        )


def approve(worker_responses):
    """
    The approve callback is called when work is complete; it receives
    a list of worker responses. Return a list of True (approve) and
    False (reject) values. Approved tasks are passed on to the
    completed callback, and	rejected tasks are automatically relaunched.
    """

    approvals = [get_article_stance(response) is not None and len(get_article_stance(response)) > 0 for response in
                 worker_responses]

    return approvals


def completed(worker_responses):
    """
    Once tasks are approved, the completed callback is sent a list of
    final approved worker responses. Perform any computation that you
    want on the results. Don't forget to send Daemo the	rating scores
    so that it can improve and find better workers next time.
    """

    gold_data = load_gold_data()

    # rate only those workers who did gold set
    trained_workers = [response for response in worker_responses if str(get_task_id(response)) in gold_data]

    ratings = [{
                   "task_id": worker_response.get("task_id"),
                   "worker_id": worker_response.get("worker_id"),
                   "weight": 1 if gold_data[str(get_task_id(worker_response))] == get_article_stance(
                       worker_response) else 0
               } for worker_response in trained_workers]

    client.rate(project_key=PROJECT_KEY, ratings=ratings)


classify_articles()
