import logging
import os
import sys

import json

sys.path.append(os.path.abspath('../../'))

from daemo.client import DaemoClient

PROJECT_KEY = ''
RERUN_KEY = ''
BATCH = ''

logger = logging.getLogger()
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('log.txt')
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)

daemo = DaemoClient(rerun_key=RERUN_KEY)


def load_training_data():
    training_data = None

    filename = "gold_truth.json"
    with open(filename, "r") as source:
        training_data = json.load(source)

    return training_data


def load_batch_tasks():
    tasks = None

    filename = "batch%s.json" % BATCH
    with open(filename, "r") as source:
        tasks = json.load(source)

    return tasks


def classify_images():
    """
    Post images to Daemo server for classification
    """
    tasks = load_batch_tasks()

    if tasks is not None:
        daemo.publish(
            project_key=PROJECT_KEY,
            tasks=tasks,
            approve=approve_correct_response,
            completed=rate_workers
        )


def get_task_id(worker_response):
    """
    Find out the unique ID from the task input in a worker's submission

    :param worker_response: submission made by a worker for a task
    :return: ID value from task_data for the submission
    """
    return worker_response.get('task_data').get('ID')


def get_image_category(worker_response):
    """
    Filter out image classification from a worker's complete submission

    :param worker_response: submission made by a worker for a task
    :return: image category/label
    """
    return worker_response.get('fields').get('category')


def approve_correct_response(worker_responses):
    """
    Verify each worker response if it meets the requirements
    All responses are approved in this case

    :param worker_responses: submission made by a worker for a task
    :return: list of True/False
    """
    approvals = [get_image_category(response) is not None and len(get_image_category(response)) > 0 for response in
                 worker_responses]
    return approvals


def rate_workers(worker_responses):
    """
    Rate all responses by workers which got approved

    :param worker_responses: submission made by a worker for a task
    """
    training_set = load_training_data()

    # rate only those workers who did training set
    trained_workers = [response for response in worker_responses if str(get_task_id(response)) in training_set]

    ratings = [{
        "task_id": worker_response.get("task_id"),
        "worker_id": worker_response.get("worker_id"),
        "weight": 3 if training_set[str(get_task_id(worker_response))] == get_image_category(worker_response) else 1
    } for worker_response in trained_workers]

    daemo.rate(project_key=PROJECT_KEY, ratings=ratings)


classify_images()
