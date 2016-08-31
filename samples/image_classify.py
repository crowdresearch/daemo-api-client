import logging
import os
import sys

sys.path.append(os.path.abspath('../../'))

from daemo.client import DaemoClient

CREDENTIALS_FILE = 'credentials.json'

PROJECT_KEY = ''
RERUN_KEY = ''

PROJECT_KEY = 'kQm0M6Aw4VPX'
RERUN_KEY = 'firstrun'

logger = logging.getLogger()
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('log.txt')
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)

daemo = DaemoClient(credentials_path=CREDENTIALS_FILE, rerun_key=RERUN_KEY)


def classify_images():
    """
    Post images to Daemo server for classification
    """
    daemo.publish(
        project_key=PROJECT_KEY,
        tasks=[
            {
                "image_url": "http://cdn-media-2.lifehack.org/wp-content/files/2015/08/Your-Words-Affect-Your-Mind-10-Things-Happy-People-Say-Every-Day.jpg"
            },
            # {
            #     "image_url": "http://i.dailymail.co.uk/i/pix/2014/06/20/1403279579539_wps_2_29_Mar_2013_Cape_Town_Sou.jpg"
            # }
        ],
        approve=approve_correct_response,
        completed=rate_workers
    )


def get_response_text(worker_response):
    """
    Filter out classification (True/False) from a worker's complete submission

    :param worker_response: submission made by a worker for a task
    :return: actual tweet text
    """
    return worker_response.get('fields').get('is_happy')


def approve_correct_response(worker_responses):
    """
    Verify each worker response if it meets the requirements

    :param worker_responses: submission made by a worker for a task
    :return: list of True/False
    """
    approvals = [get_response_text(response) is not None and len(get_response_text(response)) > 0 for response in
                 worker_responses]
    return approvals


def rate_workers(worker_responses):
    """
    Rate all responses by workers which got approved

    :param worker_responses: submission made by a worker for a task
    """
    for worker_response in worker_responses:
        rate(worker_response)


def rate(worker_response):
    """
    Rate an approved worker's response based on length of response

    :param worker_response: submission made by a worker for a task
    """
    if worker_response is not None:
        image_url = worker_response.get('task_data').get('image_url').lower()
        response = get_response_text(worker_response)

        if 'happy' in image_url:
            weight = 3 if response == 'Yes' else 1
        else:
            weight = 3 if response == 'No' else 1

        rating = {
            "task_id": worker_response.get("task_id"),
            "worker_id": worker_response.get("worker_id"),
            "weight": weight
        }

        daemo.rate(project_key=PROJECT_KEY, ratings=[rating])


classify_images()
