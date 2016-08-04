import logging
import os
import sys

sys.path.append(os.path.abspath('../../'))

from daemo.client import DaemoClient

CREDENTIALS_FILE = 'credentials.json'

PROJECT_ID = ''
RERUN_KEY = ''

logger = logging.getLogger()
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logger.setLevel(logging.DEBUG)

daemo = DaemoClient(CREDENTIALS_FILE, rerun_key=RERUN_KEY)


def classify_images():
    daemo.publish(
        project_key=PROJECT_ID,
        tasks=[
            {
                "image_url": "http://cdn-media-2.lifehack.org/wp-content/files/2015/08/Your-Words-Affect-Your-Mind-10-Things-Happy-People-Say-Every-Day.jpg"},
            {
                "image_url": "http://i.dailymail.co.uk/i/pix/2014/06/20/1403279579539_wps_2_29_Mar_2013_Cape_Town_Sou.jpg"
            },
            {
                "image_url": "http://media3.popsugar-assets.com/files/thumbor/YZygkdjNf5Vmn8snnQbKFg9dQKs/fit-in/2048xorig/filters:format_auto-!!-:strip_icc-!!-/2014/06/30/080/n/28443503/1c00786033691f2c_shutterstock_114882880/i/1-Most-people-SAD-women.jpg"
            },
            {
                "image_url": "https://grainemediationblog.files.wordpress.com/2013/11/group-of-happy-people-2.jpg"
            },
            {
                "image_url": "http://forums.steves-digicams.com/attachments/people-photos/132254d1229797858-sad-little-boy-advice-re-post-production-required-aaimg_3570.jpg"
            }
        ],
        approve=approve_correct_response,
        completed=rate_workers
    )


def get_response_text(worker_response):
    """
    Filter out just the tweet text from a worker's complete submission

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
    approvals = [get_response_text(response) is not None and len(get_response_text(response)) > 0 for response in worker_responses]
    return approvals


def rate_workers(worker_responses):
    """
    Post worker's response to twitter and add to monitoring list

    :param worker_responses: submission made by a worker for a task
    """
    for worker_response in worker_responses:
        print get_response_text(worker_response)
        rate(worker_response)


def rate(worker_response):
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

        daemo.rate(project_key=PROJECT_ID, ratings=[rating])


classify_images()
