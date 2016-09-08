import time

from daemo.client import DaemoClient
from samples.utils import NYTUtils

PROJECT_KEY = ''
RERUN_KEY = ''

nyt = NYTUtils()
daemo = DaemoClient(rerun_key=RERUN_KEY)


def transform_top_news(interval):
    """
    Fetches top news from New York Times after every "interval" seconds and posts them
    to Daemo server for translation to Dr. Suess version

    :param interval: period in seconds to repeat the process
    """
    while True:
        messages = nyt.fetch_top_news()

        if len(messages) > 0:
            for message in messages:
                translate_to_dr_suess_version(message)
        else:
            print "No top news released by NY Times recently"

        time.sleep(interval * 3600)


def translate_to_dr_suess_version(item):
    """
    Create a Daemo task using "title" and "url" as data and pass it to workers for translation

    :param message: news item to be used as data for daemo task
    """
    title = item.get('title')
    url = item.get('url')

    daemo.publish(
        project_key=PROJECT_KEY,
        tasks=[{
            "title": title,
            "url": url
        }],
        approve=approve_responses,
        completed=show_approved_responses
    )


def get_suess_text(worker_response):
    """
    Filter out just the suess version from a worker's complete submission

    :param worker_response: submission made by a worker for a task
    :return: suess translated text
    """
    return worker_response.get('fields').get('suess_version')


def approve_responses(worker_responses):
    """
    Verify each worker response if it meets the requirements

    :param worker_responses: submission made by a worker for a task
    :return: list of True/False
    """
    approvals = [get_suess_text(response) is not None and len(get_suess_text(response)) > 0 for response in
                 worker_responses]
    return approvals


def show_approved_responses(worker_responses):
    """
    Rate an approved worker's response based on length of response

    :param worker_responses: submission made by a worker for a task
    """
    for worker_response in worker_responses:
        rating = {
            "task_id": worker_response.get("task_id"),
            "worker_id": worker_response.get("worker_id"),
            "weight": 3 if get_suess_text(worker_response) is not None and len(
                get_suess_text(worker_response)) > 0 else 1
        }

        daemo.rate(project_key=PROJECT_KEY, ratings=[rating])


transform_top_news(12)
