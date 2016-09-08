from __future__ import division

import threading
import time

from daemo.client import DaemoClient
from samples.utils import TwitterUtils

PROJECT_KEY = ''
REVIEW_PROJECT_KEY = ''
RERUN_KEY = ''

INPUT_TWITTER_NAME = 'HillaryClinton'
MINUTES = 60
FETCH_INTERVAL_MIN = 5
TWEET_COUNT = 10
MONITOR_INTERVAL_MIN = 60

twitter = TwitterUtils()
daemo = DaemoClient(rerun_key=RERUN_KEY)


def transform_new_tweets(twitter_name, count, interval):
    """
    Fetches "count" number of tweets from twitter feed of "twitter_name" after every "interval" seconds and posts them
    to Daemo server for translation

    :param twitter_name: twitter account to fetch tweets from
    :param count: number of tweets to fetch
    :param interval: period in seconds to repeat the process
    """
    while True:
        messages = twitter.fetch_tweets(twitter_name=twitter_name, count=count, interval=interval)

        if len(messages) > 0:
            for message in messages:
                translate_to_trump_version(message)
        else:
            print "@%s has not tweeted in the last %d minutes." % (twitter_name, int(interval / MINUTES))

        time.sleep(interval)


def translate_to_trump_version(message):
    """
    Create a Daemo task using "message" as data and pass it to workers for translation

    :param message: tweet object to be used as data for daemo task
    """
    text = message.get('text')
    id = message.get('id')

    daemo.publish(
        project_key=PROJECT_KEY,
        tasks=[{
            "id": id,
            "tweet": text
        }],
        approve=approve_tweet,
        completed=create_review_task,
        stream=True
    )


def get_tweet(worker_response):
    """
    Filter out just the tweet text from task data

    :param worker_response: submission made by a worker for a task
    :return: actual tweet input
    """
    return worker_response.get('task_data').get('tweet')


def get_tweet_response(worker_response):
    """
    Filter out just the tweet text from a worker's complete submission

    :param worker_response: submission made by a worker for a task
    :return: actual tweet text
    """
    return worker_response.get('fields').get('tweet_result')


def get_tweet_rating(worker_response):
    """
    Filter out just the tweet rating from peer review

    :param worker_response: submission made by a worker for a task
    :return: actual tweet text
    """
    return int(worker_response.get('fields').get('rating')[0])


def approve_tweet(worker_responses):
    """
    Verify each worker response if it meets the requirements

    :param worker_responses: submission made by a worker for a task
    :return: list of True/False
    """
    approvals = [len(get_tweet_response(response)) > 0 for response in worker_responses]
    return approvals


def create_review_task(worker_responses):
    """
    Create a task on Daemo server for reviewing worker submissions

    :param worker_responses: submission made by a worker for a task
    """
    tasks = [{
                 "tweet": get_tweet(worker_response),
                 "tweet_result": get_tweet_response(worker_response)
             } for worker_response in worker_responses]

    daemo.publish(
        project_key=REVIEW_PROJECT_KEY,
        tasks=tasks,
        approve=approve_review,
        completed=post_to_twitter
    )


def approve_review(worker_responses):
    """
    Verify each worker response if it meets the requirements

    :param worker_responses: submission made by a worker for a review task
    :return: list of True/False
    """
    approvals = [get_tweet_rating(response) > 0 for response in worker_responses]
    return approvals


def post_to_twitter(approved_responses):
    """
    Post worker's response to twitter

    :param approved_responses: submission made by a worker for a task
    """
    ratings = [get_tweet_rating(response) for response in approved_responses]
    avg_rating = float(reduce(lambda x, y: x + y, ratings)) / len(ratings)
    text = approved_responses[0].get('task_data').get('tweet_result')

    if avg_rating > 4:
        twitter.post(text)


thread = threading.Thread(target=transform_new_tweets,
                          args=(INPUT_TWITTER_NAME, TWEET_COUNT, FETCH_INTERVAL_MIN * MINUTES))
thread.start()
