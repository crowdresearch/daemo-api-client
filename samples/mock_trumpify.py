import threading
import time

from daemo.client import DaemoClient
from samples.utils import TwitterUtils


PROJECT_KEY = ''
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
        completed=post_to_twitter,
        mock_workers=mock_workers
    )


def mock_workers(task, num_workers):
    """
    Simulate workers responses to verify the workflow

    :param task: task object with all the fields and available choices
    :param num_workers: number of workers who will perform this task
    :return: task_result object which provides key-value map for each field and the result
    """
    results = [
        [{
            "name": "tweet",
            "value": "%d. Trump Trump everywhere not a Hillary to see." % x
        }] for x in range(num_workers)]
    return results


def get_tweet_text(worker_response):
    """
    Filter out just the tweet text from a worker's complete submission

    :param worker_response: submission made by a worker for a task
    :return: actual tweet text
    """
    return worker_response.get('fields').get('tweet')


def approve_tweet(worker_responses):
    """
    Verify each worker response if it meets the requirements

    :param worker_responses: submission made by a worker for a task
    :return: list of True/False
    """
    approvals = [len(get_tweet_text(response)) > 0 for response in worker_responses]
    return approvals


def post_to_twitter(worker_responses):
    """
    Post worker's response to twitter

    :param worker_responses: submission made by a worker for a task
    """
    for worker_response in worker_responses:
        print get_tweet_text(worker_response)


thread = threading.Thread(target=transform_new_tweets,
                          args=(INPUT_TWITTER_NAME, TWEET_COUNT, FETCH_INTERVAL_MIN * MINUTES))
thread.start()
