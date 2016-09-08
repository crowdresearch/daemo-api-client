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
        completed=post_to_twitter, stream=True
    )


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
    Post worker's response to twitter and add to monitoring list

    :param worker_responses: submission made by a worker for a task
    """
    for worker_response in worker_responses:
        twitter.post(worker_response)


def rate_worker_responses(interval):
    """
    Use tweet's retweet count in first "interval" seconds after it is posted as criterion for rating workers which is
    pushed to Daemo's reputation system

    :param interval: period in seconds to wait before checking retweet count for a tweet
    """
    while True:
        tweet = twitter.monitor_next_tweet(interval)

        if tweet is not None:
            retweet_count = twitter.get_retweet_count(tweet_id=tweet.get('id'))

            rating = {
                "task_id": tweet.get("task_id"),
                "worker_id": tweet.get("worker_id"),
                "weight": retweet_count
            }

            daemo.rate(project_key=PROJECT_KEY, ratings=[rating])


thread = threading.Thread(target=transform_new_tweets,
                          args=(INPUT_TWITTER_NAME, TWEET_COUNT, FETCH_INTERVAL_MIN * MINUTES))
thread.start()

thread = threading.Thread(target=rate_worker_responses,
                          args=(MONITOR_INTERVAL_MIN * MINUTES,))
thread.start()
