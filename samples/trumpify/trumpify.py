import os
import sys
import threading
import time

sys.path.append(os.path.abspath('../../'))

from samples.trumpify.utils import TwitterUtils
from daemo.client import DaemoClient

CREDENTIALS_FILE = 'credentials.json'

PROJECT_ID = -1

INPUT_TWITTER_NAME = 'HillaryClinton'
MINUTES = 60
FETCH_INTERVAL_MIN = 5
TWEET_COUNT = 10
MONITOR_INTERVAL_MIN = 60

twitter = TwitterUtils()
client = DaemoClient(CREDENTIALS_FILE)


def fetch_new_tweets(count, interval):
    while True:
        messages = twitter.fetch_tweets(twitter_name=INPUT_TWITTER_NAME, count=count, interval=interval)

        if len(messages) > 0:
            for message in messages:
                post_to_daemo(message)
        else:
            print "@%s has not tweeted in the last %d minutes." % (INPUT_TWITTER_NAME, interval)

        time.sleep(interval)


def post_to_daemo(message):
    text = message.get('text')
    id = message.get('id')

    client.publish(project_id=PROJECT_ID, tasks=[{
        "id": id,
        "tweet": text
    }], approve=approve_tweet, completed=post_to_twitter, stream=False)


def get_tweet_text(worker_response):
    return worker_response.get('results')[0].get('result')


def approve_tweet(worker_responses):
    approvals = []
    for worker_response in worker_responses:
        text = get_tweet_text(worker_response)
        is_approved = len(text) > 0
        approvals.append(is_approved)
    return approvals


def post_to_twitter(worker_responses):
    for worker_response in worker_responses:
        twitter.post(worker_response)


def fetch_retweet_count(interval):
    while True:
        tweet = twitter.monitor_next_tweet(interval)

        if tweet is not None:
            # check for retweet count after X min of posting to twitter
            retweet_count = twitter.get_retweet_count(tweet_id=tweet.get('id'))

            rating = {
                "task_id": tweet.get("task_id"),
                "worker_id": tweet.get("worker_id"),
                "weight": retweet_count
            }

            client.update_rating(project_id=PROJECT_ID, ratings=[rating])


thread = threading.Thread(target=fetch_new_tweets, args=(TWEET_COUNT, FETCH_INTERVAL_MIN * MINUTES))
thread.start()

thread = threading.Thread(target=fetch_retweet_count, args=(MONITOR_INTERVAL_MIN * MINUTES,))
thread.start()
