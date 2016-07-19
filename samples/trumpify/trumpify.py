import os
import sys
import threading
import time

sys.path.append(os.path.abspath('../../'))

from samples.trumpify.twitter_client import TwitterClient
from daemo.client import DaemoClient

CREDENTIALS_FILE = 'credentials.json'

PROJECT_ID = -1

TW_CONSUMER_KEY = ''
TW_CONSUMER_SECRET = ''
TW_ACCESS_TOKEN = ''
TW_ACCESS_TOKEN_SECRET = ''

INPUT_TWITTER_NAME = 'HillaryClinton'
TIMESPAN_MIN = 5
TWEET_COUNT = 10

twitter = TwitterClient(
    consumer_key=TW_CONSUMER_KEY,
    consumer_secret=TW_CONSUMER_SECRET,
    token=TW_ACCESS_TOKEN,
    token_secret=TW_ACCESS_TOKEN_SECRET
)
client = DaemoClient(CREDENTIALS_FILE)


def fetch_new_tweets(count, interval):
    while True:
        messages = twitter.fetch_tweets(twitter_name=INPUT_TWITTER_NAME, count=count, interval=interval)

        if len(messages) > 0:
            for message in messages:
                post_to_daemo(message)
        else:
            print "@%s has not tweeted in the last %d minutes." % (INPUT_TWITTER_NAME, interval)

        time.sleep(interval * 60)


def post_to_daemo(message):
    text = message.get('text')
    id = message.get('id')

    client.publish(project_id=PROJECT_ID, tasks=[{
        "id": id,
        "tweet": text
    }], approve=approve_tweet, completed=post_to_twitter, stream=True)


def approve_tweet(results):
    approvals = []
    for result in results:
        text = result.get('results')[0].get('result')
        is_approved = len(text) > 0
        approvals.append(is_approved)
    return approvals


def post_to_twitter(results):
    for result in results:
        tweet = twitter.post(result.get('results')[0].get('result'))
        twitter.store(result, tweet)


def fetch_retweet_count():
    interval = 3600  # 1 hr

    while True:
        tweet = twitter.get_tweet_response()

        if tweet is not None:
            seconds_elapsed= twitter.seconds_left(timestamp=tweet.get('created_at'))
            seconds_left = interval - seconds_elapsed

            if seconds_left > 0:
                # check after 1 hr of tweet posting
                time.sleep(seconds_left)

            retweet_count = twitter.get_retweet_count(tweet_id=tweet.get('id'))

            rating = {
                "task_id": tweet.get("task_id"),
                "worker_id": tweet.get("worker_id"),
                "weight": retweet_count
            }

            client.update_rating(project_id=PROJECT_ID, ratings=[rating])


thread = threading.Thread(target=fetch_new_tweets, args=(TWEET_COUNT, TIMESPAN_MIN))
thread.start()

thread = threading.Thread(target=fetch_retweet_count)
thread.start()

client.publish(
    project_id=PROJECT_ID,
    tasks=[],
    approve=approve_tweet,
    completed=post_to_twitter,
    stream=True
)
