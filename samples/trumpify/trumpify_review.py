import os
import sys
import threading
import time

from .twitter_client import TwitterClient

sys.path.append(os.path.abspath('../../'))

from daemo.client import DaemoClient

CREDENTIALS_FILE = 'credentials.json'

PROJECT_ID = ''
REVIEW_PROJECT_ID = ''

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

    client.publish(project_key=PROJECT_ID, tasks=[{
        "id": id,
        "tweet": text
    }], approve=approve_tweet, completed=create_review_task, stream=True)


def approve_tweet(results):
    approvals = []
    for result in results:
        text = result.get('results')[0].get('result')
        is_approved = len(text) > 0
        approvals.append(is_approved)
    return approvals


def create_review_task(results):
    tasks = [{
                 "id": result.get('results')[0].get('id'),
                 "tweet_result": result.get('results')[0].get('result')
             } for result in results]

    client.publish(
        project_key=REVIEW_PROJECT_ID,
        tasks=tasks,
        approve=approve_review,
        completed=post_to_twitter,
        stream=True
    )


def approve_review(results):
    approvals = []
    for result in results:
        text = result.get('results')[0].get('result')
        is_approved = len(text) > 0
        approvals.append(is_approved)
    return approvals


def post_to_twitter(results):
    for result in results:
        text = result.get('task_data').get('tweet_result')
        twitter.post(text)


thread = threading.Thread(target=fetch_new_tweets, args=(TWEET_COUNT, TIMESPAN_MIN))
thread.start()

client.publish(
    project_key=PROJECT_ID,
    tasks=[],
    approve=approve_tweet,
    completed=create_review_task,
    stream=True
)
