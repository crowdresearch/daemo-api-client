import os
import sys
import threading
import time
from datetime import datetime
from email._parseaddr import mktime_tz, parsedate_tz

from twitter import *

sys.path.append(os.path.abspath('../../'))

from daemo.client import Client

CREDENTIALS_FILE = 'credentials.json'

PROJECT_ID = -1
REVIEW_PROJECT_ID = -1

TW_CONSUMER_KEY = ''
TW_CONSUMER_SECRET = ''
TW_ACCESS_TOKEN = ''
TW_ACCESS_TOKEN_SECRET = ''

INPUT_TWITTER_NAME = 'HillaryClinton'
TIMESPAN_MIN = 5
TWEET_COUNT = 10

auth = OAuth(
    consumer_key=TW_CONSUMER_KEY,
    consumer_secret=TW_CONSUMER_SECRET,
    token=TW_ACCESS_TOKEN,
    token_secret=TW_ACCESS_TOKEN_SECRET
)

print "Initializing..."
twitter = Twitter(auth=auth)
client = Client(CREDENTIALS_FILE)


def to_local_time(tweet_timestamp):
    """Convert rfc 5322 -like time string into a local time
       string in rfc 3339 -like format.
    """
    timestamp = mktime_tz(parsedate_tz(tweet_timestamp))
    return datetime.fromtimestamp(timestamp)


def is_from_last_interval(message, interval):
    current_time = datetime.now()
    created_at = to_local_time(message.get('created_at'))
    delta = current_time - created_at
    return 0 < delta.total_seconds() < (interval * 60)


def fetch_new_tweets(count, interval):
    while True:
        print "Fetching new tweets..."
        messages = twitter.statuses.user_timeline(
            screen_name=INPUT_TWITTER_NAME,
            exclude_replies=True,
            include_rts=False,
            count=count
        )

        # get messages within last interval
        messages_last_interval = [message for message in messages if is_from_last_interval(message, interval)]

        if len(messages_last_interval) > 0:
            for message in messages_last_interval:
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
    }], approve=approve_tweet, completed=create_review_task, stream=True)


def approve_tweet(results):
    approvals = []
    for result in results:
        text = result.get('results')[0].get('result')
        is_approved = len(text) > 0
        approvals.append(is_approved)
    return approvals


def create_review_task(results):
    for result in results:
        id = result.get('results')[0].get('id')
        text = result.get('results')[0].get('result')

        client.publish(
            project_id=REVIEW_PROJECT_ID,
            tasks=[{
                "id": id,
                "tweet_result": text
            }],
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

        try:
            twitter.statuses.update(status=text)
        except Exception as e:
            print e.message


thread = threading.Thread(target=fetch_new_tweets, args=(TWEET_COUNT, TIMESPAN_MIN))
thread.start()

client.publish(
    project_id=PROJECT_ID,
    tasks=[],
    approve=approve_tweet,
    completed=create_review_task,
    stream=True
)
