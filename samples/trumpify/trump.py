import os
import sys
import time
from threading import Thread

from twitter import *

sys.path.append(os.path.abspath('../../'))

from daemo.client import Client

CREDENTIALS_FILE = 'credentials.json'

PROJECT_ID = -1

TW_CONSUMER_KEY = ''
TW_CONSUMER_SECRET = ''
TW_ACCESS_TOKEN = ''
TW_ACCESS_TOKEN_SECRET = ''

INPUT_TWITTER_NAME = 'HillaryClinton'
TIMESPAN_MIN = 5

auth = OAuth(
    consumer_key=TW_CONSUMER_KEY,
    consumer_secret=TW_CONSUMER_SECRET,
    token=TW_ACCESS_TOKEN,
    token_secret=TW_ACCESS_TOKEN_SECRET
)

print "Initializing..."
twitter = Twitter(auth=auth)
client = Client(CREDENTIALS_FILE)


def fetch_new_tweets(count, interval):
    last_id = None

    while True:
        messages = twitter.statuses.user_timeline(
            screen_name=INPUT_TWITTER_NAME,
            exclude_replies=True,
            include_rts=False,
            count=count
        )

        message = messages[0]
        new_id = message.get('id')

        if last_id is None or last_id != new_id:
            push_to_daemo(message)
            last_id = new_id
        else:
            print "%s has not tweeted in the last %d minutes." % (INPUT_TWITTER_NAME, TIMESPAN_MIN)

        time.sleep(interval)


def push_to_daemo(message):
    text = message.get('text')
    id = message.get('id')

    client.add_data(project_id=PROJECT_ID, data={"tasks": [{
        "tweet": text, "id": id
    }]})


def approve(result):
    text = result.get('results')[0].get('result')
    return len(text) > 0


def post_to_twitter(result):
    text = result.get('results')[0].get('result')

    try:
        twitter.statuses.update(status=text)
    except Exception as e:
        print e.message


thread = Thread(target=fetch_new_tweets, args=(1, TIMESPAN_MIN * 60))
thread.daemon = True
thread.start()

client.publish(project_id=PROJECT_ID, approve=approve, completed=post_to_twitter, stream=True)
