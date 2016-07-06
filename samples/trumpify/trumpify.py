import os
import sys

from twitter import *

sys.path.append(os.path.abspath('../../'))

from daemo.client import Client

CREDENTIALS_FILE = 'credentials.json'

PROJECT_ID = os.getenv('PROJECT_ID', False)
assert PROJECT_ID, "Missing environ variable PROJECT_ID"

PROJECT_ID = int(PROJECT_ID)

TW_CONSUMER_KEY = os.getenv('TW_CONSUMER_KEY', False)
TW_CONSUMER_SECRET = os.getenv('TW_CONSUMER_SECRET', False)
TW_ACCESS_TOKEN = os.getenv('TW_ACCESS_TOKEN', False)
TW_ACCESS_TOKEN_SECRET = os.getenv('TW_ACCESS_TOKEN_SECRET', False)

assert TW_CONSUMER_KEY, "Missing environ variable TW_CONSUMER_KEY"
assert TW_CONSUMER_SECRET, "Missing environ variable TW_CONSUMER_SECRET"
assert TW_ACCESS_TOKEN, "Missing environ variable TW_ACCESS_TOKEN"
assert TW_ACCESS_TOKEN_SECRET, "Missing environ variable TW_ACCESS_TOKEN_SECRET"

auth = OAuth(
    consumer_key=TW_CONSUMER_KEY,
    consumer_secret=TW_CONSUMER_SECRET,
    token=TW_ACCESS_TOKEN,
    token_secret=TW_ACCESS_TOKEN_SECRET
)
twitter_client = Twitter(auth=auth)


def approve(result):
    # this function gets called when there is a new worker response available
    # it should return a boolean value indicating if the worker response should be approved or not

    assert result is not None and result.get('results', None) is not None and len(result.get('results')) > 0

    text = result.get('results')[0].get('result')
    return len(text) > 10


def post_to_twitter(result):
    # this function gets called when there is an approved worker response available
    # it doesn't expect any response in return and can be used to pass worker response to rest of the workflow

    assert result is not None and result.get('results', None) is not None

    text = result.get('results')[0].get('result')

    # push the worker response back to twitter
    try:
        twitter_client.statuses.update(status=text)
    except Exception as e:
        pass


print "Initializing Daemo Client..."

client = Client(CREDENTIALS_FILE)
client.publish(project_id=PROJECT_ID, approve=approve, completed=post_to_twitter, stream=True)
