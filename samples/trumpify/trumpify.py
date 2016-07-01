import os
import sys

from twitter import *

sys.path.append(os.path.abspath('../../'))

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

TWITTER_NAME = 'HillaryClinton'

auth = OAuth(
    consumer_key=TW_CONSUMER_KEY,
    consumer_secret=TW_CONSUMER_SECRET,
    token=TW_ACCESS_TOKEN,
    token_secret=TW_ACCESS_TOKEN_SECRET
)
twitter_client = Twitter(auth=auth)


def approve(result):
    assert result is not None and result.get('results', None) is not None and len(result.get('results')) > 0
    text = result.get('results')[0].get('result')
    return len(text) > 10


def completed(result):
    assert result is not None and result.get('results', None) is not None
    text = result.get('results')[0].get('result')

    try:
        twitter_client.statuses.update(status=text)
    except Exception as e:
        pass


class DaemoClient:
    def __init__(self):
        from daemo.client import Client

        print "Initializing Daemo Client..."

        self.client = Client(CREDENTIALS_FILE)
        self.client.publish(project_id=PROJECT_ID, approve=approve, completed=completed, stream=True)


daemo_client = DaemoClient()
