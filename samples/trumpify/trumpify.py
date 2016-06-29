import sys
import os
import threading

from twitter import *

sys.path.append(os.path.abspath('../../'))

CREDENTIALS_FILE = '.credentials'

PROJECT_ID = 53

TW_CONSUMER_KEY = 'DsrLkapnW62Jh9CrSGowhHrFz'
TW_CONSUMER_SECRET = '2GqubzcNN11x3P3IZjDgL2wRW0tERzG1rG67ydmVR5Uh0Ctk4E'
TW_ACCESS_TOKEN = '746086097109688320-F46uQCNnFiZOUki8EfbAZpzrIoUUbxS'
TW_ACCESS_TOKEN_SECRET = 'aGUiYsBAYyS9J3uTU4sZl4dID9PcpnUUZOEX5YKDAEoJL'

TWITTER_ID = '1339835893'
TWITTER_NAME = 'HillaryClinton'

auth = OAuth(
    consumer_key=TW_CONSUMER_KEY,
    consumer_secret=TW_CONSUMER_SECRET,
    token=TW_ACCESS_TOKEN,
    token_secret=TW_ACCESS_TOKEN_SECRET
)
twitter_client = Twitter(auth=auth)


def approve(result):
    assert result is not None and result.get('results', None) is not None
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
