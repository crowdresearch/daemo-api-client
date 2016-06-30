import json
import os
import sys
import time

from twitter import *

sys.path.append(os.path.abspath('../../'))

CREDENTIALS_FILE = 'credentials.json'
LAST_ID_FILE = '.lastid'

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
TWITTER_START_ID = '747865622030200832'


class TwitterClient:
    def __init__(self):
        from daemo.client import Client

        print "Initializing Twitter Client..."

        self.auth = OAuth(
            consumer_key=TW_CONSUMER_KEY,
            consumer_secret=TW_CONSUMER_SECRET,
            token=TW_ACCESS_TOKEN,
            token_secret=TW_ACCESS_TOKEN_SECRET
        )

        self.client = Client(CREDENTIALS_FILE)

        self.twitter = Twitter(auth=self.auth)
        self.messages = []

        while True:
            self.fetch_new_tweets()
            time.sleep(1800)  # check messages every 30 minutes

    def fetch_new_tweets(self):
        last_id = self.get_last_id()

        self.messages = self.twitter.statuses.user_timeline(
            screen_name=TWITTER_NAME,
            exclude_replies=True,
            include_rts=False,
            since_id=last_id
        )

        for message in reversed(self.messages):
            self.process_message(message)

    def process_message(self, message):
        response = "FAIL"

        if message.get('text', False):
            text = message.get('text')
            id = message.get('id')

            if len(text) > 10:
                try:
                    response = self.client.add_data(project_id=PROJECT_ID, data={"tasks": [{
                        "tweet": text, "id": id
                    }]})

                    assert response is not None, "Failed to add data to project"

                    last_id = id
                    self.update_last_id(last_id)

                    return "SUCCESS"
                except Exception as e:
                    print e.message
        return response

    def update_last_id(self, last_id):
        with open(LAST_ID_FILE, 'w') as outfile:
            data = {
                'last_id': last_id
            }
            json.dump(data, outfile)

        outfile.close()

    def get_last_id(self):
        last_id = TWITTER_START_ID
        try:
            with open(LAST_ID_FILE, 'r') as infile:
                data = json.load(infile)
                last_id = data['last_id']
        except:
            pass
        return last_id


twitter_client = TwitterClient()
