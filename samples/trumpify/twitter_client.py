import json
import os
import sys
import time

from twitter import *

sys.path.append(os.path.abspath('../../'))

CREDENTIALS_FILE = '.credentials'
LAST_ID_FILE = '.lastid'

TW_CONSUMER_KEY = 'DsrLkapnW62Jh9CrSGowhHrFz'
TW_CONSUMER_SECRET = '2GqubzcNN11x3P3IZjDgL2wRW0tERzG1rG67ydmVR5Uh0Ctk4E'
TW_ACCESS_TOKEN = '746086097109688320-F46uQCNnFiZOUki8EfbAZpzrIoUUbxS'
TW_ACCESS_TOKEN_SECRET = 'aGUiYsBAYyS9J3uTU4sZl4dID9PcpnUUZOEX5YKDAEoJL'

TWITTER_ID = '1339835893'
TWITTER_NAME = 'HillaryClinton'
TWITTER_START_ID = '747865622030200832'

PROJECT_ID = 53


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
        if message.get('text', False):
            text = message.get('text')
            id = message.get('id')

            if len(text) > 10:
                response = None
                try:
                    response = self.client.add_data(project_id=PROJECT_ID, data={"tasks": [{
                        "tweet": text, "id": id
                    }]})

                    last_id = id
                    self.update_last_id(last_id)
                except Exception as e:
                    print e.message
                    print response.text

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
