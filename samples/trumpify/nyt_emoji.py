import os
import sys

import requests
import time

from daemo.client import DaemoClient
CREDENTIALS_FILE = 'credentials.json'

TOP_STORIES_URL = 'https://api.nytimes.com/svc/topstories/v2/home.json?api-key=%s'

NYT_API_KEY = os.getenv('NYT_API_KEY', False)
assert NYT_API_KEY, "Missing environ variable NYT_API_KEY"

PROJECT_ID = os.getenv('PROJECT_ID', False)
assert PROJECT_ID, "Missing environ variable PROJECT_ID"
PROJECT_ID = int(PROJECT_ID)


class NYTClient:
    def __init__(self):
        print "Initializing NYT Client..."

        self.session = requests.session()

        self.client = Client(CREDENTIALS_FILE)
        self.messages = []

        while True:
            self.fetch_top_news()
            time.sleep(21600)  # check messages every 6 hrs

    def fetch_top_news(self):
        response = self.session.get(TOP_STORIES_URL % NYT_API_KEY)
        response.raise_for_status()

        messages = response.json()
        self.messages = messages.get('results', [])

        for message in reversed(self.messages):
            self.process_message(message)

    def process_message(self, message):
        response = "FAIL"

        if message.get('title', False) and message.get('url', False):
            title = message.get('title')
            url = message.get('url')

            if len(title) > 10:
                try:
                    response = self.client.add_data(project_id=PROJECT_ID, data={"tasks": [{
                        "title": title, "url": url
                    }]})

                    assert response is not None, "Failed to add data to project"
                    response.raise_for_status()

                    return "SUCCESS"
                except Exception as e:
                    print e.message
        return response


nyt_client = NYTClient()
