import time
from Queue import Queue
from datetime import datetime
from email._parseaddr import mktime_tz, parsedate_tz

import requests
from twitter import *

from daemo.errors import Error

TOP_STORIES_URL = 'https://api.nytimes.com/svc/topstories/v2/home.json?api-key=%s'
NYT_API_KEY = ''

TW_CONSUMER_KEY = ''
TW_CONSUMER_SECRET = ''
TW_ACCESS_TOKEN = ''
TW_ACCESS_TOKEN_SECRET = ''


class NYTUtils:
    client = None
    news_items = Queue()

    def __init__(self):
        self.client = requests.session()

    def fetch_top_news(self):
        response = self.client.get(TOP_STORIES_URL % NYT_API_KEY)
        response.raise_for_status()

        data = response.json()
        messages = data.get('results', [])

        for message in reversed(messages):
            self.news_items.put(message)


class TwitterUtils:
    client = None
    tweets = Queue()

    def __init__(self):
        assert TW_CONSUMER_KEY != '', Error.required('TW_CONSUMER_KEY')
        assert TW_CONSUMER_SECRET != '', Error.required('TW_CONSUMER_SECRET')
        assert TW_ACCESS_TOKEN != '', Error.required('TW_ACCESS_TOKEN')
        assert TW_ACCESS_TOKEN_SECRET != '', Error.required('TW_ACCESS_TOKEN_SECRET')

        auth = OAuth(
            consumer_key=TW_CONSUMER_KEY,
            consumer_secret=TW_CONSUMER_SECRET,
            token=TW_ACCESS_TOKEN,
            token_secret=TW_ACCESS_TOKEN_SECRET
        )

        self.client = Twitter(auth=auth)

    def to_local_time(self, tweet_timestamp):
        """Convert rfc 5322 -like time string into a local time
           string in rfc 3339 -like format.
        """
        timestamp = mktime_tz(parsedate_tz(tweet_timestamp))
        return datetime.fromtimestamp(timestamp)

    def is_from_last_interval(self, timestamp, interval):
        delta_seconds = self.seconds_left(timestamp)
        return 0 < delta_seconds < interval

    def seconds_left(self, timestamp):
        current_time = datetime.now()
        created_at = self.to_local_time(timestamp)
        delta = current_time - created_at
        return delta.total_seconds()

    def fetch_tweets(self, twitter_name, count, interval):
        print "Fetching new tweets..."
        messages = self.client.statuses.user_timeline(
            screen_name=twitter_name,
            exclude_replies=True,
            include_rts=False,
            count=count
        )

        # get messages within last interval
        messages_last_interval = [message for message in messages if
                                  self.is_from_last_interval(message.get('created_at'), interval)]
        return messages_last_interval

    def post(self, worker_response):
        tweet = None
        try:
            text = self.get_tweet_text(worker_response)

            if text is not None:
                tweet = self.client.statuses.update(status=text)
                self.store(worker_response, tweet)
        except Exception as e:
            print e.message
        return tweet

    def store(self, worker_response, tweet):
        self.tweets.put({
            "id": tweet.get('id'),
            "tweet": tweet.get('text'),
            "created_at": tweet.get('created_at'),
            "task_id": worker_response.get('task'),
            "worker_id": worker_response.get('worker'),
        })

    def get_tweet_response(self):
        return self.tweets.get()

    def has_tweet_responses(self):
        return self.tweets.qsize() > 0

    def get_retweet_count(self, tweet_id):
        tweet = self.client.statuses.show(id=tweet_id)
        retweet_count = tweet.get('retweet_count', 0)
        return retweet_count

    def get_tweet_text(self, worker_response):
        if worker_response is not None:
            return worker_response.get('fields').get('tweet')
        return None

    def monitor_next_tweet(self, interval):
        tweet = self.get_tweet_response()

        if tweet is not None:
            seconds_elapsed = self.seconds_left(timestamp=tweet.get('created_at'))
            seconds_left = interval - seconds_elapsed

            if seconds_left > 0:
                time.sleep(seconds_left)

        return tweet
