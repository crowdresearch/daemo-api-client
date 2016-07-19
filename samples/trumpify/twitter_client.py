from Queue import Queue
from datetime import datetime
from email._parseaddr import mktime_tz, parsedate_tz

from twitter import *


class TwitterClient:
    client = None
    tweets = Queue()

    def __init__(self, consumer_key, consumer_secret, token, token_secret):
        auth = OAuth(
            consumer_key=consumer_key,
            consumer_secret=consumer_secret,
            token=token,
            token_secret=token_secret
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

    def post(self, text):
        tweet = None
        try:
            tweet = self.client.statuses.update(status=text)
        except Exception as e:
            print e.message
        return tweet

    def store(self, result, tweet):
        self.tweets.put({
            "id": tweet.get('id'),
            "tweet": tweet.get('text'),
            "created_at": tweet.get('created_at'),
            "task_id": result.get('task'),
            "worker_id": result.get('worker'),
        })

    def get_tweet_response(self):
        if self.has_tweet_responses():
            return self.tweets.get()
        return None

    def has_tweet_responses(self):
        return self.tweets.qsize() > 0

    def get_retweet_count(self, tweet_id):
        tweet = self.client.statuses.show(id=tweet_id)
        retweet_count = tweet.get('retweet_count', 0)
        return retweet_count
