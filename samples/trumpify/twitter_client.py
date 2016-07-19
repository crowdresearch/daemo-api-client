from datetime import datetime
from email._parseaddr import mktime_tz, parsedate_tz

from twitter import *


class TwitterClient:
    client = None

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

    def is_from_last_interval(self, message, interval):
        current_time = datetime.now()
        created_at = self.to_local_time(message.get('created_at'))
        delta = current_time - created_at
        return 0 < delta.total_seconds() < (interval * 60)

    def fetch_tweets(self, twitter_name, count, interval):
        print "Fetching new tweets..."
        messages = self.client.statuses.user_timeline(
            screen_name=twitter_name,
            exclude_replies=True,
            include_rts=False,
            count=count
        )

        # get messages within last interval
        messages_last_interval = [message for message in messages if self.is_from_last_interval(message, interval)]
        return messages_last_interval

    def post(self, text):
        try:
            self.client.statuses.update(status=text)
        except Exception as e:
            print e.message
