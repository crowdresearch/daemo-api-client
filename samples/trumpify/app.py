from flask import Flask
from twitter import *
from twitter.util import printNicely

CONSUMER_KEY = 'DsrLkapnW62Jh9CrSGowhHrFz'
CONSUMER_SECRET = '2GqubzcNN11x3P3IZjDgL2wRW0tERzG1rG67ydmVR5Uh0Ctk4E'
ACCESS_TOKEN = '746086097109688320-F46uQCNnFiZOUki8EfbAZpzrIoUUbxS'
ACCESS_TOKEN_SECRET = 'aGUiYsBAYyS9J3uTU4sZl4dID9PcpnUUZOEX5YKDAEoJL'

app = Flask(__name__)

auth = OAuth(
    consumer_key=CONSUMER_KEY,
    consumer_secret=CONSUMER_SECRET,
    token=ACCESS_TOKEN,
    token_secret=ACCESS_TOKEN_SECRET
)

stream = TwitterStream(auth=auth)
iterator = stream.statuses.filter(track="trump")  # .filter(track='#India')  # ="realDonaldTrump"

messages = []
for message in iterator:
    if message.get('text', None) is not None:
        print message.get('text')


@app.route('/')
def hello_world():
    return "hello"
