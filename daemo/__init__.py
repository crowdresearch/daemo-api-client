import logging

__version__ = "0.0.1"

HOST = "daemo.stanford.edu"
# HOST = "127.0.0.1:8000"
HTTP = "https://"
WEBSOCKET = "wss://"
WS_BOT_SUBSCRIBE_URL = "/ws/bot?subscribe-user"
OAUTH_TOKEN_URL = "/api/oauth2-ng/token/"


try:
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

logging.getLogger(__name__).addHandler(NullHandler())
