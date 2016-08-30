import logging

__version__ = "1.0.2"

HOST = "daemo.stanford.edu"
WS_BOT_SUBSCRIBE_URL = "/ws/bot?subscribe-user"
OAUTH_TOKEN_URL = "/api/oauth2-ng/token/"


try:
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

logging.getLogger(__name__).addHandler(NullHandler())
