import logging

__version__ = "0.0.1"
# HOST = "https://daemo.stanford.edu"
HOST = "http://127.0.0.1:8000"
CONTENT_JSON = 'application/json'
CONTENT_FORM_URLENCODED= 'application/x-www-form-urlencoded'
TOKEN = "Bearer %s"
AUTHORIZATION = "Authorization"
CONTENT_TYPE = "Content-Type"


try:
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

logging.getLogger(__name__).addHandler(NullHandler())
