import sys
import os

sys.path.append(os.path.abspath('../../'))

CREDENTIALS_FILE = '.credentials'

PROJECT_ID = 47


def approve(result):
    assert result is not None and result.get('results', None) is not None
    text = result.get('results')[0].get('result')
    return len(text) > 10


def completed(result):
    assert result is not None and result.get('results', None) is not None
    text = result.get('results')[0].get('result')


from daemo.client import Client

print "Initializing Daemo Client..."

client = Client(CREDENTIALS_FILE)
client.publish(project_id=PROJECT_ID, approve=approve, completed=completed, stream=True)

