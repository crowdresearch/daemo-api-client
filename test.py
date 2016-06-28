from daemo.client import Client

CREDENTIALS_FILE = '.credentials'
PROJECT_ID = 39


def approve(result):
    print result
    return True


def completed(result):
    print result

if __name__ == "__main__":
    client = Client(CREDENTIALS_FILE)
    client.publish(project_id=PROJECT_ID, approve=approve, completed=completed, stream=True)
