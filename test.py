from daemo.client import Client

CLIENT_ID = ""
ACCESS_TOKEN = ""
REFRESH_TOKEN = ""

PROJECT_ID = 39


def approve(result):
    print result
    return True


def completed(result):
    print result

if __name__ == "__main__":
    client = Client(
        client_id=CLIENT_ID, access_token=ACCESS_TOKEN, refresh_token=REFRESH_TOKEN
    )
    client.publish(project_id=PROJECT_ID, approve=approve, completed=completed, stream=True)
