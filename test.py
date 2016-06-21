from daemo.client import Client

CLIENT_ID = "4vHcIwU1iS8judu0XcRTTWtaR2Def0ArxUkwvqqK"
ACCESS_TOKEN = "wNP8MUKcz8UJ85NQDw1Sr6dSRVDD5C"
REFRESH_TOKEN = "pCizsEjIZc2a6N9iDcV8RqyJzmtM0E"

PROJECT_ID = 39

if __name__ == "__main__":
    client = Client(
        # username=USERNAME, password=PASSWORD,
        client_id=CLIENT_ID, access_token=ACCESS_TOKEN,
        refresh_token=REFRESH_TOKEN,
        project_id=PROJECT_ID
    )
    print client
