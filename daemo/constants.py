__version__ = "1.0.2"

PRODUCTION = "daemo.herokuapp.com"
SANDBOX = "daemo-test.herokuapp.com"
WS_BOT_SUBSCRIBE_URL = "/ws/bot?subscribe-user"
OAUTH_TOKEN_URL = "/api/oauth2-ng/token/"

GRANT_TYPE = "grant_type"
REFRESH_TOKEN = "refresh_token"
ACCESS_TOKEN = "access_token"
CLIENT_ID = "client_id"
CREDENTIALS_NOT_PROVIDED = "Authentication credentials were not provided."
CONTENT_JSON = "application/json"
TOKEN = "Bearer %s"
AUTHORIZATION = "Authorization"
CONTENT_TYPE = "Content-Type"
STATUS_ACCEPTED = 3
STATUS_REJECTED = 4


class API:
    task = "/api/task/%d/"
    rerun_config = "/api/task/?filter_by=rerun_key&rerun_key=%s"
    publish_project = "/api/project/%s/publish/"
    add_tasks = "/api/project/%s/add-data/"
    task_results = "/api/task-worker/list-submissions/?task_id=%d"
    task_worker_results = "/api/task-worker/%d/"
    update_task_status = "/api/task-worker/bulk-update-status/"
    task_status = "/api/task/%s/is-done/"
    mock_results = "/api/task-worker-result/mock-results/"
    peer_review = "/api/task/peer-review/"
    true_skill_score = "/api/worker-requester-rating/trueskill/?match_group_id={}"
