import json
import logging

from daemo.rest import RestClient
from daemo.router import Route
from daemo.utils import raise_if_error

log = logging.getLogger("daemo.client")


class ApiClient:
    def __init__(self, credentials_path, host, http_proto):
        self.route = Route()
        self.client = RestClient(credentials_path, host, http_proto)

    def get_auth_token(self):
        return self.client.auth.get_auth_token()

    def fetch_config(self, rerun_key):
        response = self.client.get(self.route.rerun_config % rerun_key, data=json.dumps({}))
        raise_if_error("publish project", response)

        return response.json()

    def publish_project(self, project_id):
        response = self.client.post(self.route.publish_project % project_id, data=json.dumps({}))
        raise_if_error("publish project", response)

        return response.json()

    def add_data(self, project_key, tasks, rerun_key):
        response = self.client.post(self.route.add_tasks % project_key, data=json.dumps({
            "tasks": tasks,
            "rerun_key": rerun_key
        }))

        raise_if_error("publish project", response)

        return response.json()

    def get_task_results_by_taskworker_id(self, taskworker_id):
        try:
            response = self.client.get(self.route.task_worker_results % taskworker_id, data={})
            raise_if_error("process result", response)
            results = response.json()
            return results
        except Exception as e:
            return None

    def update_approval_status(self, task):
        log.debug(msg="updating status for task %d" % task["id"])

        STATUS_ACCEPTED = 3
        STATUS_REJECTED = 4

        data = {
            "status": STATUS_ACCEPTED if task["accept"] else STATUS_REJECTED,
            "workers": [task["id"]]
        }

        response = self.client.post(self.route.update_task_status, data=json.dumps(data))
        raise_if_error("task approval", response)

        return response.json()

    def fetch_task(self, task_id):
        response = self.client.get(self.route.task % task_id, data=json.dumps({}))
        raise_if_error("task", response)

        task_data = response.json()
        return task_data

    def fetch_task_status(self, task_id):
        response = self.client.get(self.route.task_status % task_id, data={})
        raise_if_error("task status", response)

        task_data = response.json()
        return task_data

    def submit_results(self, task_id, results):
        data = {
            "task_id": task_id,
            "results": results
        }

        response = self.client.post(self.route.mock_results, data=json.dumps(data))
        raise_if_error("result submission", response)
        return response.json()

    def launch_peer_review(self, task_workers, inter_task_review, rerun_key):
        data = {
            "task_workers": task_workers,
            "inter_task_review": inter_task_review,
            "rerun_key": rerun_key
        }

        response = self.client.post(self.route.peer_review, data=json.dumps(data))
        raise_if_error("peer review", response)
        return response.json()

    def get_trueskill_scores(self, match_group_id):
        response = self.client.get(self.route.true_skill_score.format(match_group_id))
        raise_if_error("rating", response)

        return response.json()

    def boomerang_feedback(self, data):
        response = self.client.post(self.route.boomerang_rating, data=json.dumps(data))
        raise_if_error("rating", response)

        return response.json()
