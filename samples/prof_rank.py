from daemo.client import DaemoClient

PROJECT_KEY = ''
RERUN_KEY = ''

daemo = DaemoClient(rerun_key=RERUN_KEY)


def find_professors():
    """
    Post institute name and branch to Daemo server for finding top professor in that domain
    """
    daemo.publish(
        project_key=PROJECT_KEY,
        tasks=[
            {
                "stream": "Computer Science",
                "institute": "Stanford University"
            },
            {
                "stream": "Bioengineering",
                "institute": "Stanford University"
            },
        ],
        approve=approve_correct_response,
        completed=rate_workers
    )


def get_response_text(worker_response):
    """
    Filter out professor name from worker's complete submission

    :param worker_response: submission made by a worker for a task
    :return: professor name
    """
    return worker_response.get('fields').get('professor')


def approve_correct_response(worker_responses):
    """
    Verify each worker response if it meets the requirements

    :param worker_responses: submission made by a worker for a task
    :return: list of True/False
    """
    approvals = [get_response_text(response) is not None and len(get_response_text(response)) > 0 for response in
                 worker_responses]
    return approvals


def rate_workers(worker_responses):
    """
    Rate all responses by workers which got approved

    :param worker_responses: submission made by a worker for a task
    """
    for worker_response in worker_responses:
        rate(worker_response)


def rate(worker_response):
    """
    Rate an approved worker's response based on length of response

    :param worker_response: submission made by a worker for a task
    """
    if worker_response is not None:
        response = get_response_text(worker_response)

        rating = {
            "task_id": worker_response.get("task_id"),
            "worker_id": worker_response.get("worker_id"),
            "weight": 3 if len(response) > 0 else 1
        }

        daemo.rate(project_key=PROJECT_KEY, ratings=[rating])


find_professors()
