import logging
import threading

from daemo.exceptions import ClientException, ServerException

log = logging.getLogger("daemo.client")


def callback_thread(name, target, on_complete=None, args=[], kwargs={}):
    def target_callback(*args, **kwargs):
        # log.debug(msg="~~~~ %s started ~~~~" % name)
        target(*args, **kwargs)
        # log.debug(msg="~~~~ %s completed ~~~~" % name)

        if on_complete is not None:
            on_complete()

    thread = threading.Thread(name=name, target=target_callback, args=args, kwargs=kwargs)
    return thread


def check_dependency(condition, message):
    try:
        if not condition:
            raise ClientException(message)
    except Exception, e:
        log_exit(e)


def raise_if_error(context, response):
    try:
        if 400 <= response.status_code < 600:
            data = None
            try:
                data = response.json()
            except:
                raise ServerException(context, response.status_code, response.text)

            if data is not None:
                if "message" in data:
                    raise ServerException(context, response.status_code, data.get("message"))
                else:
                    raise ServerException(context, response.status_code, response.text)

    except Exception, e:
        log_exit(e)


def log_exit(e):
    debug = logging.getLogger().isEnabledFor(logging.DEBUG)
    log.error(e, exc_info=debug)
    exit()


def get_template_item_id(template_item_name, template_items):
    """
    Find template item by name from list of template items in a task

    :param template_item_name:
    :param template_items:
    :return:
    """
    fields = filter(lambda x: x["name"] == template_item_name, template_items)

    if fields is not None and len(fields) > 0:
        return fields[0]["id"]
    return -1


def transform_task(data):
    fields = []

    if "items" in data["template"]:
        for item in data["template"]["items"]:
            if item["role"] == "input":
                options = []

                if "choices" in item["aux_attributes"]:
                    for option in item["aux_attributes"]["choices"]:
                        options.append({
                            "position": option["position"],
                            "value": option["value"],
                        })

                fields.append({
                    "id": item["id"],
                    "name": item["name"],
                    "type": item["type"],
                    "position": item["position"],
                    "question": item["aux_attributes"]["question"]["value"],
                    "options": options
                })

    task = {
        "id": data["id"],
        "project": {
            "id": data["project_data"]["id"],
            "key": data["project_data"]["hash_id"],
            "name": data["project_data"]["name"],
            "num_workers": data["project_data"]["repetition"],
        },
        "template": {
            "id": data["template"]["id"],
            "fields": fields
        }
    }

    return task


def transform_task_results(data):
    fields = {}
    for result in data.get("results"):
        fields[result["key"]] = result["result"]

    data["fields"] = fields
    del data["results"]

    data["task_id"] = data["task"]
    data["worker_id"] = data["worker"]

    return data
