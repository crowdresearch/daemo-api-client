import logging

log = logging.getLogger("daemo.client")


class Store:
    batches = None
    tasks = None
    cache = None

    def __init__(self):
        self.batches = []
        self.tasks = {}
        self.cache = {}

    def batch_count(self):
        return len(self.batches)

    def map_task(self, task, batch_index, count):
        task_id = task["id"]
        task_group_id = task["group_id"]

        self.batches[batch_index]["status"][task_group_id] = False

        if task_group_id not in self.batches[batch_index]["submissions"]:
            self.batches[batch_index]["submissions"][task_group_id] = 0

        if task_group_id not in self.batches[batch_index]["expected"]:
            self.batches[batch_index]["expected"][task_group_id] = count

        if task_group_id in self.tasks:
            self.tasks[task_group_id]["batches"].append(batch_index)
        else:
            self.tasks[task_group_id] = {
                "batches": [batch_index],
                "task_id": task_id,
                "task_group_id": task_group_id,
            }

    def is_task_complete(self, batch_index, task_id, task_group_id):
        # task_status = self._fetch_task_status(task_id)
        #
        # is_done = task_status["is_done"]
        # expected = int(task_status["expected"])

        # expected = int(self.batches[batch_index]["count"])

        expected = self.batches[batch_index]["expected"][task_group_id]
        actual = self.batches[batch_index]["submissions"][task_group_id]

        # compare result counts too
        log.debug(msg="is task complete? Expected submissions = %d, Total submissions = %d" % (
            expected, actual))

        return expected <= actual

    def mark_task_completed(self, batch_index, task_id, task_group_id):
        if task_group_id in self.batches[batch_index]["status"]:
            self.batches[batch_index]["status"][task_group_id] = True
            log.debug(msg="task %d is complete" % task_group_id)

    def mark_task_incomplete(self, batch_index, task_id, task_group_id):
        if task_group_id in self.batches[batch_index]["status"]:
            self.batches[batch_index]["submissions"][task_group_id] = self.batches[batch_index]["submissions"][
                task_group_id] - 1
            self.batches[batch_index]["status"][task_group_id] = False
            log.debug(msg="task %d is NOT complete" % task_group_id)

    def is_batch_complete(self, batch_index):
        is_complete = all(self.batches[batch_index]["status"].values())
        log.debug(msg="batch %d is %s complete" % (batch_index, ''if is_complete else 'NOT'))
        return is_complete

    def mark_batch_completed(self, batch_index):
        log.debug(msg="batch %d is complete" % batch_index)

        self.batches[batch_index]["is_complete"] = True

    def mark_batch_incomplete(self, batch_index):
        log.debug(msg="batch %d is NOT complete" % batch_index)

        self.batches[batch_index]["is_complete"] = False

    def mark_match_completed(self, match_index):
        log.debug(msg="match %d is complete" % match_index)

        self.cache[match_index]["is_complete"] = True

    def all_batches_complete(self):
        return all([batch["is_complete"] for batch in self.batches])

    def all_reviews_complete(self):
        return all([self.cache[match_group_id]["is_complete"] for match_group_id in self.cache.keys()])

    def aggregate(self, batch_index, task_id, task_group_id, taskworker_id, task_data):
        task_data["taskworker_id"] = taskworker_id
        self.batches[batch_index]["aggregated_data"].append({
            "task_id": task_id,
            "task_group_id": task_group_id,
            "data": task_data
        })

    def get_aggregated(self, batch_index):
        return [x["data"] for x in self.batches[batch_index]["aggregated_data"]]
