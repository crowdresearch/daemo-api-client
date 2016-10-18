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

    def map_task(self, task, batch_index):
        task_id = task["id"]

        self.batches[batch_index]["status"][task_id] = False

        if task_id not in self.batches[batch_index]["submissions"]:
            self.batches[batch_index]["submissions"][task_id] = 0

        if task_id in self.tasks:
            self.tasks[task_id]["batches"].append(batch_index)
        else:
            self.tasks[task_id] = {
                "batches": [batch_index],
                "task_id": task_id,
            }

    def is_task_complete(self, batch_index, task_id):
        # task_status = self._fetch_task_status(task_id)
        #
        # is_done = task_status["is_done"]
        # expected = int(task_status["expected"])

        expected = int(self.batches[batch_index]["count"])

        # compare result counts too
        log.debug(msg="is task complete? Expected submissions = %d, Total submissions = %d" % (
            expected, self.batches[batch_index]["submissions"][task_id]))

        # return is_done and expected <= self.batches[batch_index]["submissions"][task_id]
        return expected <= self.batches[batch_index]["submissions"][task_id]

    def mark_task_completed(self, batch_index, task_id):
        if task_id in self.batches[batch_index]["status"]:
            self.batches[batch_index]["status"][task_id] = True
            log.debug(msg="task %d is complete" % task_id)

    def is_batch_complete(self, batch_index):
        return all(self.batches[batch_index]["status"].values())

    def mark_batch_completed(self, batch_index):
        log.debug(msg="batch %d is complete" % batch_index)

        self.batches[batch_index]["is_complete"] = True

    def mark_match_completed(self, match_index):
        log.debug(msg="match %d is complete" % match_index)

        self.cache[match_index]["is_complete"] = True

    def all_batches_complete(self):
        return all([batch["is_complete"] for batch in self.batches])

    def all_reviews_complete(self):
        return all([self.cache[match_group_id]["is_complete"] for match_group_id in self.cache.keys()])

    def aggregate(self, batch_index, task_id, task_data):
        self.batches[batch_index]["aggregated_data"].append({
            "task_id": task_id,
            "data": task_data
        })

    def get_aggregated(self, batch_index):
        return [x["data"] for x in self.batches[batch_index]["aggregated_data"]]
