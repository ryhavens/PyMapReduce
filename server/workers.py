class Workers(object):
    """
    The set of available workers
    """
    def __init__(self):
        self.workers = []

    def add(self, worker):
        # TODO: check to make sure the worker doesn't already exist
        self.workers.append(worker)

    def get_by_id(self, worker_id):
        for worker in self.workers:
            if worker.worker_id == worker_id:
                return worker
        return None

    def get_by_socket(self, sock):
        for worker in self.workers:
            if worker.file_descriptor == sock:
                return worker
        return None

    def remove(self, file_descriptor):
        for index, worker in enumerate(self.workers):
            if worker.file_descriptor == file_descriptor:
                del self.workers[index]

    def get_read_set(self):
        # TODO: Add any filtering necessary
        return map((lambda x: x.file_descriptor), self.workers)

    def get_write_set(self):
        return map(
            lambda x: x.file_descriptor,
            filter(lambda x: x.needs_write(), self.workers)
        )
