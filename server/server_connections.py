from connection import PMRConnection
from messages import MessageTypes
import time


class WorkerConnection(PMRConnection):
    def __init__(self, file_descriptor, address=None):
        self.subscribed = False
        self.worker_id = ''
        self.prev_message = None
        self.job_id = None
        self.current_job = None

        self.instructions_ackd = False
        self.data_ackd = False

        self.byte_processing_rate = -1
        self.last_heartbeat_ack = -1

        self.result_file = None
        self.data_file = None

        self.running = False

        super().__init__(file_descriptor, address)

    def __str__(self):
        return '<WorkerConnection: sock={sock} subscribed={subscribed} current_job={job} rate={rate} last_hb_ack={time} data_file={data_file}'.format(
            sock=self.file_descriptor.fileno(), # change back to just file_descriptor to see other details
            subscribed=self.subscribed,
            job=self.current_job and self.current_job.id,
            rate=self.byte_processing_rate,
            time=time.strftime('%H:%M:%S', time.localtime(self.last_heartbeat_ack)),
            data_file = self.data_file,
        )

    def subscribe(self):
        self.subscribed = True

    def prep_for_new_job(self):
        """
        Called when the job is completely done executing

        Reset this connection's properties so that it
        is ready to accept a new job
        :return:
        """
        self.prev_message = MessageTypes.SUBSCRIBE_MESSAGE
        self.job_id = None
        self.current_job = None
        self.instructions_ackd = False
        self.data_ackd = False
        self.data_file = None
        self.result_file = None

    def return_resources(self):
        """
        Called in case of a disconnection
        Should mark any job fragments that were currently
        in progress as up for grabs again
        :return:
        """
        if self.current_job:
            self.current_job.client = None
            self.current_job.pending_assignment = False


class ConnectionsList(object):
    """
    The set of connections
    """
    def __init__(self):
        self.connections = []

    def __str__(self):
        return '<ConnectionsList: [{conn_list}]>'.format(
            conn_list=',\n\t'.join([str(c) for c in self.connections])
        )

    def add(self, connection):
        # TODO: check to make sure the connection doesn't already exist
        self.connections.append(connection)

    def get_by_socket(self, sock):
        for connection in self.connections:
            if connection.file_descriptor == sock:
                return connection
        return None

    def remove(self, file_descriptor):
        for index, connection in enumerate(self.connections):
            if connection.file_descriptor == file_descriptor:
                del self.connections[index]

    def get_read_set(self):
        # TODO: Add any filtering necessary
        return map((lambda x: x.file_descriptor), self.connections)

    def get_write_set(self):
        return map(
            lambda x: x.file_descriptor,
            filter(lambda x: x.needs_write(), self.connections)
        )

    def empty(self):
        return not self.connections

    def pop(self):
        return self.connections.pop()

    def sort(self, key_func=lambda x: x, reverse_opt=True):
        self.connections = sorted(self.connections, key=key_func, reverse=reverse_opt)
