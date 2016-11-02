import struct

HEADER_SIZE = struct.calcsize('!Bi')


class ClientDisconnectedException(Exception):
    pass


class Message(object):
    def __init__(self, m_type, body):
        self.m_type = m_type
        self.body = body


class Worker(object):
    """
    Represents a connected client
    """
    def __init__(self, file_descriptor, address):
        self.file_descriptor = file_descriptor
        self.address = address

        self.data_bytes_remaining = 0
        self.header_buffer = []
        self.receive_buffer = []

    def clear_buffers(self):
        self.data_bytes_remaining = 0
        self.header_buffer = []
        self.receive_buffer = []

    def receive(self):
        """
        Receive a single message
        Returns the Message object if finished and False if not
        :param sock:
        :return: Message if finished, False if not
        """
        if len(self.header_buffer) < HEADER_SIZE:
            data = self.file_descriptor.recv(HEADER_SIZE - len(self.header_buffer))

            if not data:
                raise ClientDisconnectedException()

            self.header_buffer += data

            if len(self.header_buffer) == HEADER_SIZE:
                _, self.data_bytes_remaining = struct.unpack('>Bi', bytes(self.header_buffer))
        else:
            data = self.file_descriptor.recv(self.data_bytes_remaining)

            if not data:
                raise ClientDisconnectedException()

            self.data_bytes_remaining -= len(data)
            self.receive_buffer += data

        if len(self.header_buffer) == HEADER_SIZE and self.data_bytes_remaining == 0:
            m_type, _ = struct.unpack('>Bi', bytes(self.header_buffer))
            message = Message(m_type, bytes(self.receive_buffer))
            self.clear_buffers()
            return message
        else:
            return False


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
