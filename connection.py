import struct

from messages import HEADER_SIZE, Message


class ClientDisconnectedException(Exception):
    pass


class PMRConnection(object):
    """
    Represents a connected client
    """
    def __init__(self, file_descriptor, address=None):
        self.file_descriptor = file_descriptor
        self.address = address

        self.data_bytes_remaining = 0
        self.header_buffer = []
        self.receive_buffer = []

        self.write_buffer = []

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

    def needs_write(self):
        """
        Does this connection need something written?
        :return: bool
        """
        return bool(self.write_buffer)

    def send_message(self, message):
        self._write_to_buffer(message.get_header_for_send())
        if message.has_body():
            self._write_to_buffer(message.get_body_for_send())

    def _write_to_buffer(self, buffer):
        """
        Write to connection's buffer to be sent at next opportunity
        :param buffer:
        :return:
        """
        self.write_buffer += buffer

    def write(self):
        self.file_descriptor.sendall(bytes(self.write_buffer))
        self.write_buffer = []
