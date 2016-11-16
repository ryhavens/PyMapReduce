import struct

HEADER_FORMAT = '!Bi'
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

SUBSCRIBE_MESSAGE = 1  # Sent by client to subscribe
SUBSCRIBE_ACK_MESSAGE = 2  # Sent by server to ack subscribe
JOB_READY_FOR_REQUEST = 3  # Sent by server to tell client a job is ready
JOB_READY_TO_RECEIVE = 4  # Sent by client to acknowledge job ready, ready to receive job
ERROR = 5 # Sent by client to indicate that it is unable to 


class Message(object):
    def __init__(self, m_type, body=None):
        self.m_type = m_type
        self.body = body

    def __str__(self):
        return '<Message: type={m_type} body={body}>'.format(m_type=self.m_type, body=self.body)

    def has_body(self):
        return self.body

    def get_header_for_send(self):
        return struct.pack(HEADER_FORMAT, self.m_type, len(self.body) if self.body else 0)

    def get_body_for_send(self):
        return struct.pack(str(len(self.body)) + 's', bytes(self.body, encoding='utf-8'))

    def is_type(self, m_type):
        return m_type == self.m_type


class SubscribeMessage(Message):
    def __init__(self):
        super().__init__(SUBSCRIBE_MESSAGE)


class SubscribeAckMessage(Message):
    def __init__(self):
        super().__init__(SUBSCRIBE_ACK_MESSAGE)