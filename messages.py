import struct
from enum import Enum

HEADER_FORMAT = '!Bi'
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)



# jon : get the value by calling MessageTypes.--type--.value
# e.g. MessageTypes.SUBSCRIBE_MESSAGE.value
class MessageTypes(Enum):
    # format: 
    SUBSCRIBE_MESSAGE = 1  # Sent by client to subscribe
    # [SUBSCRIBE_MESSAGE]
    # Server responses:
        # [SUBSCRIBE_ACK_MESSAGE][ClientID], where ClientID is autogenerated by server
        # [ERROR][TOO_MANY_CLIENTS], error code where client is unable to subscribe
    SUBSCRIBE_ACK_MESSAGE = 2  # Sent by server to ack subscribe
    # [SUBSCRIBE_ACK_MESSAGE][ClientID], where ClientID is autogenerated by server
    RESUBSCRIBE_MESSAGE = 3 # Sent by client that was temporarily disconnected to reconnect
    # [RESUBSCRIBE_MESSAGE][ClientID]
    # Server responses:
        # [SUBSCRIBE_ACK_MESSAGE][ClientID]
        # [ERROR][NAME_ACTIVE], sent by server to client that is resubscribing to a name it considers actively connected
        # [ERROR][TOO_MANY_CLIENTS], sent by server if it already has enough clients
    JOB_READY = 4  # Sent by server to tell client a job is ready
    # [JOB_READY][JobID], JobID unique to job
    JOB_READY_TO_RECEIVE = 5  # Sent by client to acknowledge job ready, ready to receive job
    # [JOB_READY_TO_RECEIVE][ClientID][JobID], ACKs job ID
    DATAFILE = 6 # Sent by server to client to give it the data for the current job
    # [DATAFILE][ClientID][Filename][SeqNo]
    # Client is responsible for writing the data to a file in order
    DATAFILE_ACK = 7 # Sent by client to server to ACK  data
    # [DATAFILE_ACK][ClientID][Filename][SeqNo]
    # Server can move on to next file when current file is done sending
    JOB_START = 8 # Sent by server to client to tell client to begin job
    # [JOB_START][ClientID][JobID]
    # If client does not have data for current job it will send an error
    JOB_START_ACK = 9 # Sent by client to server to notify job has begun
    # [JOB_START_ACK][ClientID][JobID]
    JOB_HEARTBEAT = 10 # Sent by client to server to periodically update server on job progress for client
    # [JOB_HEARTBEAT][ClientID][JobID][Phase][SeqNo]
    # Phase is Mapper or Reducer
    # SeqNo is heartbeat #
    # Server logic should track rate of client
    JOB_MAPPING_DONE = 11 # Sent by client to server to tell server it has finished mapping
    # [JOB_MAPPING_DONE][ClientID][JobID]
    JOB_REDUCING_DONE = 12 # Same as above, but for reducing
    # [JOB_REDUCING_DONE][ClientID][JobID]

    SERVER_ERROR = 98
    # [SERVER_ERROR][ERROR_CODE]
    # Error can mean a variety of things, it is up to the error handler to interpret the error code
    CLIENT_ERROR = 99
    # [CLIENT_ERROR][ClientID][ERROR_CODE]
    # Additionally specified is the client ID



class Message(object):
    def __init__(self, m_type, body=None):
        self.m_type = m_type
        self.body = body

    def __str__(self):
        return '<Message: type={m_type} body={body}>'.format(m_type=self.m_type, body=self.body)

    def has_body(self):
        return self.body

    def get_header_for_send(self):
        return struct.pack(HEADER_FORMAT, self.m_type.value, len(self.body) if self.body else 0)

    def get_body_for_send(self):
        return struct.pack(str(len(self.body)) + 's', bytes(self.body, encoding='utf-8'))

    def is_type(self, m_type):
        return m_type is self.m_type


class SubscribeMessage(Message):
    def __init__(self):
        super().__init__(MessageTypes.SUBSCRIBE_MESSAGE)


class SubscribeAckMessage(Message):
    def __init__(self):
        super().__init__(MessageTypes.SUBSCRIBE_ACK_MESSAGE)


class JobReadyToReceiveMessage(Message):
    def __init__(self):
        super().__init__(MessageTypes.JOB_READY_TO_RECEIVE)


class DataFileMessage(Message):
    def __init__(self, body):
        super().__init__(MessageTypes.DATAFILE, body=body)


class JobMappingDone(Message):
    def __init__(self):
        super().__init__(MessageTypes.JOB_MAPPING_DONE)


class JobReducingDone(Message):
    def __init__(self):
        super().__init__(MessageTypes.JOB_REDUCING_DONE)