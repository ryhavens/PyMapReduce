import importlib
import socket
import select
from optparse import OptionParser
import time
import random

from PMRProcessing.mapper.mapper import Mapper
from PMRProcessing.reducer.reducer import Reducer
from connection import PMRConnection
from filesystems import SimpleFileSystem
from messages import *


class Client(object):
    REMOTE_HOST = 'localhost'
    REMOTE_PORT = 8888

    message_write_queue = [
        SubscribeMessage()
    ]
    message_read_queue = []

    def __init__(self, slow_mode=False):
        options, args = self.parse_opts()
        self.server_address = (options.host, options.port)
        self.has_job = False
        self.instructions_file = None
        self.instructions_type = None
        self.data_path = None
        self.slow_mode = slow_mode
        self.num_workers = None
        self.partition_num = None
        self.connection = None

    def parse_opts(self):
        parser = OptionParser()
        parser.add_option('-p', '--port', dest='port',
                          help='address of server port', type='int', default=self.REMOTE_PORT)
        parser.add_option('-s', '--host', dest='host',
                          help='server host address', type='string', default=self.REMOTE_HOST)
        return parser.parse_args()

    def prep_for_new_job(self):
        self.has_job = False
        self.instructions_file = None
        self.instructions_type = None
        self.data_path = None
        self.num_workers = None
        self.partition_num = None

    def do_processing(self):
        if len(self.message_read_queue):
            message = self.message_read_queue.pop(0)

            if message.m_type is MessageTypes.JOB_READY:
                if not self.has_job:
                    self.has_job = True
                    self.message_write_queue.append(JobReadyToReceiveMessage())

            elif message.m_type is MessageTypes.JOB_INSTRUCTIONS_FILE:
                self.instructions_file = JobInstructionsFileMessage.get_path_from_message(message)
                self.instructions_type = JobInstructionsFileMessage.get_type_from_message(message)
                self.num_workers = JobInstructionsFileMessage.get_num_workers_from_message(message)
                self.partition_num = JobInstructionsFileMessage.get_partition_num_from_message(message)
            elif message.m_type is MessageTypes.DATAFILE:
                self.data_path = message.get_body()
            elif message.m_type is MessageTypes.JOB_START:
                # Start job
                fs = SimpleFileSystem()

                pkg = importlib.import_module(self.instructions_file)
                instructions_class = getattr(pkg, self.instructions_type)

                in_file = None
                if self.data_path:
                    in_file = fs.open(self.data_path, 'r')

                if self.instructions_type == 'Mapper':
                    # pass instruction class to mapper
                    task = Mapper(self.data_path, instructions_class,
                                  self.num_workers, in_stream=in_file, slow_mode=self.slow_mode)
                elif self.instructions_type == 'Reducer':
                    # pass instruction class to reducer
                    task = Reducer(instructions_class, self.num_workers, self.partition_num,
                                   slow_mode=self.slow_mode)

                # beat method will send status reports to the server
                # on a separate thread to avoid blocking during the
                # actual map/reduce
                task.SetBeatMethod(lambda: [
                    self.connection.send_message(JobHeartbeatMessage(
                        str(task.progress), 
                        str(task.progress/(time.time() - task.start_time))
                    )),
                    self.connection.write(),
                    ])
                # completion actions upon finishing map/reduce steps
                # this doesn't actually have to be on a different thread 
                task.SetDieMethod(lambda: [
                    self.message_write_queue.append(JobDoneMessage()),
                    self.prep_for_new_job()
                    ])

                self.connection.send_message(JobStartAckMessage())
                self.connection.write()

                task.run()

                if in_file:
                    fs.close(in_file)

    def send_ack_for(self, message):
        if message.m_type is MessageTypes.JOB_INSTRUCTIONS_FILE:
            self.message_write_queue.append(JobInstructionsFileAckMessage())
        elif message.m_type is MessageTypes.DATAFILE:
            # if random.choice([0,1]) == 1:
            self.message_write_queue.append(DataFileAckMessage())
            # else:
            #     return False
        elif message.m_type is MessageTypes.JOB_START:
            self.message_write_queue.append(JobStartAckMessage())
        return True

    def run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(self.server_address)

        # setting this as an object variable so that it can be accessed from the
        # threaded calls
        self.connection = PMRConnection(sock)

        while True:
            self.do_processing()

            if self.message_write_queue:
                readable, writeable, _ = select.select([sock], [sock], [])
            else:
                readable, _, _ = select.select([sock], [], [])

            if readable:
                message = self.connection.receive()
                if message:
                    # print(message)
                    # TODO: Remove this random dropping once done testing
                    if self.send_ack_for(message):
                        self.message_read_queue.append(message)

            # Wait for write again, to send acks asap
            if not writeable and self.message_write_queue:
                _, writeable, _ = select.select([], [sock], [])

            if writeable:
                # Write things if we need to
                while self.message_write_queue:
                    message = self.message_write_queue.pop(0)
                    print('sending type {}'.format(message.m_type))
                    self.connection.send_message(message)
                self.connection.write()

