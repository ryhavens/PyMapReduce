import importlib
import socket
import select
from optparse import OptionParser
import time

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

    def __init__(self):
        options, args = self.parse_opts()
        self.server_address = (options.host, options.port)
        self.has_job = False
        self.instructions_file = None
        self.instructions_type = None
        self.data_path = None
        self.num_workers = None
        self.partition_num = None

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
                self.message_write_queue.append(JobInstructionsFileAckMessage())
            elif message.m_type is MessageTypes.DATAFILE:
                self.data_path = message.get_body()
                self.message_write_queue.append(DataFileAckMessage())
            elif message.m_type is MessageTypes.JOB_START:
                # Start job
                pkg = importlib.import_module(self.instructions_file)
                instructions_class = getattr(pkg, self.instructions_type)

                with open(self.data_path, 'r') as in_file:
                    fs = SimpleFileSystem()

                    if self.instructions_type == 'Mapper':
                        out_path = fs.get_writeable_file_path()
                    elif self.instructions_type == 'Reducer':
                        out_path = fs.get_file_with_name('partition_{}'.format(self.partition_num))

                    with fs.open(out_path, 'w') as out_file:
                        if self.instructions_type == 'Mapper':
                            task = Mapper(self.data_path, instructions_class,
                                          self.num_workers, in_stream=in_file,
                                          out_stream=out_file)
                        elif self.instructions_type == 'Reducer':
                            task = Reducer(instructions_class, self.num_workers,
                                           in_stream=in_file, out_stream=out_file)

                        task.SetBeatMethod(lambda:
                            self.message_write_queue.append(JobHeartbeatMessage(
                                str(task.progress), 
                                str(task.progress/(time.time() - task.start_time))
                            )))
                        task.SetDieMethod(lambda: [
                            self.message_write_queue.append(JobDoneMessage(out_path)),
                            self.prep_for_new_job()
                            ])

                        task.run()

    def run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(self.server_address)

        connection = PMRConnection(sock)

        while True:
            self.do_processing()
            if self.message_write_queue:
                readable, writeable, _ = select.select([sock], [sock], [])
            else:
                readable, writeable, _ = select.select([sock], [], [])

            if readable:
                message = connection.receive()
                if message:
                    # print(message)
                    self.message_read_queue.append(message)

            if writeable:
                # Write things if we need to
                while self.message_write_queue:
                    message = self.message_write_queue.pop(0)
                    connection.send_message(message)
                connection.write()

