import importlib
import os
import socket
import select

from connection import PMRConnection
from PMRProcessing.mapper.mapper import Mapper
from PMRProcessing.reducer.reducer import Reducer
from filesystems import SimpleFileSystem
from messages import *


## TODO REMOVE THESE
reducer_class = Reducer
mapper_class = Mapper

from optparse import OptionParser


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

    def do_processing(self):
        if len(self.message_read_queue):
            message = self.message_read_queue.pop()

            # if message.m_type is MessageTypes.DATAFILE:
            #     self.message_write_queue.append(JobStartAckMessage())
            #     with open('client_map_out', 'w') as f:
            #         print(message.body.splitlines())
            #         mapper = mapper_class(instream=message.body.splitlines(), outstream=f)
            #         mapper.Map()
            #         self.message_write_queue.append(JobMappingDone())
            #
            #     os.system('cat client_map_out | sort -k1,1 > client_map_out_sorted')
            #
            #     with open('client_map_out_sorted', 'r') as map_f:
            #         with open('client_reduce_out', 'w') as red_f:
            #             reducer = reducer_class(instream=map_f, outstream=red_f)
            #             reducer.Reduce()
            #             self.message_write_queue.append(JobReducingDone())

            if message.m_type is MessageTypes.JOB_READY:
                if not self.has_job:
                    self.has_job = True
                    self.message_write_queue.append(JobReadyToReceiveMessage())

            elif message.m_type is MessageTypes.JOB_INSTRUCTIONS_FILE:
                self.instructions_file = JobInstructionsFileMessage.get_path_from_message(message)
                self.instructions_type = JobInstructionsFileMessage.get_type_from_message(message)
                self.message_write_queue.append(JobInstructionsFileAckMessage())
            elif message.m_type is MessageTypes.DATAFILE:
                self.data_path = message.get_body()
                self.message_write_queue.append(DataFileAckMessage())
            elif message.m_type is MessageTypes.JOB_START:
                # Start job
                pkg = None
                instructions_class = None
                print(self.instructions_file)
                try:
                    pkg = importlib.import_module(self.instructions_file)
                except ImportError:
                    print('Error: Could not load instructions module.')
                try:
                    instructions_class = getattr(pkg, self.instructions_type)
                except AttributeError:
                    print('Error: Module was loaded, but does not contain a "{}" class.'.format(self.instructions_type))

                print(self.data_path)
                with open(self.data_path, 'r') as in_file:
                    fs = SimpleFileSystem()
                    out_path = fs.get_writeable_file_path()
                    with fs.open(out_path, 'w') as out_file:
                        task = instructions_class(instream=in_file, outstream=out_file)
                        task.run()

                    # TODO: This is a hack for now - remove the sorting when possible
                    if self.instructions_type == 'Mapper':
                        sorted_path = fs.get_writeable_file_path()
                        os.system('cat {} | sort -k1,1 > {}'.format(out_path, sorted_path))
                        self.message_write_queue.append(JobDoneMessage(sorted_path))
                    else:
                        self.message_write_queue.append(JobDoneMessage(out_path))
                # self.prep_for_new_job()

    def run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(self.server_address)

        connection = PMRConnection(sock)

        while True:
            self.do_processing()
            readable, writeable, _ = select.select([sock], [sock], [])

            if readable:
                message = connection.receive()
                if message:
                    # print(message)
                    self.message_read_queue.append(message)

            if writeable:
                # Write things if we need to
                while self.message_write_queue:
                    message = self.message_write_queue.pop()
                    connection.send_message(message)
                connection.write()

