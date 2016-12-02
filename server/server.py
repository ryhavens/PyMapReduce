import socket
import select
import collections
from optparse import OptionParser

from PMRJob.job import Job
from connection import ClientDisconnectedException
from messages import MessageTypes
from .server_connections import WorkerConnection, ConnectionsList
from .message_handlers import handle_message

import importlib
import sys


class Server(object):
    _PORT = '8888'
    _HOST = 'localhost'
    JobID = 0


    def __init__(self):
        options, args = self.parse_opts()
        self.server_address = (options.host, options.port)

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind(self.server_address)
        self.sock.listen(10)  # Backlog of 10

        self.running = True
        print('Server running on HOST {}, PORT {}'.format(self.server_address[0], self.server_address[1]))
        self.connections_list = ConnectionsList()

        self.jobs_queue = collections.deque()
        self.jobs_queue.append('f1.txt')

        """
        How should system state work?

        A map of client ids to lists of jobs

        For now, client_id will be file descriptor
        """
        self.conn_status_map = {}

    def parse_opts(self):
        """
        Parse command line arguments
        :return: (options, args)
        """
        parser = OptionParser()
        parser.add_option('-p', '--port', dest='port',
                          help='port to bind to', type='int', default=self._PORT)
        parser.add_option('-s', '--host', dest='host',
                          help='host address to bind to', type='string', default=self._HOST)
        return parser.parse_args()

    def Start(self):
        self.InitializeJob()

    def stop(self):
        self.running = False
        while not self.connections_list.empty():
            conn = self.connections_list.pop()
            conn.file_descriptor.shutdown(socket.SHUT_RDWR)
            conn.file_descriptor.close()
        self.sock.close()

    def do_processing(self, mapper_class, reducer_class, datafile):
        """
        Do any necessary processing that isn't linked to one
        particular client
        Runs on every server loop
        :return:
        """
        # conns = self.connections_list.connections
        # for conn in conns:
        #     if conn.file_descriptor not in self.conn_status_map:
        #         self.conn_status_map[conn.file_descriptor] = []

        # Select conns who want a job
        conns = [c for c in self.connections_list.connections if c.prev_message is MessageTypes.JOB_READY_TO_RECEIVE]

        while conns and len(self.jobs_queue):
            job_data = self.jobs_queue.pop()
            job = Job(JobID=self.GetNextJobID(), mapper=mapper_class, reducer=reducer_class, instream=datafile, client_list=conns)
            job.PartitionJob(conns)

    def InitializeJob(self):
        mapper_class = None
        reducer_class = None
        datafile = None

        print('Please specify the path to where your Mapper is located now:')
        while (1):
            mapper_name = sys.stdin.readline()
            mapper_name = mapper_name.strip() # truncate '\n'
            try:
                mapper_pkg = importlib.import_module(mapper_name)
            except ImportError:
                print('Could not load module. Please retry.')
                continue
            try:
                mapper_class = mapper_pkg.Mapper
            except AttributeError:
                print('Module was loaded, but does not contain a "Mapper" class. Please retry.')
                continue
            break
        print('Please specify the path to where your Reducer is located now:')
        while (1):
            reducer_name = sys.stdin.readline()
            reducer_name = reducer_name.strip() # truncate '\n'
            try:
                reducer_pkg = importlib.import_module(reducer_name)
            except ImportError:
                print('Could not load module. Please retry.')
                continue
            try:
                reducer_class = reducer_pkg.Reducer
            except AttributeError:
                print('Module was loaded, but does not contain a "Reducer" class. Please retry.')
                continue
            break
        print('Please specify the path to your Data file now:')
        while (1):
            datafile_name = sys.stdin.readline()
            datafile_name = datafile_name.strip() # truncate '\n'
            try:
                datafile = open(datafile_name, 'r')
            except FileNotFoundError:
                print('Could not load datafile. Please retry.')
                continue
            break

        self.run(mapper_class, reducer_class, datafile)

    def run(self, mapper_class, reducer_class, datafile):

        while self.running:
            self.do_processing(mapper_class, reducer_class, datafile)
            print(self.connections_list)
            read_list = [self.sock]
            read_list += self.connections_list.get_read_set()
            write_list = self.connections_list.get_write_set()

            readable, writeable, _ = select.select(read_list, write_list, [])

            for s in readable:
                if s == self.sock:
                    connection, client_address = self.sock.accept()
                    connection.setblocking(0)

                    self.connections_list.add(WorkerConnection(connection, client_address))
                else:
                    conn = self.connections_list.get_by_socket(s)
                    try:
                        message = conn.receive()
                        if message:
                            to_write = handle_message(message, conn)
                            while to_write:
                                w_message = to_write.pop()
                                conn.send_message(w_message)
                    except (ClientDisconnectedException, ConnectionResetError) as e:
                        self.connections_list.remove(s)

            for s in writeable:
                conn = self.connections_list.get_by_socket(s)
                if conn and conn.needs_write():
                    try:
                        conn.write()
                    except Exception as e:
                        # TODO: What exceptions can happen here? Should we resend?
                        print(e)

    def GetNextJobID(self):
        self.JobID += 1
        return self.JobID