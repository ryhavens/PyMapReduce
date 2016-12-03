import sys
import importlib
import socket
import select
from optparse import OptionParser

from PMRJob.job import SubJob, fixed_size_partition, stitch_data_files, sort_data_file
from connection import ClientDisconnectedException
from messages import JobReadyMessage
from .server_connections import WorkerConnection, ConnectionsList
from .message_handlers import handle_message


class Server(object):
    _PORT = '8888'
    _HOST = 'localhost'
    job_id = 0  # Track job_ids so as not to reuse them

    def __init__(self):
        options, args = self.parse_opts()
        self.server_address = (options.host, options.port)

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind(self.server_address)
        self.sock.listen(10)  # Backlog of 10

        self.running = True
        print('Server running on HOST {}, PORT {}'.format(self.server_address[0], self.server_address[1]))
        self.connections_list = ConnectionsList()

        self.job_started = False
        self.sub_jobs = list()  # Jobs to be executed at next opportunity
        self.pending_jobs = list()  # Jobs that are blocked by something in sub_jobs

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

    def start(self):
        """
        Start the server by reading in information
        and then kicking off event loop
        :return:
        """
        self.initialize_job()

    def stop(self):
        """
        Stop the server loop and clean up resources
        :return:
        """
        self.running = False
        while not self.connections_list.empty():
            conn = self.connections_list.pop()
            conn.file_descriptor.shutdown(socket.SHUT_RDWR)
            conn.file_descriptor.close()
        self.sock.close()

    def do_processing(self, mapper_name, reducer_name, data_path):
        """
        Do any necessary processing that isn't linked to one
        particular client
        Runs on every server loop
        :return:
        """

        # Assuming one overall job
        # Current tactic is to spread out the mapping
        # and have one reducer job depending on them finishing
        if not self.job_started:
            self.job_started = True
            partitions = fixed_size_partition(data_path)

            self.pending_jobs.append(SubJob(
                id=self.get_next_job_id(),
                instruction_path=reducer_name,
                instruction_type='Reducer',
                data_paths_list=[],
                num_data_paths_required=len(partitions),
                # Stitch files together and sort output before start reducer
                do_before=[stitch_data_files, sort_data_file]
            ))

            for partition_path in partitions:
                self.sub_jobs.append(SubJob(
                    id=self.get_next_job_id(),
                    instruction_path=mapper_name,
                    instruction_type='Mapper',
                    data_path=partition_path,
                    pass_result_to=self.pending_jobs[0]
                ))

        # Find any pending jobs that are ready to be done
        pending_jobs_to_pop = []
        for job in self.pending_jobs:
            if job.is_ready_to_execute():
                self.sub_jobs.append(job)
                pending_jobs_to_pop.append(job)

        for job in pending_jobs_to_pop:
            for index, p_job in enumerate(self.pending_jobs):
                if p_job == job:
                    self.pending_jobs.pop(index)
                    break

        # Find clients that can do the job for us
        # Aka clients who are subscribed and don't have a job id
        conns = [c for c in self.connections_list.connections if c.subscribed and c.current_job is None]

        for index, job in enumerate(self.sub_jobs):
            if job.client is None and conns:
                print('Assigning job {} to conn'.format(job.id))
                conn = conns.pop()
                job.pre_execute()
                job.client = conn
                job.pending_assignment = True
                conn.current_job = job
                conn.send_message(JobReadyMessage(str(job.id)))

    def initialize_job(self):
        """
        Read in paths and verify them
        Call self.run(..) when finished
        :return:
        """
        mapper_name = None
        reducer_name = None

        print('Please specify the path to where your Mapper is located now:')
        while True:
            mapper_name = sys.stdin.readline()
            mapper_name = mapper_name.strip()  # truncate '\n'
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
        while True:
            reducer_name = sys.stdin.readline()
            reducer_name = reducer_name.strip()  # truncate '\n'
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
        while True:
            datafile_name = sys.stdin.readline()
            datafile_name = datafile_name.strip()  # truncate '\n'
            try:
                datafile = open(datafile_name, 'r')
                datafile.close()
            except FileNotFoundError:
                print('Could not load datafile. Please retry.')
                continue
            break

        self.run(mapper_name, reducer_name, datafile_name)

    def run(self, mapper_name, reducer_name, datafile):
        """
        The server main loop

        For each loop, call do_processing to do any extraneous processing
        Then block on select(..) and read/write from clients, handling any
        messages that they send

        :param mapper_name:
        :param reducer_name:
        :param datafile:
        :return:
        """
        while self.running:
            self.do_processing(mapper_name, reducer_name, datafile)
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

    def get_next_job_id(self):
        self.job_id += 1
        return self.job_id