import os
import socket
import select
import collections
from optparse import OptionParser

from PMRJob.job import Job
from connection import ClientDisconnectedException
from filesystems import SimpleFileSystem
from messages import MessageTypes, JobReadyMessage, JobInstructionsFileMessage, DataFileMessage
from .server_connections import WorkerConnection, ConnectionsList
from .message_handlers import handle_message

import importlib
import sys


class SubJob(object):
    def __init__(self,
                 id,
                 instruction_path,
                 instruction_type,
                 data_path=None,

                 # List of jobs - Use if other jobs depend on the output of this job
                 pass_result_to=[],

                 # Wait for several data-producing jobs to finish
                 data_paths_list=[],
                 num_data_paths_required=0,

                 # Anything to do before running, gets passed this instance
                 # Can be a single function or a list
                 # If data_path is not set on create, do_before must set it
                 do_before=None,

                 # Passed self, output_path
                 # Can be a single function or a list of functions
                 do_after=None
                 ):
        self.id = id
        self.instruction_path = instruction_path
        self.instruction_type = instruction_type
        self.data_path = data_path

        self.pass_result_to = pass_result_to

        self.data_paths_list = data_paths_list
        self.num_data_paths_required = num_data_paths_required

        self.do_before = do_before
        self.do_after = do_after

        self.client = None
        self.pending_assignment = True

    def is_ready_to_execute(self):
        if self.pass_result_to:
            return True

        if self.data_paths_list and len(self.data_paths_list) == self.num_data_paths_required:
            return True

        return False

    def is_last(self):
        """
        Is this the last job to be done
        :return:
        """
        return not self.pass_result_to

    def pre_execute(self):
        if self.do_before:
            if type(self.do_before) is list:
                for action in self.do_before:
                    action(self)
            else:
                self.do_before()

    def post_execute(self, output_path):
        if type(self.do_after) is list:
            for action in self.do_after:
                action(self, output_path)

        if self.pass_result_to:
            if type(self.pass_result_to) is list:
                for job in self.pass_result_to:
                    job.data_paths_list.append(output_path)
            else:
                self.pass_result_to.data_paths_list.append(output_path)


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

        # self.jobs_queue = collections.deque()
        # self.jobs_queue.append('f1.txt')

        self.job_started = False
        self.sub_jobs = list()
        self.pending_jobs = list()  # Jobs not dependent on one job, but multiple

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

    def partition(self, data_path):
        """
        Create n partitions of 500 lines each
        :return:
        """
        lines_per_partition = 500
        partition = -1
        partition_paths = []
        partition_handlers = []

        fs = SimpleFileSystem()
        with fs.open(data_path, 'r') as f:
            for index, line in enumerate(f):
                if index % lines_per_partition == 0:
                    # Clean up open descriptor
                    if partition_handlers:
                        fs.close(partition_handlers[partition])

                    # Create new partition
                    partition += 1
                    partition_paths.append(fs.get_writeable_file_path())
                    partition_handlers.append(fs.open(partition_paths[partition], 'w'))

                partition_handlers[partition].write(line)

        return partition_paths

    def stitch_data_files(self, job):
        assert(job.data_paths_list, 'Job must have data_paths_list if stitch_data_files is called')
        fs = SimpleFileSystem()
        out_file_path = fs.get_writeable_file_path()
        with fs.open(out_file_path, 'w') as f:
            for path in job.data_paths_list:
                with fs.open(path, 'r') as g:
                    f.write(g.read())
        job.data_path = out_file_path

    def sort_data_file(self, job):
        fs = SimpleFileSystem()
        in_path = fs.get_writeable_file_path()
        os.system('mv {} {}'.format(job.data_path, in_path))
        os.system('cat {} | sort -k1,1 > {}'.format(in_path, job.data_path))

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
            partitions = self.partition(data_path)

            self.pending_jobs.append(SubJob(
                id=self.GetNextJobID(),
                instruction_path=reducer_name,
                instruction_type='Reducer',
                data_paths_list=[],
                num_data_paths_required=len(partitions),
                # Stitch files together and sort output before start reducer
                do_before=[self.stitch_data_files, self.sort_data_file]
            ))

            for partition_path in partitions:
                self.sub_jobs.append(SubJob(
                    id=self.GetNextJobID(),
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

    def InitializeJob(self):
        """
        Read in paths and verify them
        Call self.run(..) when finished
        :return:
        """
        mapper_name = None
        reducer_name = None

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
                datafile.close()
            except FileNotFoundError:
                print('Could not load datafile. Please retry.')
                continue
            break

        self.run(mapper_name, reducer_name, datafile_name)

    def run(self, mapper_name, reducer_name, datafile):

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
                            to_write = handle_message(message, conn, self.sub_jobs)
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