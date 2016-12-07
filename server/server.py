import time
import curses
import socket
import select
from optparse import OptionParser

from PMRJob.job import setup_mapping_tasks, setup_reducing_tasks
from connection import ClientDisconnectedException
from filesystems import SimpleFileSystem
from messages import JobReadyMessage
from .server_connections import WorkerConnection, ConnectionsList
from .message_handlers import handle_message

from PMRProcessing.heartbeat.heartbeat import *


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
        self.connections_list = ConnectionsList()

        self.job_started = False
        self.mapping = False
        self.reducing = False
        self.mapper_name = None
        self.reducer_name = None
        self.num_partitions = 1

        self.job_submitter_connection = None  # The conn that submitted the current job
        self.sub_jobs = list()  # Jobs to be executed at next opportunity
        self.pending_jobs = list()  # Jobs that are blocked by something in sub_jobs

        self.show_info_pane = options.info is None  # Would be False for no
        if self.show_info_pane:
            self.stdscr = curses.initscr()  # For the info pane
            curses.noecho()  # Don't show typed characters

        self.slow = options.slow

    def stop_gui(self):
        """
        Cleans up the curses settings to return terminal
        to a usable mode
        :return:
        """
        if self.show_info_pane:
            curses.endwin()

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
        parser.add_option('-n', '--no-info', dest='info',
                          help='don\'t show the informational pane (useful for printing)', action='store_false')
        parser.add_option('--slow', dest='slow',
                          help='slow down event loop for testing', action='store_true')
        return parser.parse_args()

    def start(self):
        """
        Start the server by reading in information
        and then kicking off event loop
        :return:
        """
        self.run()

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

    def update_job_distribution(self):
        """
        Move pending jobs that are ready to the sub_jobs queue for execution
        Assign jobs in sub_jobs to clients
        :return:
        """
        self.update_client_performance_statistics()
        remaining_map_jobs = [j for j in self.sub_jobs if j.instruction_type == 'Mapper' and not j.done]

        if self.mapping and not remaining_map_jobs:
            self.mapping = False
            self.reducing = True
            setup_reducing_tasks(self.reducer_name, self.num_partitions, self.sub_jobs, self.get_next_job_id)

        # Find clients that can do the job for us
        # Aka clients who are subscribed and don't have a job id
        conns = [c for c in self.connections_list.connections if c.subscribed and c.current_job is None]

        for index, job in enumerate(self.sub_jobs):
            if job.client is None and conns:
                conn = conns.pop()
                job.pre_execute()
                job.client = conn
                job.pending_assignment = True
                conn.current_job = job
                conn.send_message(JobReadyMessage(str(job.id)))

    # sort clients by their estimated processing rate
    def update_client_performance_statistics(self):
        if (self.job_started):
            self.connections_list.sort(key_func=lambda conn: conn.byte_processing_rate, reverse_opt=True)

    def initialize_job(self, submitter, mapper_name, reducer_name, data_file_path):
        """

        :return:
        """
        self.job_submitter_connection = submitter
        self.job_started = True
        self.mapping = True
        self.reducing = False
        self.mapper_name = mapper_name
        self.reducer_name = reducer_name
        # equal partitions distributed based off number of workers
        self.num_partitions = len([c for c in self.connections_list.connections if c.subscribed])
        
        # monitor utilization of worker resources during job
        self.begin_monitor_job_efficiency()

        SimpleFileSystem().clean_directories()
        setup_mapping_tasks(data_file_path, mapper_name, self.num_partitions, self.sub_jobs, self.get_next_job_id)

    def job_finished(self):
        """
        Return whether the job is finished
        Aka is it in reducing mode w/ all jobs completed
        :return:s
        """
        if self.reducing:
            remaining_jobs = [j for j in self.sub_jobs if not j.done]
            return not remaining_jobs
        return False

    def mark_job_as_finished(self):
        """
        Prep server for new job
        Called when commander acks the job completion
        :return:
        """
        self.job_started = False
        self.mapping = False
        self.reducing = False
        self.mapper_name = None
        self.reducer_name = None
        self.job_submitter_connection = None
        self.num_partitions = 1
        self.end_monitor_job_efficiency()

    def update_interface(self):
        """
        Update the info pane
        :return:
        """
        self.stdscr.erase()

        line_number = 0
        self.stdscr.addstr(line_number, 0, 'PyMapReduce Master Status')
        line_number += 1
        self.stdscr.addstr(line_number, 0,
                           'Server running on HOST {}, PORT {}'.format(self.server_address[0], self.server_address[1]))
        line_number += 2
        self.stdscr.addstr(line_number, 0,
                           '{} workers'.format(len([c for c in self.connections_list.connections if c.subscribed])))

        for i, conn in enumerate(self.connections_list.connections):
            if conn.subscribed:
                line_number += 1
                self.stdscr.addstr(line_number, 0, conn.worker_id + ' ' + ('working' if conn.current_job else 'idle'))

        line_number += 2
        if self.job_started:
            total_tasks = len(self.sub_jobs) + len(self.pending_jobs)
            tasks_complete = len([j for j in self.sub_jobs + self.pending_jobs if j.client])
            percent = round(tasks_complete / total_tasks * 100)
            self.stdscr.addstr(line_number, 0, 'Running job... {percent}% complete'.format(percent=percent))
        else:
            self.stdscr.addstr(line_number, 0, 'Waiting for job...')

        self.stdscr.refresh()

    def run(self):
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
            if self.slow:
                time.sleep(.5)
            if self.show_info_pane:
                self.update_interface()
            read_list = [self.sock]
            read_list += self.connections_list.get_read_set()
            write_list = self.connections_list.get_write_set()

            readable, writeable, _ = select.select(read_list, write_list, [])
            if (self.job_started):
                print (self.connections_list)
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
                            to_write = handle_message(message, conn,
                                                      num_partitions=self.num_partitions,
                                                      initialize_job=self.initialize_job,
                                                      current_job_connection=self.job_submitter_connection,
                                                      job_finished=self.job_finished,
                                                      mark_job_as_finished=self.mark_job_as_finished)

                            while to_write:
                                w_message = to_write.pop()
                                conn.send_message(w_message)
                            self.update_job_distribution()
                    except (ClientDisconnectedException, ConnectionResetError) as e:
                        conn.return_resources()
                        self.connections_list.remove(s)

            for s in writeable:
                conn = self.connections_list.get_by_socket(s)
                if conn and conn.needs_write():
                    try:
                        conn.write()
                    except Exception as e:
                        # TODO: What exceptions can happen here? Should we resend?
                        # print(e)
                        pass

    def get_next_job_id(self):
        self.job_id += 1
        return self.job_id

    # returns proportion of workers running compared to workers available
    def get_efficiency(self):
        n_working = 0
        n_subscribed = 0
        for conn in self.connections_list.connections:
            if (conn.subscribed == True):
                n_subscribed += 1
                if (conn.current_job is not None):
                    n_working += 1

        if (n_subscribed == 0):
            return 0.0
        return n_working/float(n_subscribed)

    # called at the end of the job to determine overall efficiency
    def log_efficiency(self):
        print('Start time: %s' % (time.strftime('%H:%M:%S', time.localtime(self.monitor_stats['start_time']))))
        print('End time: %s' % (time.strftime('%H:%M:%S', time.localtime())))
        print('Average worker utilization: %s' % (str(100*self.monitor_stats['avg_efficiency'])))

    # begin monitoring efficiency at the start of a job
    # uses a BeatingProcess to allow the efficiency to be updated concurrently
    def begin_monitor_job_efficiency(self):
        self.monitor_interval = 0.1
        self.monitor = BeatingProcess(heartbeat_interval=self.monitor_interval)
        self.monitor.SetHeartbeatID('Job')
        self.monitor_stats = dict()
        self.monitor_stats['start_time'] = time.time()
        self.monitor_stats['avg_efficiency'] = 0
        self.monitor.SetBeatMethod(self.update_avg_efficiency)
        self.monitor.SetDieMethod(self.log_efficiency)
        self.monitor.BeginHeartbeat()

    def end_monitor_job_efficiency(self):
        self.monitor.EndHeartbeat()
            
    # function called from the server's BeatingProcess thread
    def update_avg_efficiency(self):
        # updates average efficiency as a weighted average
        self.monitor_stats['avg_efficiency'] = \
            (self.get_efficiency() * self.monitor_interval + \
            self.monitor_stats['avg_efficiency'] * (time.time() - self.monitor_stats['start_time'])) / \
            (self.monitor_interval + time.time() - self.monitor_stats['start_time'])



