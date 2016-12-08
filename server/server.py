import time
import curses
import socket
import select
import os
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
        self.sock.setblocking(1)
        self.sock.settimeout(1.0)
        self.sock.bind(self.server_address)
        self.sock.listen(10)  # Backlog of 10

        self.running = True
        self.connections_list = ConnectionsList()

        # job related stats
        self.job_started = False
        self.mapping = False
        self.reducing = False
        self.mapper_name = None
        self.reducer_name = None

        # number of connected workers
        self.num_workers = 0

        self.job_submitter_connection = None  # The conn that submitted the current job
        self.sub_jobs = list()  # Jobs to be executed at next opportunity
        self.pending_jobs = list()  # Jobs that are blocked by something in sub_jobs

        self.show_info_pane = options.info is None  # Would be False for no
        if self.show_info_pane:
            self.stdscr = curses.initscr()  # For the info pane
            curses.noecho()  # Don't show typed characters

        # slow mode for debugging
        self.slow = options.slow

        # max time allowed in between heartbeats of running workers
        # before the worker is assumed dead
        self.timeout_allowance = 5
        # buffer to avoid unnecessary slowdown in performance when determining
        # whether to boot an underperforming worker
        self.time_buffer = 5 


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
            self.reset_performance_stats()
            setup_reducing_tasks(self.reducer_name, self.num_workers, self.sub_jobs, self.get_next_job_id)

        # Find clients that can do the job for us
        # Aka clients who are subscribed and don't have a job id
        conns = [c for c in self.connections_list.connections if c.subscribed and c.current_job is None]

        for index, job in enumerate(self.sub_jobs):
            if job.client is None and conns:
                conn = conns.pop()
                self.assign_job(conn, job)
                

    def assign_job(self, conn, job):
        # hacky workaround because mapper reads file from data_path and reducer
        # reads files (plural) from a partition directory
        if (job.data_path is None):
            conn.chunk_size = sum([os.path.getsize(file) for file in SimpleFileSystem().get_partition_files(job.partition_num)])
        else:
            conn.chunk_size = os.path.getsize(job.data_path)

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
        self.num_workers = self.get_num_subscribed_workers()
        
        # monitor utilization of worker resources during job
        self.begin_monitor_job_efficiency()

        SimpleFileSystem().clean_directories()
        setup_mapping_tasks(data_file_path, mapper_name, self.num_workers, self.sub_jobs, self.get_next_job_id)

    def get_num_subscribed_workers(self):
        return len([c for c in self.connections_list.connections if c.subscribed])

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
        # self.num_workers = 0
        self.end_monitor_job_efficiency()
        self.reset_performance_stats()

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

            if (self.job_started):
                pass
                print (self.connections_list)
            readable, writeable, _ = select.select(read_list, write_list, [], 1.0)
            for s in readable:
                if s == self.sock:
                    try:
                        connection, client_address = self.sock.accept()
                        connection.setblocking(0)

                        self.connections_list.add(WorkerConnection(connection, client_address))
                    except:
                        pass
                else:
                    conn = self.connections_list.get_by_socket(s)
                    try:
                        message = conn.receive()
                        if message:
                            to_write = handle_message(message, conn,
                                                      num_workers=self.get_num_subscribed_workers,
                                                      initialize_job=self.initialize_job,
                                                      current_job_connection=self.job_submitter_connection,
                                                      job_finished=self.job_finished,
                                                      mark_job_as_finished=self.mark_job_as_finished)

                            while to_write:
                                w_message = to_write.pop()
                                conn.send_message(w_message)
                            self.update_job_distribution()

                    except (ClientDisconnectedException, ConnectionResetError) as e:
                        self.handle_conn_error(conn, "Disconnected")

            for s in writeable:
                conn = self.connections_list.get_by_socket(s)
                if conn and conn.needs_write():
                    try:
                        conn.write()
                    except Exception as e:
                        # TODO: What exceptions can happen here? Should we resend?
                        pass

            self.operational_check()

    def handle_conn_error(self, conn, error=None):
        print(error)
        conn.return_resources()
        self.connections_list.remove(conn.file_descriptor)

    # operational_check
    # Performs any operations the server deems necessary to improve performance
    #   and/or handle subtle errors from clients. 
    def operational_check(self):
        if (self.mapping or self.reducing):
            self.performance_check()
            self.check_timed_out_heartbeats()

    # performance_check
    # If there are nodes in the network that have a processing rate 1000+ times
    # slower than that of a free node, kick out that worker as it is only 
    # hindering the performance of jobs
    def performance_check(self):
        top_rate_and_free = None
        low_rate = None

        for conn in self.connections_list.connections:
            if (conn.subscribed):
                if (conn.running == False):
                    if ((top_rate_and_free is None) or \
                        (conn.byte_processing_rate > top_rate_and_free.byte_processing_rate)):
                        top_rate_and_free = conn
                elif ((low_rate is None) or \
                    (low_rate.byte_processing_rate != 0 and low_rate.byte_processing_rate > conn.byte_processing_rate)):
                    low_rate = conn

        # this worker is ready to take on a job if needed
        if (top_rate_and_free is not None and low_rate is not None):
            if (low_rate.byte_processing_rate == 0):
                return
            multiplier = 1 if not self.reducing else 2 # progress updates twice for reducer
            # compute the estimated completion time of the job for the low rate worker
            low_rate_estimated_completion = self.estimate_completion_time(
                multiplier, low_rate.chunk_size, low_rate.progress, low_rate.byte_processing_rate)
            top_rate_estimated_completion = self.estimate_completion_time(
                multiplier, low_rate.chunk_size, 0, top_rate_and_free.byte_processing_rate)

            # print(time.strftime('%H:%M:%S', time.localtime(low_rate_estimated_completion)))
            # print(time.strftime('%H:%M:%S', time.localtime(top_rate_estimated_completion)))
            # print(top_rate_estimated_completion - low_rate_estimated_completion)

            if (low_rate_estimated_completion - top_rate_estimated_completion > self.time_buffer):
                # they took our jobs!!!!!!!!
                # dey terk er jerbsss!!!
                self.assign_job(top_rate_and_free, low_rate.current_job)
                self.handle_conn_error(low_rate, error="Client is too slow")

    # generic function to estimate completion time
    def estimate_completion_time(self, multiplier, chunk_size, progress, byte_processing_rate):
        if (byte_processing_rate == 0):
            return 0
        return (multiplier * chunk_size - progress) / byte_processing_rate



    # compares last acked heartbeat to current time, disconnects client if difference is
    # greater than self.heartbeat_allowance
    def check_timed_out_heartbeats(self):
        current_time = time.time()
        timed_out_conns = filter(lambda c: c.running and (current_time - c.last_heartbeat_ack) > self.timeout_allowance, self.connections_list.connections)
        for conn in timed_out_conns:
            self.handle_conn_error(conn, "Heartbeat timeout")

    # serialized job ID
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

    # client performance should only apply per phase
    def reset_performance_stats(self):
        for c in self.connections_list.connections:
            c.byte_processing_rate = -1
            c.progress = 0



