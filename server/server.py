import time
import curses
import socket
import select
from optparse import OptionParser

from PMRJob.job import prep_job_for_execution
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
        self.connections_list = ConnectionsList()

        self.job_started = False
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
                conn = conns.pop()
                job.pre_execute()
                job.client = conn
                job.pending_assignment = True
                conn.current_job = job
                conn.send_message(JobReadyMessage(str(job.id)))

    def initialize_job(self, submitter, mapper_name, reducer_name, data_file_path):
        """

        :return:
        """
        self.job_submitter_connection = submitter
        self.job_started = True
        prep_job_for_execution(data_path=data_file_path,
                               reducer_name=reducer_name,
                               mapper_name=mapper_name,
                               pending_jobs=self.pending_jobs,
                               sub_jobs=self.sub_jobs,
                               get_next_job_id=self.get_next_job_id)

    def mark_job_as_finished(self):
        """
        Prep server for new job
        Called when commander acks the job completion
        :return:
        """
        self.job_started = False
        self.job_submitter_connection = None

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
                                                      initialize_job=self.initialize_job,
                                                      current_job_connection=self.job_submitter_connection,
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
                        print(e)

    def get_next_job_id(self):
        self.job_id += 1
        return self.job_id
