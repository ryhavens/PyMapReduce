import struct
import socket
import select
from optparse import OptionParser

from connection import PMRConnection, Message, ClientDisconnectedException
from .workers import Workers


class Server(object):
    DEFAULT_PORT = '8888'
    DEFAULT_HOST = 'localhost'

    def parse_opts(self):
        """
        Parse command line arguments
        :return: (options, args)
        """
        parser = OptionParser()
        parser.add_option('-p', '--port', dest='port',
                          help='port to bind to', type='int', default=self.DEFAULT_PORT)
        parser.add_option('-s', '--host', dest='host',
                          help='host address to bind to', type='string', default=self.DEFAULT_HOST)
        return parser.parse_args()

    def run(self):
        options, args = self.parse_opts()
        server_address = (options.host, options.port)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(server_address)

        sock.listen(10)  # Backlog of 10

        workers = Workers()

        while True:
            read_list = [sock]
            read_list += workers.get_read_set()
            write_list = workers.get_write_set()

            readable, writeable, _ = select.select(read_list, write_list, [])

            for s in readable:
                if s == sock:
                    connection, client_address = sock.accept()
                    connection.setblocking(0)

                    workers.add(PMRConnection(connection, client_address))
                else:
                    worker = workers.get_by_socket(s)
                    try:
                        message = worker.receive()
                        if message:
                            worker.send_message(Message(2, 'this is an ack'))
                    except (ClientDisconnectedException, ConnectionResetError) as e:
                        workers.remove(s)

            for s in writeable:
                worker = workers.get_by_socket(s)
                if worker and worker.needs_write():
                    try:
                        worker.write()
                    except Exception as e:
                        # TODO: What exceptions can happen here? Should we resend?
                        print(e)