import socket
import select
from optparse import OptionParser

from connection import ClientDisconnectedException
from .server_connections import WorkerConnection, ConnectionsList
from .message_handlers import handle_message


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

        connections_list = ConnectionsList()

        while True:
            print(connections_list)
            read_list = [sock]
            read_list += connections_list.get_read_set()
            write_list = connections_list.get_write_set()

            readable, writeable, _ = select.select(read_list, write_list, [])

            for s in readable:
                if s == sock:
                    connection, client_address = sock.accept()
                    connection.setblocking(0)

                    connections_list.add(WorkerConnection(connection, client_address))
                else:
                    conn = connections_list.get_by_socket(s)
                    try:
                        message = conn.receive()
                        if message:
                            to_write = handle_message(message, conn)
                            while to_write:
                                w_message = to_write.pop()
                                conn.send_message(w_message)
                    except (ClientDisconnectedException, ConnectionResetError) as e:
                        connections_list.remove(s)

            for s in writeable:
                conn = connections_list.get_by_socket(s)
                if conn and conn.needs_write():
                    try:
                        conn.write()
                    except Exception as e:
                        # TODO: What exceptions can happen here? Should we resend?
                        print(e)