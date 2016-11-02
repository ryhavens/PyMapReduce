import sys
import socket
import select
from optparse import OptionParser

from worker import Worker, Workers, ClientDisconnectedException


DEFAULT_PORT = '8888'
DEFAULT_HOST = 'localhost'


def parse_opts():
    """
    Parse command line arguments
    :return: (options, args)
    """
    parser = OptionParser()
    parser.add_option('-p', '--port', dest='port',
                      help='port to bind to', type='int', default=DEFAULT_PORT)
    parser.add_option('-s', '--host', dest='host',
                      help='host address to bind to', type='string', default=DEFAULT_HOST)
    return parser.parse_args()


def main():
    options, args = parse_opts()
    server_address = (options.host, options.port)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(server_address)

    sock.listen(10)  # Backlog of 10

    workers = Workers()

    while True:
        read_list = [sock]
        read_list += workers.get_read_set()

        readable, _, _ = select.select(read_list, [], [])

        for s in readable:
            if s == sock:
                connection, client_address = sock.accept()
                connection.setblocking(0)

                workers.add(Worker(connection, client_address))
            else:
                worker = workers.get_by_socket(s)
                try:
                    message = worker.receive()
                    if message:
                        print('We got a message!')
                        print('Type is: {}'.format(message.m_type))
                        print('Message is: {}'.format(str(message.body, encoding='utf-8')))
                except ClientDisconnectedException:
                    print('Client disconnected')
                    workers.remove(s)


if __name__ == '__main__':
    main()