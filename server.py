import sys
import socket
from optparse import OptionParser


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

    sock.listen(1)  # Backlog of 1

    while True:
        connection, client_address = sock.accept()

        try:
            data = connection.recv(1)
            if data:
                connection.sendall(data)
        finally:
            connection.close()


if __name__ == '__main__':
    main()