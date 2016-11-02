import socket
import select

from connection import PMRConnection
from messages import SubscribeMessage


class Client(object):
    REMOTE_HOST = 'localhost'
    REMOTE_PORT = 8888

    message_write_queue = [
        SubscribeMessage()
    ]
    message_read_queue = []

    def run(self):
        server_address = (self.REMOTE_HOST, self.REMOTE_PORT)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(server_address)

        connection = PMRConnection(sock)

        while True:
            readable, writeable, _ = select.select([sock], [sock], [])

            if readable:
                message = connection.receive()
                if message:
                    # Do something with the new message
                    print(message)

            if writeable:
                # Write things if we need to
                while self.message_write_queue:
                    message = self.message_write_queue.pop()
                    connection.send_message(message)
                connection.write()

