import os
import socket
import select

from connection import PMRConnection
from PMRProcessing.mapper.mapper import Mapper
from PMRProcessing.reducer.reducer import Reducer
from messages import *


class Client(object):
    REMOTE_HOST = 'localhost'
    REMOTE_PORT = 8888

    message_write_queue = [
        SubscribeMessage()
    ]
    message_read_queue = []

    def do_processing(self):
        if len(self.message_read_queue):
            message = self.message_read_queue.pop()
            if message.m_type is MessageTypes.SUBSCRIBE_ACK_MESSAGE:
                self.message_write_queue.append(JobReadyToReceiveMessage())

            if message.m_type is MessageTypes.DATAFILE:
                with open('client_map_out', 'w') as f:
                    mapper = Mapper(instream=message.body.splitlines(), outstream=f)
                    mapper.Map()
                    self.message_write_queue.append(JobMappingDone())

                os.system('cat client_map_out | sort -k1,1 > client_map_out_sorted')

                with open('client_map_out_sorted', 'r') as map_f:
                    with open('client_reduce_out', 'w') as red_f:
                        reducer = Reducer(instream=map_f, outstream=red_f)
                        reducer.Reduce()
                        self.message_write_queue.append(JobReducingDone())

    def run(self):
        server_address = (self.REMOTE_HOST, self.REMOTE_PORT)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(server_address)

        connection = PMRConnection(sock)

        while True:
            self.do_processing()
            readable, writeable, _ = select.select([sock], [sock], [])

            if readable:
                message = connection.receive()
                if message:
                    print(message)
                    self.message_read_queue.append(message)

            if writeable:
                # Write things if we need to
                while self.message_write_queue:
                    message = self.message_write_queue.pop()
                    connection.send_message(message)
                connection.write()

