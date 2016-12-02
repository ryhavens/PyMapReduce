import os
import socket
import select

from connection import PMRConnection
from PMRProcessing.mapper.mapper import Mapper
from PMRProcessing.reducer.reducer import Reducer
from messages import *


## TODO REMOVE THESE
reducer_class = Reducer
mapper_class = Mapper

from optparse import OptionParser


class Client(object):
    REMOTE_HOST = 'localhost'
    REMOTE_PORT = 8888

    message_write_queue = [
        SubscribeMessage()
    ]
    message_read_queue = []

    def __init__(self):
        options, args = self.parse_opts()
        self.server_address = (options.host, options.port)



    def parse_opts(self):
        parser = OptionParser()
        parser.add_option('-p', '--port', dest='port',
                          help='address of server port', type='int', default=self.REMOTE_PORT)
        parser.add_option('-s', '--host', dest='host',
                          help='server host address', type='string', default=self.REMOTE_HOST)
        return parser.parse_args()

    def do_processing(self):
        if len(self.message_read_queue):
            message = self.message_read_queue.pop()
            if message.m_type is MessageTypes.SUBSCRIBE_ACK_MESSAGE:
                self.message_write_queue.append(JobReadyToReceiveMessage())

            # elif message.m_type is MessageTypes.DATAFILE:


            elif message.m_type is MessageTypes.DATAFILE:
                self.message_write_queue.append(JobStartAckMessage())
                with open('client_map_out', 'w') as f:
                    print(message.body.splitlines())
                    mapper = mapper_class(instream=message.body.splitlines(), outstream=f)
                    mapper.Map()
                    self.message_write_queue.append(JobMappingDone())

                os.system('cat client_map_out | sort -k1,1 > client_map_out_sorted')

                with open('client_map_out_sorted', 'r') as map_f:
                    with open('client_reduce_out', 'w') as red_f:
                        reducer = reducer_class(instream=map_f, outstream=red_f)
                        reducer.Reduce()
                        self.message_write_queue.append(JobReducingDone())

    def run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(self.server_address)

        connection = PMRConnection(sock)

        while True:
            self.do_processing()
            readable, writeable, _ = select.select([sock], [sock], [])

            if readable:
                message = connection.receive()
                if message:
                    # print(message)
                    self.message_read_queue.append(message)

            if writeable:
                # Write things if we need to
                while self.message_write_queue:
                    message = self.message_write_queue.pop()
                    connection.send_message(message)
                connection.write()

