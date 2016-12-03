import socket
import select
from optparse import OptionParser

from connection import PMRConnection
from messages import MessageTypes, SubmitJobMessage, SubmittedJobFinishedMessage, SubmittedJobFinishedAckMessage

"""
A command file for submitting jobs to the server
"""


def parse_opts():
    """
    Parse command line arguments
    :return: (options, args)
    """
    parser = OptionParser()
    parser.add_option('-p', '--port', dest='port',
                      help='port to bind to', type='int', default=8888)
    parser.add_option('-s', '--host', dest='host',
                      help='host address to bind to', type='string', default='localhost')
    parser.add_option('-m', '--mapper', dest='mapper',
                      help='the mapper package path', type='string',
                      default='PMRProcessing.mapper.word_count_mapper')
    parser.add_option('-r', '--reducer', dest='reducer',
                      help='the reducer package path', type='string',
                      default='PMRProcessing.reducer.word_count_reducer')
    parser.add_option('-d', '--datafile', dest='datafile',
                      help='the datafile path', type='string', default='f1.txt')
    return parser.parse_args()


def main():
    options, args = parse_opts()
    server_address = (options.host, options.port)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(server_address)

    connection = PMRConnection(sock)

    # Send job command
    connection.send_message(SubmitJobMessage(
        mapper_name=options.mapper,
        reducer_name=options.reducer,
        data_file_path=options.datafile
    ))
    connection.write()

    # Wait for ack
    readable, _, _ = select.select([sock], [], [])
    if readable:
        message = connection.receive()
        if message and message.is_type(MessageTypes.SUBMIT_JOB_ACK):
            print('Server acknowledged job submission')
        else:
            print('Received unexpected message from server')

    # Wait for job completion
    complete = False
    while not complete:
        readable, _, _ = select.select([sock], [], [])
        if readable:
            message = connection.receive()
            if message:
                complete = True

    if message.is_type(MessageTypes.SUBMITTED_JOB_FINISHED):
        print('Job finished. Output located at: {}'.format(
            SubmittedJobFinishedMessage.get_data_file_path(message)
        ))
    else:
        print('Received unexpected message from server')

    # Ack completion
    connection.send_message(SubmittedJobFinishedAckMessage())
    connection.write()
    connection.file_descriptor.close()

if __name__ == '__main__':
    main()