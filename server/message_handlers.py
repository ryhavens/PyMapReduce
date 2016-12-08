import time
import string
import random
import importlib
from datetime import datetime, timedelta

from PMRJob.job import get_job_result_file_path
from filesystems import SimpleFileSystem
from messages import *


def should_send_job_start(conn):
    return conn.data_file_ackd and conn.instructions_ackd


def is_valid_package_path(package_path, cls):
    """
    Check if the provided package path is valid
    Example path: PMRProcessing.mapper.word_count_mapper
    :param package_path: The package path to verify
    :param cls: The class to check for in the package
    :return: boolean
    """
    try:
        pkg = importlib.import_module(package_path)
    except ImportError:
        return False
    try:
        _ = getattr(pkg, cls)
        return True
    except AttributeError:
        return False


def is_valid_file_path(path):
    """
    Check if the provided file path is valid
    :param path: the file path
    :return: boolean
    """
    sf = SimpleFileSystem()
    try:
        sf.close(sf.open(path, 'r'))
        return True
    except FileNotFoundError:
        return False


def handle_ack_timeout(expected_ack_triplet, connection, current_job_connection):
    print('HANDING ACK TIMEOUT')
    # TODO: Replace current_job_connection with connection once hooked into server loop
    expected_ack_triplet[2] += 1
    ack_cls, expected_time_start, num_timeouts = expected_ack_triplet

    if num_timeouts >= 3:
        # TODO: This worker is dead - handle that
        print('Worker dead!')
        connection.current_job.return_resources()
        # connection.prep_for_new_job()

        # TODO: Need to close and remove this connection
        # s = connection.file_descriptor
        # s.close()

        return

    if ack_cls is JobInstructionsFileAckMessage:
        connection.send_message(
            JobInstructionsFileMessage(connection.current_job.instruction_path, connection.current_job.instruction_type,
                                       connection.current_job.num_workers, connection.current_job.partition_num)
        )
    elif ack_cls is DataFileAckMessage:
        connection.send_message(DataFileMessage(connection.current_job.data_path))
    elif ack_cls is JobStartAckMessage:
        connection.send_message(JobStartMessage())
    elif ack_cls is SubmittedJobFinishedAckMessage:
        current_job_connection.send_message(SubmittedJobFinishedMessage())


def handle_message(message, connection, num_partitions=1,
                   initialize_job=None, current_job_connection=None,
                   num_workers=None,
                   job_finished=None,
                   mark_job_as_finished=None):
    """
    Process the messages received from workers and perform
    any necessary operations accordingly
    :param message: The message to handle
    :param connection: The WorkerConnection of this client
    :param sub_jobs: the sub_jobs list from the server
    :param initialize_job: The server function to set a new job up on command
    :param current_job_connection: the conn that corresponds to overall job submitter
    :param job_finished: the server function to test whether the overall job is finished
    :param mark_job_as_finished: the server function to prep for new job
    :return: Message list to write to worker
    """
    connection.prev_message = message.m_type
    # figured this might as well go here
    connection.last_heartbeat_ack = time.time()

    TIMEOUT_SECONDS = 1
    msg_index = None
    for index, pair in enumerate(connection.expected_messages):
        if message.is_type(pair[0]().m_type):
            msg_index = index
            break
    if msg_index is not None:
        connection.expected_messages.pop(msg_index)

    for pair in connection.expected_messages:
        message_cls, expect_start, num_timeouts = pair
        now = datetime.now()
        if expect_start < now - timedelta(seconds=TIMEOUT_SECONDS):
            handle_ack_timeout(pair, connection, current_job_connection)
    print(message)

    if message.is_type(MessageTypes.SUBMIT_JOB):
        if (current_job_connection is not None):
            return [SubmitJobDeniedMessage(body='Server is busy with another job')]

        if (num_workers() == 0):
            return [SubmitJobDeniedMessage(body='No workers available')]

        mapper_name = SubmitJobMessage.get_mapper_name(message)
        reducer_name = SubmitJobMessage.get_reducer_name(message)
        data_file_path = SubmitJobMessage.get_data_file_path(message)
        # TODO: Check if we can accept this job. Return error if not.
        # Probably just check the server.job_started boolean

        mapper_name = SubmitJobMessage.get_mapper_name(message)
        reducer_name = SubmitJobMessage.get_reducer_name(message)
        data_file_path = SubmitJobMessage.get_data_file_path(message)

        invalid_fields = []
        if not is_valid_package_path(mapper_name, 'Mapper'):
            invalid_fields.append(mapper_name)
        if not is_valid_package_path(reducer_name, 'Reducer'):
            invalid_fields.append(reducer_name)
        if not is_valid_file_path(data_file_path):
            invalid_fields.append(data_file_path)

        if invalid_fields:
            # Not valid
            return [SubmitJobDeniedMessage(
                body='{fields} {verb} invalid path{s}.'.format(
                    fields=', '.join(invalid_fields),
                    verb='is' if len(invalid_fields) == 1 else 'are',
                    s='' if len(invalid_fields) == 1 else ''
                )
            )]
        else:
            initialize_job(connection, mapper_name, reducer_name, data_file_path)
            return [SubmitJobAckMessage()]

    elif message.is_type(MessageTypes.SUBSCRIBE_MESSAGE):
        connection.subscribe()
        connection.worker_id = 'w-' + ''.join(random.choice(string.ascii_lowercase) for _ in range(5))
        return [SubscribeAckMessage()]

    elif message.is_type(MessageTypes.JOB_READY_TO_RECEIVE):
        # Mark that this client has ack'd that a job it is
        # ready to receive the job

        job = connection.current_job
        job.pending_assignment = False
        connection.data_file = job.data_path
        connection.expected_messages.append([JobInstructionsFileAckMessage, datetime.now(), 0])
        connection.expected_messages.append([DataFileAckMessage, datetime.now(), 0])
        return [
            JobInstructionsFileMessage(job.instruction_path, job.instruction_type,
                                       job.num_workers, job.partition_num),
            DataFileMessage(job.data_path)
        ]

    elif message.is_type(MessageTypes.JOB_INSTRUCTIONS_FILE_ACK):
        # Next the client needs to be sent the datafile
        connection.instructions_ackd = True

        if should_send_job_start(connection):
            connection.expected_messages.append([JobStartAckMessage, datetime.now(), 0])
            return [JobStartMessage()]
        return []

    elif message.is_type(MessageTypes.DATAFILE_ACK):
        connection.data_file_ackd = True

        if should_send_job_start(connection):
            connection.expected_messages.append([JobStartAckMessage, datetime.now(), 0])
            return [JobStartMessage()]
        return []

    elif message.is_type(MessageTypes.JOB_START_ACK):
        connection.running = True
        return []

    elif message.is_type(MessageTypes.JOB_DONE):
        connection.result_file = message.get_body()

        # End job
        connection.running = False
        job = connection.current_job
        job.post_execute(connection.result_file)

        if job_finished():  # Overall job
            print('Job finished. Returning results to submitter.')
            # result_file = get_job_result_file_path(num_partitions)
            current_job_connection.expected_messages.append([SubmittedJobFinishedAckMessage, datetime.now(), 0])
            current_job_connection.send_message(
                SubmittedJobFinishedMessage()
            )

        # Reset this connection so that it can be assigned a new job
        connection.prep_for_new_job()

        return [JobDoneAckMessage()]

    elif message.is_type(MessageTypes.SUBMITTED_JOB_FINISHED_ACK):
        print('Marking job as finished')
        mark_job_as_finished()

    elif message.is_type(MessageTypes.JOB_HEARTBEAT):
        # TODO use heartbeat rate to keep track of most efficient clients
        progress = JobHeartbeatMessage.get_progress(message)
        rate = JobHeartbeatMessage.get_rate(message)

        connection.progress = int(progress)
        connection.byte_processing_rate = float(rate)

        return []
