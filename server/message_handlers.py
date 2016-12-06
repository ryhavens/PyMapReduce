import importlib
import random
import string
from messages import *
from filesystems import SimpleFileSystem
import time


def should_send_job_start(conn):
    return conn.datafile_ackd and conn.instructions_ackd


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


def handle_message(message, connection,
                   initialize_job=None, current_job_connection=None,
                   mark_job_as_finished=None):
    """
    Process the messages received from workers and perform
    any necessary operations accordingly
    :param message: The message to handle
    :param connection: The WorkerConnection of this client
    :param initialize_job: The server function to set a new job up on command
    :param 
    :param current_job_connection: the conn that corresponds to overall job submitter
    :param mark_job_as_finished: the server function to prep for new job
    :return: Message list to write to worker
    """
    connection.prev_message = message.m_type

    if message.is_type(MessageTypes.SUBMIT_JOB):
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
        return [
            JobInstructionsFileMessage(job.instruction_path, job.instruction_type),
            DataFileMessage(job.data_path)
        ]

    elif message.is_type(MessageTypes.JOB_INSTRUCTIONS_FILE_ACK):
        # Next the client needs to be sent the datafile
        connection.instructions_ackd = True

        if should_send_job_start(connection):
            return [JobStartMessage()]
        return []

    elif message.is_type(MessageTypes.DATAFILE_ACK):
        connection.datafile_ackd = True

        if should_send_job_start(connection):
            return [JobStartMessage()]
        return []

    elif message.is_type(MessageTypes.JOB_START_ACK):
        return []

    elif message.is_type(MessageTypes.JOB_DONE):
        connection.result_file = message.get_body()

        # End job
        job = connection.current_job
        if not job.is_last():
            job.post_execute(connection.result_file)
        else:
            # No jobs depend on this finishing so send back the result
            current_job_connection.send_message(
                SubmittedJobFinishedMessage(connection.result_file)
            )

        # Reset this connection so that it can be assigned a new job
        connection.prep_for_new_job()

        return [JobDoneAckMessage()]

    elif message.is_type(MessageTypes.SUBMITTED_JOB_FINISHED_ACK):
        mark_job_as_finished()

    elif message.is_type(MessageTypes.JOB_HEARTBEAT):
        # TODO use heartbeat rate to keep track of most efficient clients
        progress = JobHeartbeatMessage.get_progress(message)
        rate = JobHeartbeatMessage.get_rate(message)
        print('%s :: %s, %s' % (connection.file_descriptor, progress, rate) )

        connection.byte_processing_rate = float(rate)
        connection.last_heartbeat_ack = time.time()

        return []
