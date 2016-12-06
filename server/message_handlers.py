from PMRJob.job import get_job_result_file_path
from messages import *


def should_send_job_start(conn):
    return conn.datafile_ackd and conn.instructions_ackd


def handle_message(message, connection, num_partitions=1,
                   initialize_job=None, current_job_connection=None,
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

    if message.is_type(MessageTypes.SUBMIT_JOB):
        # TODO: Check if we can accept this job. Return error if not.
        # Probably just check the server.job_started boolean

        mapper_name = SubmitJobMessage.get_mapper_name(message)
        reducer_name = SubmitJobMessage.get_reducer_name(message)
        data_file_path = SubmitJobMessage.get_data_file_path(message)

        initialize_job(connection, mapper_name, reducer_name, data_file_path)

        return [SubmitJobAckMessage()]

    elif message.is_type(MessageTypes.SUBSCRIBE_MESSAGE):
        connection.subscribe()
        return [SubscribeAckMessage()]

    elif message.is_type(MessageTypes.JOB_READY_TO_RECEIVE):
        # Mark that this client has ack'd that a job it is
        # ready to receive the job

        job = connection.current_job
        job.pending_assignment = False
        return [
            JobInstructionsFileMessage(job.instruction_path, job.instruction_type,
                                       job.num_workers, job.partition_num),
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
        job.post_execute(connection.result_file)

        if job_finished():  # Overall job
            print('Job finished. Returning results to submitter.')
            result_file = get_job_result_file_path(num_partitions)
            current_job_connection.send_message(
                SubmittedJobFinishedMessage(result_file)
            )

        # Reset this connection so that it can be assigned a new job
        connection.prep_for_new_job()

        return [JobDoneAckMessage()]

    elif message.is_type(MessageTypes.SUBMITTED_JOB_FINISHED_ACK):
        mark_job_as_finished()
        print('Ready for new job')

    elif message.is_type(MessageTypes.JOB_HEARTBEAT):
        # TODO use heartbeat rate to keep track of most efficient clients
        print('%s :: %s, %s' % (connection.file_descriptor, 
            JobHeartbeatMessage.get_progress(message), 
            JobHeartbeatMessage.get_rate(message)) )

        return []
