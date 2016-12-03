from messages import *


def should_send_job_start(conn):
    return conn.datafile_ackd and conn.instructions_ackd


def handle_message(message, connection):
    """
    Process the messages received from workers and perform
    any necessary operations accordingly
    :param message: The message to handle
    :param connection: The WorkerConnection of this client
    :return: Message list to write to worker
    """
    connection.prev_message = message.m_type
    if message.is_type(MessageTypes.SUBSCRIBE_MESSAGE):
        connection.subscribe()
        return [SubscribeAckMessage()]

    if message.is_type(MessageTypes.JOB_READY_TO_RECEIVE):
        # Mark that this client has ack'd that a job it is
        # ready to receive the job

        job = connection.current_job
        job.pending_assignment = False
        return [
            JobInstructionsFileMessage(job.instruction_path, job.instruction_type),
            DataFileMessage(job.data_path)
        ]

    if message.is_type(MessageTypes.JOB_INSTRUCTIONS_FILE_ACK):
        # Next the client needs to be sent the datafile
        connection.instructions_ackd = True

        if should_send_job_start(connection):
            return [JobStartMessage()]
        return []

    if message.is_type(MessageTypes.DATAFILE_ACK):
        connection.datafile_ackd = True

        if should_send_job_start(connection):
            return [JobStartMessage()]
        return []

    if message.is_type(MessageTypes.JOB_START_ACK):
        return []

    if message.is_type(MessageTypes.JOB_DONE):
        connection.result_file = message.get_body()

        # End job
        job = connection.current_job
        print(job.pass_result_to)
        if not job.is_last():
            job.post_execute(connection.result_file)
        else:
            # No jobs depend on this finishing so print the result
            print('Job finished. Output located in: {}'.format(connection.result_file))

        # Reset this connection so that it can be assigned a new job
        connection.prep_for_new_job()

        return [JobDoneAckMessage()]
