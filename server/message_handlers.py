from messages import *


def should_send_job_start(conn):
    return conn.datafile_ackd and conn.instructions_ackd


def handle_message(message, connection, sub_jobs):
    """

    :param message: The message to handle
    :param connection: The WorkerConnection of this client
    :return: Message list to write to worker
    """
    if message.is_type(MessageTypes.SUBSCRIBE_MESSAGE):
        print('SUBSCRIBE_MESSAGE received')
        print('subscribe')
        connection.subscribe()
        connection.prev_message = MessageTypes.SUBSCRIBE_MESSAGE
        return [SubscribeAckMessage()]

    if message.is_type(MessageTypes.JOB_READY_TO_RECEIVE):
        print('JOB_READY_TO_RECEIVE received')
        # Mark that this client has ack'd that a job it is
        # ready to receive the job
        connection.prev_message = MessageTypes.JOB_READY_TO_RECEIVE

        job = connection.current_job
        job.pending_assignment = False
        return [
            JobInstructionsFileMessage(job.instruction_path, job.instruction_type),
            DataFileMessage(job.data_path)
        ]

    if message.is_type(MessageTypes.JOB_INSTRUCTIONS_FILE_ACK):
        print('JOB_INSTRUCTIONS_FILE_ACK received')
        # Next the client needs to be sent the datafile
        connection.prev_message = MessageTypes.JOB_INSTRUCTIONS_FILE_ACK
        connection.instructions_ackd = True

        if should_send_job_start(connection):
            return [JobStartMessage()]
        return []

    if message.is_type(MessageTypes.DATAFILE_ACK):
        print('DATAFILE_ACK received')
        connection.prev_message = MessageTypes.DATAFILE_ACK
        connection.datafile_ackd = True

        if should_send_job_start(connection):
            return [JobStartMessage()]
        return []

    if message.is_type(MessageTypes.JOB_START_ACK):
        print('JOB_START_ACK received')
        connection.prev_message = MessageTypes.JOB_START_ACK
        return []

    if message.is_type(MessageTypes.JOB_DONE):
        print('JOB_DONE received')
        connection.prev_message = MessageTypes.JOB_DONE
        connection.result_file = message.get_body()

        # Any jobs that depended on this job finishing should be moved
        # to the queue
        job = connection.current_job
        print(job.pass_result_to)
        if job.pass_result_to:
            for new_job in job.pass_result_to:
                new_job.data_path = message.get_body()
                sub_jobs.append(new_job)
            print(sub_jobs)
        else:
            # No jobs depend on this finishing so print the result
            print('Job finished. Output located in: {}'.format(connection.result_file))

        # Reset this connection so that it can be assigned a new job
        connection.prep_for_new_job()

        return [JobDoneAckMessage()]

    if message.is_type(MessageTypes.JOB_MAPPING_DONE):
        print('JOB_MAPPING_DONE received')
        connection.prev_message = MessageTypes.JOB_MAPPING_DONE
        return []

    if message.is_type(MessageTypes.JOB_REDUCING_DONE):
        print('JOB_REDUCING_DONE received')
        connection.prev_message = MessageTypes.JOB_REDUCING_DONE
        print('Client finished job!')
        return []