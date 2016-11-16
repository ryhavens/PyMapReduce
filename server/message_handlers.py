from messages import MessageTypes, SubscribeAckMessage


def handle_message(message, connection):
    """

    :param message: The message to handle
    :param connection: The WorkerConnection of this client
    :return: Message list to write to worker
    """
    if message.is_type(MessageTypes.SUBSCRIBE_MESSAGE):
        print('subscribe')
        connection.subscribe()
        connection.prev_message = MessageTypes.JOB_READY_TO_RECEIVE
        return [SubscribeAckMessage()]

    if message.is_type(MessageTypes.JOB_READY_TO_RECEIVE):
        # Send the job files here
        print('Job requested...')
        connection.prev_message = MessageTypes.JOB_READY_TO_RECEIVE
        return []

    if message.is_type(MessageTypes.JOB_MAPPING_DONE):
        connection.prev_message = MessageTypes.JOB_MAPPING_DONE
        return []

    if message.is_type(MessageTypes.JOB_REDUCING_DONE):
        connection.prev_message = MessageTypes.JOB_REDUCING_DONE
        print('Client finished job!')
        return []