from messages import MessageTypes, SubscribeAckMessage


def handle_message(message, connection):
    """

    :param message: The message to handle
    :param connection: The WorkerConnection of this client
    :return: Message list to write to worker
    """
    if message.is_type(MessageTypes.SUBSCRIBE_MESSAGE.value):
        connection.subscribe()
        return [SubscribeAckMessage()]

    if message.is_type(MessageTypes.JOB_READY_TO_RECEIVE):
        # Send the job files here
        print('Job was requested.. Implement me')
        return []