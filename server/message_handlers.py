from messages import *


def handle_message(message, connection):
    """

    :param message: The message to handle
    :param connection: The WorkerConnection of this client
    :return: Message list to write to worker
    """
    if message.is_type(SUBSCRIBE_MESSAGE):
        connection.subscribe()
        return [SubscribeAckMessage()]