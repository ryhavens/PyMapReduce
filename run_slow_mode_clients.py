import sys
import signal
from multiprocessing import Process

from client.client import Client

# TODO: Move this to a command line argument
NUM_CLIENTS = 3


def spawn_client():
    Client().run()

def spawn_slow_client():
    Client(slow_mode=True).run()

def main():
    """
    Run the server
    :return:
    """
    processes = []

    for i in range(NUM_CLIENTS):
        p = Process(target=spawn_slow_client)
        p.start()
        processes.append(p)

    p = Process(target=spawn_client)
    p.start()
    processes.append(p)

    for p in processes:
        p.join()

    def signal_handler(signal, frame):
        for p in processes:
            p.terminate()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

if __name__ == '__main__':
    main()