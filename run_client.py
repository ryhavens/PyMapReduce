import sys
import signal

from client.client import Client


def main():
    """
    Run the server
    :return:
    """
    client = Client()

    def signal_handler(signal, frame):
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    client.run()

if __name__ == '__main__':
    main()
