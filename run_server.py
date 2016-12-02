import sys
import signal
from server.server import Server


def main():
    """
    Run the server
    :return:
    """
    server = Server()

    def signal_handler(signal, frame):
        print('Shutting down...')
        server.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    server.Start()

if __name__ == '__main__':
    main()