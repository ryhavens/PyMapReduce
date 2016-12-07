from client.client import Client


def main():
    """
    Run the server
    :return:
    """
    client = Client(slow_mode=True)
    client.run()

if __name__ == '__main__':
    main()