from client.client import Client


def main():
    """
    Run the server
    :return:
    """
    client = Client()
    client.run()

if __name__ == '__main__':
    main()