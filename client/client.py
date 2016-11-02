import struct
import socket

server_address = ('localhost', 8888)
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

sock.connect(server_address)

sock.sendall(struct.pack('>Bi', 1, 0))
sock.sendall(struct.pack('>Bi', 2, 5))
sock.sendall(struct.pack('5s', bytes('hello', encoding='utf-8')))