import zmq
import random
import sys
import time
import threading

port = "5556"

server_context = zmq.Context()
server = server_context.socket(zmq.PAIR)
server.bind("tcp://*:%s" % port)

client_context = zmq.Context()
client = client_context.socket(zmq.PAIR)
client.connect("tcp://localhost:%s" % port)


def server_task():
    while True:
        server.send(b"Server message to client3")
        msg = server.recv()
        print(msg)
        time.sleep(1)


def client_task(client):
    while True:
        msg = client.recv()
        print(msg)
        client.send(b"client message to server1")
        client.send(b"client message to server2")
        time.sleep(1)


threading.Thread(target=client_task, args=(agent_context, agent_command_socket, agent_request_socket)).start()
