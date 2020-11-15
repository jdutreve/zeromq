import time
import zmq
import random


def server():
    context = zmq.Context()
    command = context.socket(zmq.PULL)
    command.connect("tcp://127.0.0.1:5560")
    # command.bind("ipc:///tmp/feeds/0")
    #command.setsockopt(zmq.SUBSCRIBE, b"")

    while True:
        work = command.recv_json()
        print(work)


server()
