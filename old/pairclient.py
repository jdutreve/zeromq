import zmq
import random
import sys
import time

port = "5556"
context = zmq.Context()
socket = context.socket(zmq.PAIR)
socket.connect("inproc://toto")

while True:
    msg = socket.recv()
    print(msg)
    socket.send(b"client message to server1")
    socket.send(b"client message to server2")
    time.sleep(1)