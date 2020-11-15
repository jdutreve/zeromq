#
#   Hello World server in Python
#   Binds REP socket to tcp://*:5555
#   Expects b"Hello" from client, replies with b"World"
#

import time
import zmq

context = zmq.Context()
socket = context.socket(zmq.ROUTER)
socket.connect("tcp://localhost:5560")

print("Waiting for request")
while True:
    #  Wait for next request from client
    message = socket.recv_multipart()
    print("Received request: %s" % message)

    #  Do some 'work'
    time.sleep(.4)

    #  Send reply back to client
    socket.send(b"World")
