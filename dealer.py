#
#   Hello World client in Python
#   Connects REQ socket to tcp://localhost:5555
#   Sends "Hello" to server, expects "World" back
#

import zmq

context = zmq.Context()

#  Socket to talk to server
print("Connecting to hello world server…")
socket = context.socket(zmq.DEALER)
socket.connect("tcp://localhost:5560")

#  Do 10 requests, waiting each time for a response
for request in range(10, 20):
    print("Sending request %s …" % request)
    socket.send_multipart([b"", b"Hello%s" ])
for request in range(10, 20):
    #  Get the reply.
    message = socket.recv()
    print("Received reply %s [ %s ]" % (request, message))