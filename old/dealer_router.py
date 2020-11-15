import zmq
import random
import sys
import time
import threading

port = "5556"

context = zmq.Context()

client1 = context.socket(zmq.DEALER)
client1.sndhwm = 69
client1.connect("inproc://routing.ipc")

client2 = context.socket(zmq.DEALER)
client2.sndhwm = 70
client2.connect("inproc://routing.ipc")

server = context.socket(zmq.ROUTER)
server.rcvhwm = 100
server.router_mandatory = 1
#server.bind("tcp://*:%s" % port)
#server.bind("ipc://routing.ipc")
server.bind("inproc://routing.ipc")

counter = 1


def server_task():
    while True:
        #msg = server.recv_multipart()
        #print(msg)
        time.sleep(.002)
        #server.send_multipart(msg)


def client_task(client, name):
    ctr = 0
    while True:
        client.send_multipart([b"client message to server1"])
        print("%s %d " % (name, ctr))
        ctr += 1
        time.sleep(.00001)
        #print(client.recv_multipart())


threading.Thread(target=server_task, args=()).start()
threading.Thread(target=client_task, args=(client1,'A')).start()
threading.Thread(target=client_task, args=(client2,'B')).start()

time.sleep(15)
