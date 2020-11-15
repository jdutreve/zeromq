#
#  Paranoid Pirate worker
#
#   Author: Daniel Lundin <dln(at)eintr(dot)org>
#
#   restart DEALER socket when queue restarts (timeout to receive queue heartbeat)
#   heartbeat to queue so queue unregisters this worker when unresponsive
#

import time
import sys
from random import choice, randint
from datetime import datetime

from threading import Thread
from multiprocessing import Process

import zmq


Container = Thread

HEARTBEAT = b''
cycle = 0

port = sys.argv[1]
load_duration = float(sys.argv[2])

bind_endpoint = ("tcp://*:" + port).encode()
connect_endpoint = ("tcp://127.0.0.1:" + port).encode()


def p(msg):
    print('%s   %s' % (datetime.now().strftime('%M:%S:%f')[:-3], msg))


def on_request(request, identity):
    address, control = request[:2]
    reply = [address, control]
    if control == HEARTBEAT:
        reply[1] = HEARTBEAT
        #p("I: RETURN PONG:  %s" % reply)
    else:
        global cycle
        cycle = 1 + cycle
        reply.append(b"ACK" + control + b"-" + identity.encode())
        #p("I: %s RETURN REPLY: %s, CYCLE=%d" % (identity, reply, cycle))
        time.sleep(load_duration)  # Do some heavy work
    return reply


def worker_socket(identity):
    context = zmq.Context.instance()
    dealer = context.socket(zmq.DEALER)
    dealer.setsockopt_unicode(zmq.IDENTITY, identity)
    dealer.connect("ipc://routing.ipc")
    while True:
        request = dealer.recv_multipart()
        reply = on_request(request, identity)
        dealer.send_multipart(reply)


def server_socket(context, bind, connect):
    server = context.socket(zmq.ROUTER)
    server.hwm = 1000
    server.identity = connect
    server.probe_router = 1
    server.bind(bind)
    p("I: worker is ready at %s" % connect.decode())
    return server


context = zmq.Context(1)
server = server_socket(context, bind_endpoint, connect_endpoint)

router = context.socket(zmq.ROUTER)
router.bind("ipc://routing.ipc")

identities = [b'A', b'B', b'C', b'D', b'E', b'F', b'G']
for identity in identities:
    Container(target=worker_socket, args=(identity.decode())).start()

time.sleep(1)  # Wait for threads to stabilize

poller = zmq.Poller()
poller.register(router, zmq.POLLIN)
poller.register(server, zmq.POLLIN)
max = randint(90, 500)

while True:
    try:
        events = dict(poller.poll())

        if events.get(server) == zmq.POLLIN:
            identity = choice(list(i for i in identities))
            request = server.recv_multipart()
            p("I: RECEIVE REQUEST: %s" % request)
            router.send_multipart([identity] + request)

        if events.get(router) == zmq.POLLIN:
            reply = router.recv_multipart()
            reply = reply[1:]
            server.send_multipart(reply)
            p("I: RETURN REPLY: %s" % reply)

        if cycle > max and port in ['5555','5556'] and randint(0, 3500000000) == 0:
            p("I: Simulating CPU overload ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            time.sleep(randint(2, 6))
        if cycle > max and randint(0, 5950000) == 0:
            p("I: Simulating a crash")
            import _thread
            _thread.interrupt_main()
            break

    except:
        p("I: Interrupted!!!!!!!!!!")
        break
