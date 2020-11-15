import time
import asyncio
from datetime import datetime

import zmq
import websockets

from jdu.rpc_agent import FreelanceClient
from jdu import rpc_agent

import uvloop
uvloop.install()

# TODO
# synch :           10s 102µs pour 100 000 requests
# poller :          14s 142µs pour 100 000 requests
# async :           20s 200µs pour 100 000 requests
# exception :       26s 264µs pour 100 000 requests
#
# PING/PONG client
#           quid des PING queued/delayed (behind a lot of requests)?
#           client should squeeze too ancient returned PONGs
#               sending a lot PINGs to an expired server, will it reply a lot of (thousands) PONGS?
#           use tickless (finer grained heartbeat timeout)
#           different PING timeout per worker (depending on usage; ex: every 10ms or every 30s)
#  correct shutdown : LINGER sockopt, use disconnect?

# REQUEST_NUMBER = 10
REQUEST_NUMBER = 100_000


def p(msg):
    pass
    # print('%s   %s' % (datetime.now().strftime('%M:%S:%f')[:-3], msg))


async def send_requests_poller():
    full_requester = client.create_requester()
    full_requester2 = client.create_requester()
    loop.create_task(read_replies_poller(full_requester))
    poller = zmq.Poller()
    poller.register(full_requester, zmq.POLLOUT)
    poller.register(full_requester2, zmq.POLLOUT)

    for request_nb in range(REQUEST_NUMBER):
        if full_requester in dict(poller.poll(.000000000001)):
            client.request(full_requester, [b"random name A"])
            client.request(full_requester2, [b"random name B"])
            p("REQUEST %d +++++++++++++++++++++++++++++++++++" % request_nb)
        else:
            print("QUEUE IS FULL REJECTING REQUEST +++++++++++++++++++++++++++++++++++")
        await asyncio.sleep(0)


async def read_replies_poller(full_requester):
    reply_nb = 0
    start = time.time()
    poller = zmq.Poller()
    poller.register(full_requester, zmq.POLLIN)

    while True:
        if full_requester in dict(poller.poll(.000000000001)):
            reply = client.receive(full_requester)
            reply_nb += 1
            if "FAILED" in reply[0].decode():
                p("FAIL %s %d +++++++++++++++++++++++++++++++++++" % (str(reply), reply_nb))
            else:
                p("REPLY %s %d +++++++++++++++++++++++++++++++++++" % (str(reply), reply_nb))
            if reply_nb == REQUEST_NUMBER:
                break
        await asyncio.sleep(0)

    finish(start)


async def init_websocket(websocket, path):
    requester = client.create_requester()
    loop.create_task(send_to_websocket_poller(requester, websocket))
    poller = zmq.Poller()
    poller.register(requester, zmq.POLLOUT)

    while True:
        if requester in dict(poller.poll(.000000000001)):
            client.request(requester, [b"AAAAAA"])
        else:
            print("QUEUE IS FULL REJECTING REQUEST +++++++++++++++++++++++++++++++++++")
        await asyncio.sleep(.5)


async def send_to_websocket_poller(requester, websocket):
    poller = zmq.Poller()
    poller.register(requester, zmq.POLLIN)

    while True:
        if requester in dict(poller.poll(.0001)):
            reply = client.receive(requester)
            await websocket.send(reply[1].decode())
        await asyncio.sleep(0)


async def send_requests_exception():
    full_requester = client.create_requester()
    loop.create_task(read_replies_exception(full_requester))

    for request_nb in range(REQUEST_NUMBER):
        if not client.request(full_requester, [b"random name"]):
            print("QUEUE IS FULL REJECTING REQUEST +++++++++++++++++++++++++++++++++++")
        else:
            p("REQUEST %d +++++++++++++++++++++++++++++++++++" % request_nb)
        await asyncio.sleep(.0000000005)


async def read_replies_exception(full_requester):
    reply_nb = 0
    start = time.time()

    while True:
        try:
            reply = client.receive(full_requester)
        except zmq.Again:
            pass # Nothing to receive
        else:
            reply_nb += 1
            if "FAILED" in reply[0].decode():
                p("FAIL %s %d +++++++++++++++++++++++++++++++++++" % (str(reply), reply_nb))
            else:
                p("REPLY %s %d +++++++++++++++++++++++++++++++++++" % (str(reply), reply_nb))
            if reply_nb == REQUEST_NUMBER:
                break
        await asyncio.sleep(.00000000001)
    finish(start)


def finish(start):
    duration = time.time() - start
    print("**************************** READ REPLIES FINISHED ****************************")
    print("duration %s" % duration)
    print("Average round trip cost: %d µs/req ========================================================================================" %
          (1e6 * duration / REQUEST_NUMBER))
    print("RETRY NB = %d" % rpc_agent.retry_nb)
    print("FAILED NB = %d" % rpc_agent.failed_nb)


def init_client():
    client.connect(b"tcp://127.0.0.1:5557")
    client.connect(b"tcp://127.0.0.1:5556")
    client.connect(b"tcp://127.0.0.1:5558")
    client.connect(b"tcp://127.0.0.1:5555")


client = FreelanceClient()
loop = asyncio.get_event_loop()

if __name__ == '__main__':
    init_client()
    loop.create_task(send_requests_poller())
    # start_server = websockets.serve(init_websocket, "localhost", 5678)
    # loop.run_until_complete(start_server)
    loop.run_forever()
