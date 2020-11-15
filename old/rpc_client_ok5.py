import time
import asyncio
from datetime import datetime

import websockets

from jdu.rpc_agent_ok4 import FreelanceClient
from jdu import rpc_agent_ok4 as rpc_agent

import uvloop
uvloop.install()

# TODO
# improve receive performance by using uvloop
# https://kite.com/
# PING/PONG client
#           quid des PING queued/delayed (behind a lot of requests)?
#           client should squeeze too ancient returned PONGs
#               sending a lot PINGs to an expired server, will it reply a lot of (thousands) PONGS?
#           use tickless (finer grained heartbeat timeout)
#           different PING timeout per worker (depending on usage; ex: every 10ms or every 30s)
#  correct shutdown : LINGER sockopt, use disconnect?

REQUEST_NUMBER = 20000


def p(msg):
    pass
    #print('%s   %s' % (datetime.now().strftime('%M:%S:%f')[:-3], msg))


async def send_to_websocket(requester, websocket):
    try:
        while True:
            reply = await client.receive(requester)
            if reply:
                await websocket.send(reply[1].decode())
            else:
                print("NULL")
    except Exception as e:
        print(e)
    finally:
        pass


async def init_websocket(websocket, path):
    try:
        requester = client.create_requester()
        asyncio.get_event_loop().create_task(send_to_websocket(requester, websocket))
        while True:
            client.request(requester, [b"AAAAAA"])
            await asyncio.sleep(.05)
    except Exception as e:
        print(e)
    finally:
        pass

#start_server = websockets.serve(init_websocket, "localhost", 5678)


async def send_requests():
    full_requester = client.create_requester()
    loop.create_task(read_replies(full_requester))

    request_nb = (i for i in range(REQUEST_NUMBER))
    expect_request = True
    while expect_request:
        try:
            c = next(request_nb)
        except:
            expect_request = False
        else:
            client.request(full_requester, [b"random name"])
            p("REQUEST %d +++++++++++++++++++++++++++++++++++" % (c))
            await asyncio.sleep(.0000005)


async def read_replies(full_requester):
    reply_nb = 0
    start = time.time()
    while True:
        reply = await client.receive(full_requester)
        reply_nb += 1
        if "FAILED" in reply[0].decode():
            p("FAIL %s %d +++++++++++++++++++++++++++++++++++" % (str(reply), reply_nb))
        else:
            pass
            p("REPLY %s %d +++++++++++++++++++++++++++++++++++" % (str(reply), reply_nb))
        if reply_nb == REQUEST_NUMBER:
            break

    duration = time.time() - start
    p("**************************** READ REPLIES FINISHED ****************************")
    print("duration %s" % duration)
    print("Average round trip cost: %d µs/req ========================================================================================" %
          (1e6 * duration / REQUEST_NUMBER))
    print("TOO LATE = %d " % rpc_agent.too_late_nb)
    loop.stop()


async def init_client():
    #await client.connect(b"tcp://127.0.0.1:5557")
    #await client.connect(b"tcp://127.0.0.1:5556")
    #await client.connect(b"tcp://127.0.0.1:5558")
    await client.connect(b"tcp://127.0.0.1:5555")
    loop.create_task(send_requests())
    #loop.run_until_complete(start_server)


loop = asyncio.get_event_loop()
client = FreelanceClient()


if __name__ == '__main__':
    loop.create_task(init_client())
    loop.run_forever()
