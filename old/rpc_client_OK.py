import time

import zmq

from jdu.rpc_agent_ok2 import FreelanceClient

# TODO
# https://kite.com/
# PING/PONG client
#           quid des PING queued/delayed (behind a lot of requests)?
#           client should squeeze too ancient returned PONGs
#               sending a lot PINGs to an expired server, will it reply a lot of (thousands) PONGS?
#           use tickless (finer grained heartbeat timeout)
#           different PING timeout per worker (depending on usage; ex: every 10ms or every 30s)
#  correct shutdown : LINGER sockopt, use disconnect?

from datetime import datetime

from jdu import rpc_agent

RECEIVE_TIMEOUT = .000000000001
REQUEST_NUMBER = 100_000


def p(msg):
    pass
    #print('%s   %s' % (datetime.now().strftime('%M:%S:%f')[:-3], msg))


def main():
    client = FreelanceClient()
    #client.connect(b"tcp://127.0.0.1:5557")
    #client.connect(b"tcp://127.0.0.1:5556")
    #client.connect(b"tcp://127.0.0.1:5558")
    client.connect(b"tcp://127.0.0.1:5555")

    # Send a bunch of requests, measure time
    reply_nb = 0
    start = time.time()
    poller = zmq.Poller()
    poller.register(client.requests, zmq.POLLIN)

    for request_nb in range(REQUEST_NUMBER):
        client.request([b"random name"])
        p("REQUEST %d +++++++++++++++++++++++++++++++++++" % request_nb)
        time.sleep(.0000000005)

        events = dict(poller.poll(RECEIVE_TIMEOUT))
        if events.get(client.requests) == zmq.POLLIN:
            reply = client.requests.recv_multipart()
            reply_nb += 1
            if "FAILED" in reply[0].decode():
                p("FAIL %s %d +++++++++++++++++++++++++++++++++++" % (str(reply), reply_nb))
            else:
                pass
                p("REPLY %s %d +++++++++++++++++++++++++++++++++++" % (str(reply), reply_nb))
            if reply_nb == REQUEST_NUMBER:
                break

    duration = time.time() - start
    print("duration %s" % duration)
    print("Average round trip cost: %d µs/req ========================================================================================" %
          (1e6 * duration / REQUEST_NUMBER))
    print("TOO LATE = %d " % rpc_agent.too_late_nb)


if __name__ == '__main__':
    main()
