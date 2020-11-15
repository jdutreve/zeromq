import time
import random
from threading import Thread

import zmq


NBR_WORKERS = 10


def worker_thread(ctx=None, id=None):
    ctx = ctx or zmq.Context.instance()
    #worker = ctx.socket(zmq.DEALER)
    worker = ctx.socket(zmq.REQ)

    worker.connect("tcp://localhost:5671")
    worker.setsockopt(zmq.IDENTITY, id)

    total = 0
    while True:
        # Tell the router we're ready for work
        worker.send(b"ready")

        # Get workload from router, until finished
        workload = worker.recv()
        finished = workload == b"END"
        if finished:
            print("Processed: %d tasks" % total)
            break
        total += 1

        # Do some random work
        time.sleep(0.1 * random.random())


context = zmq.Context.instance()
client = context.socket(zmq.ROUTER)
client.bind("tcp://*:5671")

for i in range(NBR_WORKERS):
    Thread(target=worker_thread, args=(context, str(i).encode())).start()

for _ in range(NBR_WORKERS * 10):
    # LRU worker is next waiting in the queue
    address, empty, ready = client.recv_multipart()

    client.send_multipart([
        address,
        b'',
        b'This is the workload',
    ])

# Now ask mama to shut down and report their results
for _ in range(NBR_WORKERS):
    address, empty, ready = client.recv_multipart()
    client.send_multipart([
        address,
        b'',
        b'END',
    ])