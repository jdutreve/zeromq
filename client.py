import time
import zmq
import random


def client():
    client_id = random.randrange(1, 2)
    print("I am client #%s" % client_id)

    context = zmq.Context()
    command = context.socket(zmq.PUSH)
    command.connect("tcp://127.0.0.1:5559")
    # command.connect("ipc:///tmp/feeds/0")
    #time.sleep(1)

    for num in range(20):
        work_message = {'num': num + client_id}
        print(work_message)
        command.send_json(work_message)
        time.sleep(0.5)


client()
