import zmq

context = zmq.Context()

machine1 = context.socket(zmq.REQ)
machine1.connect("tcp://localhost:5555")

machine1.send_string("comment on écrit queu?")
print(machine1.recv_string())
