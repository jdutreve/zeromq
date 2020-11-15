import zmq

context = zmq.Context()

machine2 = context.socket(zmq.REP)
machine2.bind("tcp://*:5555")

question = machine2.recv_string()
print(question)

machine2.send_string("ça s'écrit 'queue'")
