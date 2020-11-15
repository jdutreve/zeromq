#
#  Freelance Pattern
#
#   worker LRU load balancing (ROUTER + ready queue)
#   heartbeat to workers so they restart all (i.e. send READY) if queue is unresponsive
#

from datetime import datetime
import time
import threading

import zmq

from zhelpers import zpipe

# If no server replies within this time, abandon request
REQUEST_TIMEOUT = 2500
REQUEST_RETRIES = 3

HEARTBEAT_INTERVAL = 1000   # Milliseconds
HEARTBEAT_LIVENESS = 3
# 3..5 is reasonable
HEARTBEAT = b''


def p(msg):
    print('%s   %s' % (datetime.now().strftime('%M:%S:%f')[:-3], msg))


class FreelanceClient(object):
    context = None  # Our Context
    commands = None  # command socket in the client thread
    requests = None  # request socket in the client thread
    agent = None  # the background thread

    def __init__(self):
        self.context = zmq.Context()
        self.commands, command_frontend = zpipe(self.context)
        self.requests, request_frontend = zpipe(self.context)
        self.agent = threading.Thread(target=agent_task, args=(self.context, command_frontend, request_frontend))
        self.agent.daemon = True
        self.agent.start()

    def connect(self, endpoint):
        """Connect to new server endpoint
        Sends [CONNECT][endpoint] to the agent
        """
        self.commands.send_multipart([b"CONNECT", endpoint])

    def request(self, msg):
        "Send request, get reply"
        self.requests.send_multipart([b"REQUEST"] + msg)
        #reply = self.requests.recv_multipart()
        #status = reply.pop(0)
        #if status != b"FAILED":
        #    return reply
        #return ''

# =====================================================================
# Asynchronous part, works in the background thread


reply_nb = 0


class FreelanceAgent(object):
    context = None  # Own context
    command_socket = None  # command Socket to talk back to client
    request_socket = None  # request Socket to talk back to client
    backend_socket = None  # Socket to talk to servers
    servers = None  # Servers we've connected to, used for sending PING
    actives = None  # Servers we know are alive (reply or PONG), used for fair load balancing
    sequence = 0  # Number of requests ever sent
    request = None  # Current request if any
    reply = None  # Current reply if any
    request_expires = 0  # Timeout for request/reply

    def __init__(self, context, command_frontend, request_frontend):
        self.context = context
        self.command_socket = command_frontend
        self.request_socket = request_frontend
        self.backend_socket = context.socket(zmq.ROUTER)
        # make sure router doesn't drop unroutable message (host unreachable or again exception)
        self.backend_socket.router_mandatory = 1
        self.backend_socket.hwm = 1000
        self.servers = {}
        self.actives = []
        self.request = None
        self.request_expires = 0
        self.start = time.time()

    def on_command_message(self):
        msg = self.command_socket.recv_multipart()
        command = msg.pop(0)

        if command == b"CONNECT":
            endpoint = msg.pop(0)
            p("I: CONNECTING     %s" % [endpoint])
            self.backend_socket.connect(endpoint)
            server = Server(endpoint)
            self.servers[endpoint] = server
        else:
            p("E: Unknown command %s", command)

    def on_request_message(self):
        msg = self.request_socket.recv_multipart()
        request = msg.pop(0)

        if request == b"REQUEST":
            # assert not self.request  # Strict request-reply cycle
            # Prefix request with sequence number and empty envelope
            self.sequence += 1
            self.request = [str(self.sequence).encode()] + msg
            self.request_expires = time.time() + 1e-3 * REQUEST_TIMEOUT
        else:
            p("E: Unknown request %s", request)

    def on_reply_message(self):
        reply = self.backend_socket.recv_multipart()
        endpoint = reply[0]  # the server that replied
        server = self.servers[endpoint]
        server.reset_server_expiration()

        msg = reply[1:]
        if len(msg) == 1:
            if msg[0] is HEARTBEAT:
                p("I: RECEIVE PONG   %s" % [endpoint])
                server.connected = True
            else:
                p("E: Invalid message from Worker: %s" % reply)
        else:
            global reply_nb
            counter += 1
            p("I: RECEIVE REPLY  %s : counter=%d" % (reply, counter))
            if counter == 200:
                p("DURATION = %d" % (time.time() - self.start))
            # self.request_socket.send_multipart(msg)

        if not server.alive:
            server.alive = True
            p("I: SERVER ACTIVED %s-----------------------" % [server.address])

        # We want to move this responding server at the 'right place' in the actives queue, first remove it
        if server in self.actives:
            self.actives.remove(server)

        # Then, find the server having returned a reply the most recently (i.e. being truly alive)
        most_recently_received_index = 0
        for active in reversed(self.actives):  # reversed() because the most recent is at the end of the queue
            if active.is_last_operation_receive:
                most_recently_received_index = self.actives.index(active) + 1
                break

        # Finally, put the current server just behind the found server (Least Recently Used is the first in the queue)
        self.actives.insert(most_recently_received_index, server)
        server.is_last_operation_receive = True


def agent_task(ctx, command_socket, request_socket):

    agent = FreelanceAgent(ctx, command_socket, request_socket)

    poll_commands = zmq.Poller()
    poll_commands.register(agent.command_socket, zmq.POLLIN)
    poll_commands.register(agent.backend_socket, zmq.POLLIN)

    poll_all = zmq.Poller()
    poll_all.register(agent.command_socket, zmq.POLLIN)
    poll_all.register(agent.backend_socket, zmq.POLLIN)
    poll_all.register(agent.request_socket, zmq.POLLIN)

    while True:
        poller = poll_all if len(agent.actives) > 0 else poll_commands
        events = dict(poller.poll(HEARTBEAT_INTERVAL))

        if events.get(agent.command_socket) == zmq.POLLIN:
            agent.on_command_message()

        if events.get(agent.backend_socket) == zmq.POLLIN:
            agent.on_reply_message()

        if events.get(agent.request_socket) == zmq.POLLIN:
            agent.on_request_message()

            if agent.request and len(agent.actives) > 0:
                # p("I: ACTIVE SERVER  %s" % agent.actives)
                # Round robin active servers
                server = agent.actives.pop(0)
                agent.actives.append(server)

                request = [server.address] + agent.request
                p("I: SEND REQUEST   %s" % request)
                agent.backend_socket.send_multipart(request)
                server.ping_at = now + 1e-3 * HEARTBEAT_INTERVAL
                server.is_last_operation_receive = False  # last operation is now SEND, not RECEIVE
                agent.request = None

        # Remove any expired servers
        now = time.time()
        for server in agent.actives[:]:
            if now >= server.expires:
                p("I: SERVER EXPIRED %s-----------------------" % [server.address])
                server.alive = False
                agent.actives.remove(server)

        # Send PING to idle servers if time has come
        for server in agent.servers.values():
            server.ping(agent.backend_socket, now)


class Server(object):
    address = None  # Server identity/address
    alive = False  # 1 if known to be alive
    connected = False
    ping_at = 0  # Next ping at this time
    expires = 0  # Expires at this time
    is_last_operation_receive = False  # Whether the last action for this server was a receive or send operation

    def __init__(self, address):
        self.address = address
        self.alive = False
        self.connected = False
        self.reset_server_expiration()
        self.is_last_operation_receive = False

    def reset_server_expiration(self):
        time_time = time.time()
        self.ping_at = time_time + 1e-3 * HEARTBEAT_INTERVAL
        self.expires = time_time + 1e-3 * HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS

    def ping(self, backend_socket, now):
        if self.connected and self.alive and now > self.ping_at:
            p("I: SEND PING      %s" % [self.address])
            backend_socket.send_multipart([self.address, HEARTBEAT])
            self.ping_at = now + 1e-3 * HEARTBEAT_INTERVAL
            self.is_last_operation_receive = False  # last operation is now SEND, not RECEIVE

    def tickless(self, tickless):
        if tickless > self.ping_at:
            tickless = self.ping_at
        return tickless

    def __repr__(self):
        return self.address.decode()
