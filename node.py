import sys

import zmq
from tinyrpc import RPCClient
from tinyrpc.dispatch import RPCDispatcher
from tinyrpc.protocols.jsonrpc import JSONRPCProtocol
from tinyrpc.server import RPCServer
from tinyrpc.transports.zmq import ZmqClientTransport, ZmqServerTransport

curr_ip = 'tcp://127.0.0.1:5002'
global_store = dict()
next_node = None

ctx = zmq.Context()
dispatcher = RPCDispatcher()
transport = ZmqServerTransport.create(ctx, curr_ip)

rpc_server = RPCServer(
    transport,
    JSONRPCProtocol(),
    dispatcher
)

rpc_client = RPCClient(
    JSONRPCProtocol(),
    ZmqClientTransport.create(ctx, 'tcp://127.0.0.1:5001')
)

# is_tail = False

coordinator = rpc_client.get_proxy()
if (coordinator.register(curr_ip) < 0):
    print("[ERR] Coordinator failed to register node.")
    sys.exit(1)

print("[INFO] Successfully registered self.")

@dispatcher.public
def write_value(key, value):
    global_store[key] = value
    if next_node:
        rpc_client = RPCClient(
            JSONRPCProtocol(),
            ZmqClientTransport.create(ctx, next_node)
        )

        next_ = rpc_client.get_proxy()
        if next_.write_value(key, value) == 0: ## ACK chain
            return 0

    return 0


@dispatcher.public
def read_value(key):
    if key in global_store.keys():
        return global_store[key]
    return -9999999


@dispatcher.public
def update_next(n_addr):
    next_node = n_addr
    print("[INFO] Read server is now at", n_addr)
    return 0

rpc_server.serve_forever()

"""
@dispatcher.public
def rmtail():
    if is_tail:
        is_tail = False
        print("[INFO] No longer servicing reads.")
        return True

    else:
        return False

@dispatcher.public
def mktail():
    if is_tail is False:
        is_tail = True
        print("[INFO] Registered", curr_ip, "as tail.")
        return True

    else:
        return False
"""
