import sys
import argparse

import zmq
from tinyrpc import RPCClient
from tinyrpc.dispatch import RPCDispatcher
from tinyrpc.protocols.jsonrpc import JSONRPCProtocol
from tinyrpc.server import RPCServer
from tinyrpc.transports.zmq import ZmqClientTransport, ZmqServerTransport

parser = argparse.ArgumentParser()
parser.add_argument("-p", "--port", type=int, help="Port to run the coordinator process on.", default=5002)
args = parser.parse_args()

curr_ip = f'tcp://127.0.0.1:{args.port}'
global_store = dict()
next_node = None

s_ctx = zmq.Context()
c_ctx = zmq.Context()

dispatcher = RPCDispatcher()
transport = ZmqServerTransport.create(s_ctx, curr_ip)

rpc_server = RPCServer(
    transport,
    JSONRPCProtocol(),
    dispatcher
)

rpc_client = RPCClient(
    JSONRPCProtocol(),
    ZmqClientTransport.create(c_ctx, 'tcp://127.0.0.1:5001')
)

# is_tail = False

coordinator = rpc_client.get_proxy()
if (coordinator.register(curr_ip) < 0):
    print("[ERR] Coordinator failed to register node.")
    sys.exit(1)

print("[INFO] Successfully registered self.")

@dispatcher.public
def write_value(key, value):
    global global_store
    global next_node

    global_store[key] = value
    if next_node:
        rpc_client = RPCClient(
            JSONRPCProtocol(),
            ZmqClientTransport.create(zmq.Context(), next_node)
        )

        next_ = rpc_client.get_proxy()
        if next_.write_value(key, value) == 0: ## ACK chain
            return 0

    return 0


@dispatcher.public
def read_value(key):
    global global_store

    if key in global_store.keys():
        return global_store[key]
    return -12


@dispatcher.public
def update_next(n_addr):
    global next_node

    next_node = n_addr
    print("[INFO] Read server is now at", n_addr)
    return 0

rpc_server.serve_forever()