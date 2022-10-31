import sys
import argparse
import time

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
def write(key, value):
    global global_store
    global next_node

    global_store[key] = value
    print(f'[INFO] Stored {key} : {value}')

    if next_node:
        rpc_client = RPCClient(
            JSONRPCProtocol(),
            ZmqClientTransport.create(zmq.Context(), next_node)
        )

        next_ = rpc_client.get_proxy()
        if next_.write(key, value) == 0: ## ACK chain
            return 0

        else:
            return -1

    return 0


@dispatcher.public
def read(key):
    global global_store

    if key in global_store.keys():
        return global_store[key]
    return -12


@dispatcher.public
def update_next(n_addr):
    global next_node
    global curr_ip

    next_node = n_addr
    if next_node:
        print("[INFO] Read server is now at", next_node)
    else:
        print("[INFO] Read server is now at", curr_ip)
    return 0


@dispatcher.public
def probe():
    print(f'[INFO] Probe received at {time.time()}')
    return True

rpc_server.serve_forever()