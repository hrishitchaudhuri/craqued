import zmq

from tinyrpc.server import RPCServer
from tinyrpc import RPCClient
from tinyrpc.dispatch import RPCDispatcher
from tinyrpc.protocols.jsonrpc import JSONRPCProtocol
from tinyrpc.transports.zmq import ZmqServerTransport, ZmqClientTransport


nodes = []
tail = None

ctx = zmq.Context()
dispatcher = RPCDispatcher()
transport = ZmqServerTransport.create(ctx, 'tcp://127.0.0.1:5001')

rpc_server = RPCServer(
    transport,
    JSONRPCProtocol(),
    dispatcher
)

@dispatcher.public
def register(c_addr):
    global nodes
    global tail

    if c_addr in nodes:
        print("[ERR] Address", c_addr, "already in use")
        return -1

    nodes.append(c_addr)

    if tail:
        print("Tail: ", tail)
        rpc_client = RPCClient(
            JSONRPCProtocol(),
            ZmqClientTransport.create(zmq.Context(), tail)
        )

        tail_server = rpc_client.get_proxy()

        tail_server.update_next(c_addr)

    tail = c_addr
    print("[INFO] Registered", c_addr, "as tail")
    return 0


@dispatcher.public
def get_write_server():
    global nodes
    
    if nodes:
        return nodes[0]
    else:
        return None


@dispatcher.public
def get_read_server():
    global tail

    if tail:
        return tail
    return None

rpc_server.serve_forever()