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
    if (c_addr in nodes):
        print("[ERR] Node already registered.")
        return -1

    if tail:        
        rpc_client = RPCClient(
            JSONRPCProtocol(),
            ZmqClientTransport.create(ctx, tail)
        )

        tail_server = rpc_client.get_proxy()
        if (tail_server.rmtail() is False):
            print("[ERR] Something has gone horribly wrong.")
            return -2

    nodes.append(c_addr)
    tail = c_addr

    rpc_client = RPCClient(
        JSONRPCProtocol(),
        ZmqClientTransport.create(ctx, tail)
    )
    
    tail_server = rpc_client.get_proxy()
    if (tail_server.mktail() is False):
        print("[ERR] Tail server is already configured.")
        return -3

    return 0