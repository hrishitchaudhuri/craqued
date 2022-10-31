from threading import Timer

import zmq
from tinyrpc import RPCClient
from tinyrpc.exc import TimeoutError
from tinyrpc.dispatch import RPCDispatcher
from tinyrpc.protocols.jsonrpc import JSONRPCProtocol
from tinyrpc.server import RPCServer
from tinyrpc.transports.zmq import ZmqClientTransport, ZmqServerTransport

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

def deregister(c_addr):
    global nodes
    global tail

    if c_addr not in nodes:
        print(f'[ERR] {c_addr} was never active')
        return -1

    if c_addr == tail:
        if len(nodes) > 1:
            tail = nodes[-2]
            nodes.pop()

            tail_server = RPCClient(
                JSONRPCProtocol(),
                ZmqClientTransport.create(zmq.Context(), tail)
            ).get_proxy()

            tail_server.update_next(None)

        else:
            tail = None
            nodes.pop()

    elif c_addr == nodes[0]:
        nodes.remove(c_addr)

    else:
        idx = nodes.index(c_addr)
        nodes.remove(c_addr)

        _server = RPCClient(
            JSONRPCProtocol(),
            ZmqClientTransport.create(zmq.Context(), nodes[idx - 1])
        ).get_proxy()

        _server.update_next(nodes[idx])

def send_probe(c_addr):
    _server = RPCClient(
        JSONRPCProtocol(),
        ZmqClientTransport.create(zmq.Context(), c_addr, timeout=2.0)
    ).get_proxy()

    try:    
        print(f'Probing {c_addr}...')
        _server.probe()
        t = Timer(10.0, send_probe, args=[c_addr])
        t.start()
    
    except TimeoutError:
        print(f'{c_addr} did not respond. Deregistering...')
        deregister(c_addr)
        print(f'[INFO] Successfully deregistered {c_addr}.')

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

    t = Timer(10.0, send_probe, args=[c_addr])
    t.start()
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
