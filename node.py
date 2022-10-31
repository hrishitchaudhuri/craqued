from xml.dom.minidom import parseString
import zmq

from tinyrpc.server import RPCServer
from tinyrpc.dispatch import RPCDispatcher
from tinyrpc.protocols.jsonrpc import JSONRPCProtocol
from tinyrpc.transports.zmq import ZmqServerTransport

curr_ip = 'tcp://127.0.0.1:5002'

ctx = zmq.Context()
dispatcher = RPCDispatcher()
transport = ZmqServerTransport.create(ctx, curr_ip)

rpc_server = RPCServer(
    transport,
    JSONRPCProtocol(),
    dispatcher
)

is_tail = False

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