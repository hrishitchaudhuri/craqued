import cmd
import argparse

import zmq
from tinyrpc import RPCClient
from tinyrpc.protocols.jsonrpc import JSONRPCProtocol
from tinyrpc.transports.zmq import ZmqClientTransport

class CraquedClient(cmd.Cmd):
    def __init__(self, args):
        super(CraquedClient, self).__init__()
        self.intro = 'Welcome to CRAQUED: Chain Replication - A QUick and Easy Deployment!'
        self.prompt = 'sup?> '

        self._ctx = zmq.Context()
        self._addr = f'tcp://127.0.0.1:{args.coordinator}'

        self._rpcclient = RPCClient(
            JSONRPCProtocol(),
            ZmqClientTransport.create(self._ctx, self._addr)
        )

        self.coordinator = self._rpcclient.get_proxy()

    
    def do_read(self, key):
        """read [KEY]
        Read specified key from tail node.
        """
        r_addr = self.coordinator.get_read_server()

        if r_addr:
            read_server = RPCClient(
                JSONRPCProtocol(),
                ZmqClientTransport.create(zmq.Context(), r_addr)
            ).get_proxy()

            value = read_server.read(key)
            if value > 0:
                print(f'Read {key}: Returned {value}')

            else:
                print(f'[ERR] No value stored for {key}')

        else:
            print('[ERR] No servers have been set up yet. Try later.')


    def do_write(self, input):
        """write [KEY] [VALUE]
        Write specified value for given key
        """
        key, value = input.split()
        value = int(value)
        w_addr = self.coordinator.get_write_server()

        if w_addr:
            write_server = RPCClient(
                JSONRPCProtocol(),
                ZmqClientTransport.create(zmq.Context(), w_addr)
            ).get_proxy()

            res = write_server.write(key, value)
            if res == 0:
                print(f'Successfully wrote {key} : {value}')

        else:
            print('[ERR] No servers have been set up yet. Try later.')


if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--coordinator", type=int, help="Port of the coordinator process.", default=5001)
    args = parser.parse_args()
    CraquedClient(args).cmdloop()