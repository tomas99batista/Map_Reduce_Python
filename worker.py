# coding: utf-8
# Tomas Batista | 89296
import socket
import logging
import argparse
import time
import selectors
import types
import json

logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger('worker')

# Function to Receive Data
def receiveData(connection):
    while True:
        data = connection.recv(1024) # receive the response
        logger.debug('Received %s', repr(data))
        # Depois disto vai fazer work

# Function to Process and Send Data
def processData(stdin):
    global s
    msg = {
        "TimeStamp" : time.strftime("%H:%M:%S"),
        "Author" : "Root",
        "Message" : "Ta a funcionar"
    }
    s.send(json.dumps(msg).encode('utf8'))

def start_connections(host, port):
        server_addr = (host, port)
        print('starting connection to', server_addr)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(False)
        sock.connect_ex(server_addr)
        sock.send(b'Connected')
        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        data = types.SimpleNamespace(recv_total=0, outb=b'')
        sel.register(sock, events, data=data)
        
# Main
def main(args):
        start_connections(args.hostname, args.port)
        while True:
            events = sel.select(timeout=None)
            for key, mask in events:
                sock = key.fileobj
                data = key.data
                if mask & selectors.EVENT_READ:
                    recv_data = sock.recv(1024)  # Should be ready to read
                    if recv_data:
                        logger.debug('received %s', repr(recv_data))
                    if not recv_data:
                        sel.unregister(sock)
                        sock.close()
                if mask & selectors.EVENT_WRITE:
                        #logger.debug('echoing %s to %s', repr(data), sock)
                        # sock.send(b'toma')  # Should be ready to write
                        pass

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MapReduce worker')
    parser.add_argument('--port', dest='port', type=int, help='coordinator port', default=8765)
    parser.add_argument('--hostname', dest='hostname', type=str, help='coordinator hostname', default='localhost')
    args = parser.parse_args()
    
    main(args)