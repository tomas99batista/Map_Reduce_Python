# coding: utf-8
# Tomas Batista | 89296
import socket
import logging
import uuid
import argparse
import time
import selectors
import types
import json

selector = selectors.DefaultSelector()
id = str(uuid.uuid4())

logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger('worker')

def readData( conn, mask):
        data = conn.recv(1024)
        if data:
                logger.debug('Received: %s'. data)
                if data['task'] == 'register':

        else:
                pass
                #logger.debug('Received no data: %s', conn)
                # selector.unregister(conn)
                # conn.close()

def 

def main( args):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((args.hostname, args.port))
        sock.setblocking(False)
        initial_message = {
                "task" :  "register" ,
                "id" : id
        }
        sock.send(json.dumps(initial_message ).encode('utf8'))
        logger.debug('Sent: %s ', initial_message )

        while True:
                selector.register(sock, selectors.EVENT_READ, readData)
                while True:
                    events = selector.select()
                    for key, mask in events:
                        callback = key.data
                        callback(key.fileobj, mask)



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MapReduce worker')
    parser.add_argument('--port', dest='port', type=int, help='coordinator port', default=8765)
    parser.add_argument('--hostname', dest='hostname', type=str, help='coordinator hostname', default='localhost')
    args = parser.parse_args()
    
    main(args)