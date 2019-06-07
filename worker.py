# coding: utf-8
# Tomas Batista | 89296
import socket
import logging
import uuid
import argparse
import time
import re
import selectors
import types
import json

selector = selectors.DefaultSelector()
id = str(uuid.uuid4())

logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger('worker')

def readData(conn, mask):
        data = conn.recv(1024)
        data = json.loads(data.decode('utf8'))        
        if data:
                logger.debug('Received: %s', data)
                if data['task'] == 'map_request':
                        map_work(conn, mask, data['blob'])
                elif data['task'] == 'reduce_request':
                        reduce_work(conn, mask, data['value'])

        else:
                logger.debug('Received no data: %s', conn)
                selector.unregister(conn)
                conn.close()

def reduce_work(conn, mask, data):
        send_reduce = {}
        for word, value in data:
                if word in send_reduce:
                        send_reduce[word] += 1
                else:
                        send_reduce[word] = 1
        mensagem = {
                "task" :  "reduce_reply" ,
                "value" : send_reduce 
        }
        conn.send(json.dumps(mensagem).encode('utf8'))
        logger.debug('Finished reduce, sent to the coord: %s', mensagem)        

def map_work(conn, mask, blob):
        list = re.split('[ \n-!:,;.]+', blob.lower())
        send_blob = []
        for k in list:
               send_blob.append((k, 1))
        mensagem = {
                "task" :  "map_reply" ,
                "value" : send_blob
        }
        conn.send(json.dumps(mensagem).encode('utf8'))
        logger.debug('Finished mapping, sent to the coord: %s', mensagem)

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