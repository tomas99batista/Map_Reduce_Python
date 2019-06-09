# Tomas Batista | 89296
import socket
import logging
import uuid
import argparse
import time
import re
import selectors
import types
import string
import json

selector = selectors.DefaultSelector()
id = str(uuid.uuid4())

logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger('worker')

def readData(conn, mask):
        data = conn.recv(4096)      # Should be ready
        #logger.debug('data: %s', data)
        if data:
                handleData(conn, mask, data)
        else:
                logger.debug('Received no data: %s', conn)
                selector.unregister(conn)
                conn.close()

def handleData(conn, mask, data):
        while (b'EOM') not in data:
                data += conn.recv(4096)
        mensagem = json.loads(data.decode('utf-8'))
        logger.debug('Received: %s', mensagem)
        del mensagem['EOM']
        if mensagem['task'] == 'map_request':
                map_work(conn, mask, mensagem['blob'])
        elif mensagem['task'] == 'reduce_request':
                reduce_work(conn, mask, mensagem['value'])
        elif mensagem['task'] == 'work_done':
                logger.info('All the work is done!')

def tokenizer(txt):
        tokens = txt.lower()
        tokens = tokens.translate(str.maketrans('', '', string.digits))
        tokens = tokens.translate(str.maketrans('', '', string.punctuation))
        tokens = tokens.translate(str.maketrans('', '', '«»'))
        tokens = tokens.rstrip()
        return tokens.split()

# Reduce
# lista de listas
def reduce_work(conn, mask, data):
        send_reduce = []
        for word, value in data:
                print(word,value)
                if word in send_reduce:
                        print('DATAAAAAAAAAAAAAAAAAAAAAA %s',send_reduce[:])
                        logger.debug('true')
                        send_reduce[word[0]] += 1
                else:

                        send_reduce.append((word, 1))

        mensagem = {
                "task" :  "reduce_reply" ,
                "value" : send_reduce,
                "EOM" : ""
        }
        conn.send(json.dumps(mensagem).encode('utf-8'))
        logger.debug('Reduced and sent: %s', mensagem)        

# Map
def map_work(conn, mask, blob):
        list = tokenizer(blob)
        send_blob = []
        for k in list:
               send_blob.append((k, 1))
        mensagem = {
                "task" :  "map_reply" ,
                "value" : send_blob
                , "EOM" : ""
        }
        conn.send(json.dumps(mensagem).encode('utf-8'))
        logger.debug('Mapped and sent: %s', mensagem)

def main(args):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((args.hostname, args.port))
        sock.setblocking(False)
        initial_message = {
                "task" :  "register" ,
                "id" : id
                , "EOM" : ""       
        }
        sock.send(json.dumps(initial_message).encode('utf-8'))
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