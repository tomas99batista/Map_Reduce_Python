# coding: utf-8
# Tomas Batista | 89296
import socket
import logging
import argparse
import csv
import selectors
import types
import json

logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger('coordinator')

#Lista de blobs
datastore = []

# Lista de sockets
socks_list = {}

# Selector
sel = selectors.DefaultSelector()

# Lista de tarefas a fazer
tasks = []

# Accept an incoming socket
def accept(sock, mask):
    conn, addr = sock.accept()  # Should be ready to read
    logger.debug('accepted %s from %s', conn, addr)
    conn.setblocking(False)
    sel.register(conn, selectors.EVENT_READ, readData)

def readData(conn, mask):
        data = conn.recv(4096)      # Should be ready
        #logger.debug('data: %s', data)
        if data:
                handleData(conn, mask, data)
        else:
                logger.debug('Received no data: %s', conn)
                sel.unregister(conn)
                conn.close()

def handleData(conn, mask, data):
        while (b'EOM') not in data:
                data += conn.recv(4096)
        mensagem = json.loads(data.decode('utf-8'))
        logger.debug('Received: %s', mensagem)
        del mensagem['EOM']        
        # Register        
        if mensagem['task'] == 'register':
            socks_list[mensagem['id']] = conn
            logger.debug('Joined Worker: %s\n\tsocks_list: %s', mensagem['id'], socks_list)
            # Map_ send
            if len(datastore) > 0:
                map_send(conn, mask)
            else:
                message = {'task':'nothing_to_do', "EOM": "" }
        # Map Reply
        elif mensagem['task'] == 'map_reply':
            logger.debug('Received map_reply: %s', mensagem['value'])
            tasks.append(mensagem['value'])
            logger.debug('Tasks: %s', tasks)
            if len(datastore) > 0:
                map_send(conn, mask)
            else:
                reduce_send(conn, mask)
        # Reduce Reply
        elif mensagem['task'] == 'reduce_reply':
            if len(tasks) > 0:
                reduce_send(conn, mask)
                tasks.append(mensagem['value'])
            else:
                message = {'task': 'work_done', 'EOM': ''}
                conn.send(json.dumps(message).encode('utf-8'))
                logger.debug("Reduce done: %s", mensagem['value'])
                # store final histogram into a CSV file
                with args.out as f:
                    csv_writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                    for w,c in mensagem['value']:
                        csv_writer.writerow([w,c])

# Reduce Request
def reduce_send(conn, mask):
    if len(tasks) > 2:
        message = {
            "task" :  "reduce_request" ,
            "value" :   [tasks[0], tasks[1]],
            "EOM" : ""
        }
        del tasks[0:1]
    else:
        message = {
            "task" :  "reduce_request" ,
            "value" :   tasks[0],
            "EOM" : ""
        }    
        tasks.pop(0)
    conn.send(json.dumps(message).encode('utf-8'))
    logger.debug('Sent reduce_request: %s', message)  

# Map Request
def map_send(conn, mask):
    message = {
    "task" :  "map_request" ,
    "blob" : datastore[-1],
    "EOM" : ""
    }
    conn.send(json.dumps(message).encode('utf-8'))
    #logger.debug('Datastore bfr removing: %s', datastore)
    datastore.pop(-1)
    #logger.debug('Datastore after removing: %s', datastore)
    logger.debug('Sent Blob: %s', message)

def main(args):
    # Aqui vão ser criadas as blobs
    global datastore
    # load txt file and divide it into blobs
    with args.file as f:
        while True:
            blob = f.read(args.blob_size)
            if not blob:
                break
            # This loop is used to not break word in half
            while not str.isspace(blob[-1]):
                ch = f.read(1)
                if not ch:
                    break
                blob += ch
            #logger.debug('Blob: %s', blob)
            datastore.append(blob)
    #print (datastore)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('0.0.0.0', args.port))
    sock.listen(100)
    sock.setblocking(False)
    while True:
        sel.register(sock, selectors.EVENT_READ,accept)
        while True:
            events = sel.select()
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MapReduce Coordinator')
    parser.add_argument('-p', dest='port', type=int, help='coordinator port', default=8765)
    parser.add_argument('-f', dest='file', type=argparse.FileType('r', encoding='utf-8'), help='input file path', default='raposa e as uvas.txt')
    parser.add_argument('-o', dest='out', type=argparse.FileType('w', encoding='utf-8'), help='output file path', default='output.csv')
    parser.add_argument('-b', dest ='blob_size', type=int, help='blob size', default=1024)
    args = parser.parse_args()
    
    
    main(args)