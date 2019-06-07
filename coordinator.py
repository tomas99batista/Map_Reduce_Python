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

def accept(sock, mask):
    conn, addr = sock.accept()  # Should be ready to read
    logger.debug('accepted %s from %s', conn, addr)
    conn.setblocking(False)
    sel.register(conn, selectors.EVENT_READ, read)

def read(conn, mask):
    data = conn.recv(1024)      # Should be ready
    data = json.loads(data.decode('utf8'))
    if data:
        logger.debug('Received data: %s', data)
        if data['task'] == 'register':
            socks_list[data['id']] = conn
            logger.debug('Joined Worker: %s\n\tsocks_list: %s', data['id'], socks_list)
        
    else:
        logger.debug('closing %s', conn)
        sel.unregister(conn)
        conn.close()

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
            logger.debug('Blob: %s', blob)
            datastore.append(blob)

    hist = []
    # store final histogram into a CSV file
    with args.out as f:
        csv_writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for w,c in hist:
            csv_writer.writerow([w,c])

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
    parser.add_argument('-f', dest='file', type=argparse.FileType('r', encoding='UTF-8'), help='input file path', default=)
    parser.add_argument('-o', dest='out', type=argparse.FileType('w', encoding='UTF-8'), help='output file path', default='output.csv')
    parser.add_argument('-b', dest ='blob_size', type=int, help='blob size', default=1024)
    args = parser.parse_args()
    
    
    main(args)