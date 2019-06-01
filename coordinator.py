# coding: utf-8
# Tomas Batista | 89296
import socket
import logging
import argparse
import selectors
import types

logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger('coordinator')

# Lista de sockets
socks_list = {}

# Selector
sel = selectors.DefaultSelector()

def accept(sock, mask):
    conn, addr = sock.accept()  # Should be ready to read
    logger.debug('accepted %s from %s', conn, addr)
    conn.setblocking(False)
    socks_list[addr[1]] = conn
    logger.debug('socks_list: %s', socks_list)
    sel.register(conn, selectors.EVENT_READ, read)

def read(conn, mask):
    data = conn.recv(1024)      # Should be ready
    if data:
        logger.debug('echoing %s to %s', repr(data), conn)
        for port in socks_list.keys():
            socks_list[port].send(data)
    else:
        logger.debug('closing %s', conn)
        sel.unregister(conn)
        conn.close()

def main(args):
    # Aqui vão ser criadas as blobs
    datastore = []
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
            #logger. debug('Blob: %s', blob)
            datastore.append(blob)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
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
    parser.add_argument('-f', dest='file', type=argparse.FileType('r'), help='file path', default='lusiadas.txt')
    parser.add_argument('-b', dest ='blob_size', type=int, help='blob size', default=1024)
    args = parser.parse_args()
    
    main(args)