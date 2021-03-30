import zmq
import os
import json
import math
from random import choice
import string
import hashlib

context = zmq.Context()
socket = context.socket(zmq.REP)

partsize = 1024 * 1024 * 2
spath = 'server/'


class Server:
    def __init__(self, socket, id, genesis, successor, predecessor, responsibility, first):
        self.socket = socket
        self.id = id
        self.genesis = genesis
        self.successor = successor
        self.predecessor = predecessor
        self.responsibility = responsibility
        self.first = first

server = Server('', 0, False, '', '', [], False)

# --------------------------------------------------------------------------------------------------------
# MAIN FUNCTIONS

def initServer():
    while True:
        isGenesis = input('Is this the genesis node?: y / n ...\n')
        if isGenesis == 'y' or isGenesis == 'n':
            break
    ip = str(input('Please insert ip address for current server: '))
    port = str(input('Now insert port number for server: '))
    server.socket = f'{ip}:{port}'
    server.id = generateId(server.socket)
    socket.bind(f'tcp://{server.socket}')
    if(isGenesis == 'y'):
        server.genesis = True
        server.successor = server.socket
        server.predecessor = server.socket
        server.responsibility = [[server.id, (2**160) - 1], [0, server.id]]
        server.first = True
    else:
        server.genesis = False
        connect_ip = str(input('Please insert ip for connection: '))
        connect_port = str(input('Now insert port number for connection: '))
        resp = may_connect(f'{connect_ip}:{connect_port}')
        while True:
            if (resp[0].decode() == 'yes'):
                print(f'Can connect next to server as: {resp[1].decode()}\n')
                break
            else:
                resp = may_connect(resp[1].decode())
                print('Cannot connect to this server, asking next one...\n')
        if (resp[1].decode() == 'last'):
            get_resp_range(server.id, 'last', resp[2].decode())
        elif (resp[1].decode() == 'first'):
            server.first = True
            get_resp_range(server.id, 'first', resp[2].decode())
        elif (resp[1].decode() == 'normal'):
            get_resp_range(server.id, 'normal', resp[2].decode())


def serverUp():
    while True:
        print('-----------------------------------------------------------------------------------------------------------')
        print(f'id: {server.id} - {server.socket}\n')
        print(f'Responsibility range is: \n')
        printResRange()
        print('Waiting for requests ...\n')
        request = socket.recv_multipart()
        cmd = request[0].decode()
        if cmd == 'connect':
            connect_request(int(request[1].decode()))
        elif cmd == 'add':
            add_server(int(request[1].decode()), request[2].decode(), request[3].decode())
        elif cmd == 'new successor':
            update_successor(request[1].decode())
        elif cmd == 'delegate files':
            receive_old_files(request[1].decode(), request[2])
        elif cmd == 'upload':
            upload(int(request[1].decode()))
        elif cmd == 'send':
            save_part(request[1].decode(), request[2])
        elif cmd == 'ask download':
            ask_download(request[1].decode())
        elif cmd == 'download':
            download(request[1].decode())
        elif cmd == 'download part':
            download_part(request[1].decode())
        print('Operation completed!\n')

# --------------------------------------------------------------------------------------------------------
# FUNCTIONS FOR RESPONSE

def download_part(id_string):
    with open(spath + id_string, 'rb') as f:
        content = f.read()
    print('Filepart successfully sent')
    socket.send_multipart(['Downloading...'.encode(), content])

def download(chord_name):
    print('Downloading file...')
    files = os.listdir(spath)
    for file in files:
        fullname = os.path.splitext(file)
        name = fullname[0]
        ext = fullname[1]
        if (name == chord_name) or (ext == chord_name):
            with open(spath + file, 'rb') as f:
                content = f.read()
            socket.send_multipart(['Downloading...'.encode(), content])

def ask_download(filename):
    print('Checking if file is in server for download')
    files = os.listdir(spath)
    inserver = False
    for file in files:
        fullname = os.path.splitext(file)
        name = fullname[0]
        ext = fullname[1]
        if (name == filename) or (ext == filename):
            socket.send_multipart(['yes'.encode(), server.socket.encode()])
            inserver = True
    if inserver == False:
        socket.send_multipart(['no'.encode(), server.successor.encode()])

def save_part(id_string, bytes):
    with open(spath + id_string, 'wb') as f:
        f.write(bytes)
    print('file saved successfully')
    socket.send_string('Uploading...')

def upload(id):
    print('Checking if filepart belongs to this server...')
    if server.first:
        if (id > server.responsibility[0][0] and id <= server.responsibility[0][1])\
            or (id >= server.responsibility[1][0] and id <= server.responsibility[1][1]):
            socket.send_multipart(['yes'.encode(), server.socket.encode()])
        else:
            socket.send_multipart(['no'.encode(), server.successor.encode()])
    else:
        if (id > server.responsibility[0][0] and id <= server.responsibility[0][1]):
            socket.send_multipart(['yes'.encode(), server.socket.encode()])
        else:
            socket.send_multipart(['no'.encode(), server.successor.encode()])

def receive_old_files(filename, content):
    with open(spath + filename, 'wb') as f:
        f.write(content)
    print('file successfully written!')
    socket.send_string('Saved!!')

def add_server(id, predecessor, type):
    print(f'Adding server {id} to network...')
    if type == 'last':
        responsibility = [[server.responsibility[0][0], id]]
        jresponsibility = json.dumps(responsibility)
        socket.send_multipart([jresponsibility.encode(), server.predecessor.encode()])
        server.responsibility = [[id, (2**160) - 1], [0, server.id]]
        server.predecessor = predecessor
        delegate_files(predecessor, responsibility)
    elif type == 'first':
        responsibility = [[server.responsibility[0][0], server.responsibility[0][1]],[0, id]]
        jresponsibility = json.dumps(responsibility)
        socket.send_multipart([jresponsibility.encode(), server.predecessor.encode()])
        server.responsibility = [[id, server.id]]
        server.first = False
        server.predecessor = predecessor
        delegate_files(predecessor, responsibility)
    elif type == 'normal':
        responsibility = [[server.responsibility[0][0], id]]
        jresponsibility = json.dumps(responsibility)
        socket.send_multipart([jresponsibility.encode(), server.predecessor.encode()])
        server.responsibility = [[id, server.id]]
        server.predecessor = predecessor
        delegate_files(predecessor, responsibility)


def connect_request(id):
    print('Checking if server belongs to interval')
    if(server.first):
        if(id > server.responsibility[0][0] and id <= server.responsibility[0][1]):
            socket.send_multipart(['yes'.encode(), 'last'.encode(), server.socket.encode()])
        elif(id >= server.responsibility[1][0] and id < server.responsibility[1][1]):
            socket.send_multipart(['yes'.encode(), 'first'.encode(), server.socket.encode()])
        else:
            socket.send_multipart(['no'.encode(), server.successor.encode()])
    else:
        if (id > server.responsibility[0][0] and id < server.responsibility[0][1]):
            socket.send_multipart(['yes'.encode(), 'normal'.encode(), server.socket.encode()])
        else:
            socket.send_multipart(['no'.encode(), server.successor.encode()])

# --------------------------------------------------------------------------------------------------------
# FUNCTIONS FOR REQUEST

def delegate_files(predecessor, responsibility):
    print('Delegating old files to new server...\n')
    s = context.socket(zmq.REQ)
    s.connect(f'tcp://{predecessor}')
    files = os.listdir(spath)
    for file in files:
        full = os.path.splitext(file)
        name = full[0]
        if len(responsibility) > 1 :
            if (int(name) > responsibility[0][0] and int(name) <= responsibility[0][1]) or (int(name) > responsibility[1][0] and int(name) <= responsibility[1][1]):
                with open(spath + file, 'rb') as f:
                    content = f.read()
                s.send_multipart(['delegate files'.encode(), file.encode(), content])
                resp = s.recv_string()
                print(resp)
                print('file successfully delegated!')
        else:
            if (int(name) > responsibility[0][0] and int(name) <= responsibility[0][1]):
                with open(spath + file, 'rb') as f:
                    content = f.read()
                s.send_multipart(['delegate files'.encode(), file.encode(), content])
                resp = s.recv_string()
                print(resp)
                print('file successfully delegated!')
    print('All files were delegated to predecessor...\n')



def get_resp_range(id, type, socket):
    s = context.socket(zmq.REQ)
    s.connect(f'tcp://{socket}')
    s.send_multipart(['add'.encode(), str(id).encode(), server.socket.encode(), type.encode()])
    resp = s.recv_multipart()
    server.responsibility = json.loads(resp[0].decode())
    server.predecessor = resp[1].decode()
    server.successor = socket
    notify_predecessor(server.predecessor)
    print('Properties correctly set\n')

def may_connect(socket):
    s = context.socket(zmq.REQ)
    s.connect(f'tcp://{socket}')
    s.send_multipart(['connect'.encode(), str(server.id).encode()])
    resp = s.recv_multipart()
    return resp

def notify_predecessor(socket):
    s = context.socket(zmq.REQ)
    s.connect(f'tcp://{socket}')
    s.send_multipart(['new successor'.encode(), server.socket.encode()])
    print('notifying predecessor...\n')
# --------------------------------------------------------------------------------------------------------
# SMALL FUNCTIONS

def update_successor(new_successor):
    server.successor = new_successor
    socket.send_string('Successor successfully changed')
    print(f'Successor has changed to {new_successor}')

def printResRange():
    for t in server.responsibility:
        print(t)

def generateId(socket):
    return int(hashString(randomString(socket)), 16)

def randomString(s, n=30):
    chars = string.ascii_uppercase + string.ascii_lowercase + '0123456789'
    return s + ''.join(choice(chars) for i in range(n))

def hashString(s):
    sha1 = hashlib.sha1()
    sha1.update(s.encode('utf-8'))
    return sha1.hexdigest()
# --------------------------------------------------------------------------------------------------------
# START MAIN

def main():
    initServer()
    serverUp()


if __name__ == '__main__':
    main()
