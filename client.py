import zmq
import json
import os
import math
import hashlib

context = zmq.Context()
socket = context.socket(zmq.REQ)

partsize = 1024 * 1024 * 2
cpath = 'client/'
dpath = 'downloads/'

#------------------------------------------------------------------------------------
# MAIN FUNCTIONS

def upload():
    while True:
        filename = str(input('Insert name of the file to upload: '))
        files = os.listdir(cpath)
        if filename in files:
            break
        else:
            print('The specified file does not exist...\n')
    parts = hashparts(filename)
    splitname = os.path.splitext(filename)
    name = splitname[0]
    extension = splitname[1]
    j_object = {}
    j_object['parts'] = parts
    j_object['full name'] = filename
    chord_name = cpath + name + 'chord.json'
    with open(chord_name, 'w') as chord:
        json.dump(j_object, chord, indent=4)
    print('Chord file has been created!\n')
    with open(chord_name, 'rb') as chord:
        bytes = chord.read()
        chord_id = hash(bytes)
    upload_parts(filename, j_object['parts'])
    upload_chord(chord_id, chord_name, name)


def download():
    filename = str(input('Insert name of the file to download: '))
    splitname = os.path.splitext(filename)
    name = splitname[0]
    chord_name = f'.{name}chord'
    socket.send_multipart(['ask download'.encode(), chord_name.encode()])
    resp = socket.recv_multipart()
    if resp[0].decode() == 'yes':
        download_chord(resp[1].decode(), chord_name)
    elif resp[0].decode() == 'no':
        resp = ask_download(resp[1].decode(), chord_name)
        while True:
            if resp[0].decode() == 'yes':
                download_chord(resp[1].decode(), chord_name)
                break
            elif resp[0].decode() == 'no':
                resp = ask_download(resp[1].decode(), chord_name)
    print('Chord was downloaded!\n')
    with open(dpath + f'{chord_name}.json', 'r') as chord:
        chordjson = json.load(chord)
    # Now we can start downloading parts
    with open(dpath + chordjson['full name'], 'ab') as final:
        for part in chordjson['parts']:
            socket.send_multipart(['ask download'.encode(), str(part).encode()])
            resp = socket.recv_multipart()
            if resp[0].decode() == 'yes':
                resp = download_part(resp[1].decode(), str(part))
                print(resp[0].decode())
                final.write(resp[1])
            elif resp[0].decode() == 'no':
                resp = ask_download(resp[1].decode(), str(part))
                while True:
                    if resp[0].decode() == 'yes':
                        resp = download_part(resp[1].decode(), str(part))
                        print(resp[0].decode())
                        final.write(resp[1])
                        break
                    elif resp[0].decode() == 'no':
                        resp = ask_download(resp[1].decode(), chord_name)
            print('part appened successfully!')



def connect():
    ip = str(input('Please insert an ip address from network: '))
    port = str(input('Now insert port number: '))
    socket.connect(f'tcp://{ip}:{port}')

def workflow():
    while True:
        print('--------------------------------------------')
        print('- upload\n- download\n- exit\n')
        cmd = input('Please select one of the above options: ')
        if cmd == 'upload':
            upload()
        elif cmd == 'download':
            download()
        elif cmd == 'exit':
            break

#------------------------------------------------------------------------------------
# REQUEST FUNCTIONS

def download_part(socket, id_string):
    s = context.socket(zmq.REQ)
    s.connect(f'tcp://{socket}')
    s.send_multipart(['download part'.encode(), id_string.encode()])
    resp = s.recv_multipart()
    return resp

def ask_download(socket, chord_name):
    s = context.socket(zmq.REQ)
    s.connect(f'tcp://{socket}')
    s.send_multipart(['ask download'.encode(), chord_name.encode()])
    resp = s.recv_multipart()
    return resp

def download_chord(socket, chord_name):
    s = context.socket(zmq.REQ)
    s.connect(f'tcp://{socket}')
    s.send_multipart(['download'.encode(), chord_name.encode()])
    resp = s.recv_multipart()
    with open(dpath + f'{chord_name}.json', 'wb') as chord:
        chord.write(resp[1])
        print(resp[0].decode())


def upload_chord(chord_id, chord_name, name):
    with open(chord_name, 'rb') as chord:
        bytes = chord.read()
    socket.send_multipart(['upload'.encode(), str(chord_id).encode()])
    resp = socket.recv_multipart()
    if resp[0].decode() == 'yes':
        socket.send_multipart(['send'.encode(), f'{chord_id}.{name}chord'.encode(), bytes])
        resp = socket.recv_string()
        print(resp)
    elif resp[0].decode() == 'no':
        resp = ask_upload(resp[1].decode(), str(chord_id))
        while True:
            if resp[0].decode() == 'yes':
                sendpart(resp[1].decode(), f'{chord_id}.{name}chord', bytes)
                break
            elif resp[0].decode() == 'no':
                resp = ask_upload(resp[1].decode(), str(chord_id))
    print('Chord was uploaded!\n')


def upload_parts(filename, parts):
    print('uploading file parts...\n')
    with open(cpath + filename, 'rb') as f:
        i = 0
        while True:
            bytes = f.read(partsize)
            if not bytes:
                print('File uploaded successfully\n')
                break
            socket.send_multipart(['upload'.encode(), str(parts[i]).encode()])
            resp = socket.recv_multipart()
            if resp[0].decode() == 'yes':
                socket.send_multipart(['send'.encode(), str(parts[i]).encode(), bytes])
                resp = socket.recv_string()
                print(resp)
            elif resp[0].decode() == 'no':
                resp = ask_upload(resp[1].decode(), str(parts[i]))
                while True:
                    if resp[0].decode() == 'yes':
                        sendpart(resp[1].decode(), str(parts[i]), bytes)
                        break
                    elif resp[0].decode() == 'no':
                        resp = ask_upload(resp[1].decode(), str(parts[i]))
            i += 1

def ask_upload(socket, part_id):
    s = context.socket(zmq.REQ)
    s.connect(f'tcp://{socket}')
    s.send_multipart(['upload'.encode(), part_id.encode()])
    resp = s.recv_multipart()
    return resp

def sendpart(socket1, part_id, bytes):
    s = context.socket(zmq.REQ)
    s.connect(f'tcp://{socket1}')
    s.send_multipart(['send'.encode(), part_id.encode(), bytes])
    resp = s.recv_string()
    print(resp)

#------------------------------------------------------------------------------------
# SMALL FUNCTIONS

def hash(bytes):
    hasher = hashlib.sha1()
    hasher.update(bytes)
    return int(hasher.hexdigest(), 16)

def hashparts(filename):
    parts = []
    with open(cpath + filename, 'rb') as f:
        while True:
            part = f.read(partsize)
            if not part:
                print('parts hashed successfully')
                break
            parts.append(hash(part))
    return parts
#------------------------------------------------------------------------------------
# START MAIN

def main():
    connect()
    workflow()

if __name__ == '__main__':
    main()
