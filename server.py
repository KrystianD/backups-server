import socketserver, json, glob, struct, os, sys, hashlib, argparse
import dataset

backups_dir = None

def get_backup_path(filename):
    global backups_dir
    path = backups_dir + "/" + filename
    print(path)
    path = os.path.realpath(path)
    print(path)
    if not path.startswith(backups_dir):
        raise Exception("invalid path")
    return path

def has_backup(filename):
    path = get_backup_path(filename)
    if os.path.exists(path):
        print("path exists")
        return True
    db = dataset.connect("sqlite:///db.sqlite")
    table = db['backups']
    row = table.find_one(filename=filename)
    print(row)
    return row is not None

def save_backup(filename):
    db = dataset.connect("sqlite:///db.sqlite")
    table = db['backups']
    table.insert(dict(filename=filename))

class MyTCPHandler(socketserver.BaseRequestHandler):
    allow_reuse_address = True

    file_handle = None
    checksum = None
    filename = None
    file_path = None
    file_path_tmp = None

    def handle(self):
        # self.request is the TCP socket connected to the client
        while True:

            data = self.request.recv(5)
            if len(data) != 5:
                print("end")
                return
            
            (type, length) = struct.unpack("!bL", data)
            print(type, length)


            if type == 0:
                data = b''
                while len(data) < length:
                    left = length - len(data)
                    part = self.request.recv(left)
                    if len(part) == 0:
                        print("end")
                        return
                    data += part
                    print(len(part))

                data = json.loads(data.decode('ascii'))
                print(data)

                backups = glob.glob(backups_dir + "/*.tgz")

                if data['cmd'] == 'need_send':
                    filename = data['filename']
                    print(filename, backups)
                    self.send_res(1)
                if data['cmd'] == 'send':
                    filename = data['filename']
                    print('send')

                    if has_backup(filename):
                        print("have")
                        self.send_res(1)
                        continue

                    path = get_backup_path(filename)
                    self.filename = filename
                    self.file_path = path
                    self.file_path_tmp = path + ".tmp"
                    self.file_handle = open(self.file_path_tmp, "wb")
                    self.checksum = hashlib.md5()

                    self.send_res(0)
                if data['cmd'] == 'validate':
                    if self.file_handle:
                        checksum = data['md5']

                        if self.checksum.hexdigest() == checksum:
                            save_backup(self.filename)
                            os.rename(self.file_path_tmp, self.file_path)

                        self.file_handle = None
                        self.send_res(0)

            elif type == 1:

                received = 0
                while received < length:
                    left = length - received
                    part = self.request.recv(left)
                    if len(part) == 0:
                        print("end")
                        return
                    received += len(part)
                    if self.file_handle:
                        self.file_handle.write(part)
                        self.checksum.update(part)
                print(received, self.checksum.hexdigest())

    def send_res(self, res):
        resp = {'res': res}
        resp = json.dumps(resp)
        self.request.sendall(resp.encode('ascii'))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='0.0.0.0')
    parser.add_argument('--port', default=4444)
    parser.add_argument('--backups-dir', required=True)
    args = parser.parse_args()
    print(args)

    backups_dir = os.path.realpath(args.backups_dir)

    socketserver.TCPServer.allow_reuse_address = True
    server = socketserver.TCPServer((args.host, args.port), MyTCPHandler)
    server.serve_forever()
