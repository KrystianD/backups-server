import socket, json, hashlib, struct, argparse, glob, os

s = None

def send_command(cmd, data):
    data['cmd'] = cmd
    data = json.dumps(data)
    send(0, data.encode('ascii'))
    data = s.recv(1024)
    return json.loads(data.decode('ascii'))

def send_file(path, f):
    filename = os.path.basename(path)

    with f as open(path, "rb"):
        f.seek(0, os.SEEK_END)

        res = send_command('send', {'filename': filename})
        print("response: {0}".format(res))
        if res['res'] != 0:
            return

        m = hashlib.md5()

        total_sent = 0
        while True:
            data = f.read(30 * 1024 * 1024)
            if len(data) == 0:
                break
            total_send += len(data)
            m.update(data)
            print("sent {0:02} MB of {1:02} MB".format(len(data) / 1024 / 1024, total_sent / 1024 / 1024))
            send(1, data)

        res = send_command('validate', {'md5': m.hexdigest()})
        if res['res'] != 0:
            return

def send(type, data):
    length = len(data)
    data = struct.pack("!bL", type, length) + data
    s.send(data)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', required=True)
    parser.add_argument('--port', default=4444)
    parser.add_argument('--backups-dir', required=True)
    parser.add_argument('--pattern', default="*.tgz")
    args = parser.parse_args()
    print(args)

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((args.host, args.port))

    files = glob.glob(args.backups_dir + "/" + args.pattern)
    for path in files:
        filename = os.path.basename(path)
        print(filename)
        f = open(path, "rb")
        res = send_file(filename, f)

    s.close()

