import socket, json, hashlib, struct

TCP_IP = '192.168.200.1'
TCP_PORT = 4444
BUFFER_SIZE = 1024

def main():
    global s
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((TCP_IP, TCP_PORT))

    f = open("test1.tgz", "rb")
    res = send_file('test4.tgz', f)

    s.close()

def send_command(cmd, data):
    data['cmd'] = cmd
    data = json.dumps(data)
    send(0, data.encode('ascii'))
    data = s.recv(BUFFER_SIZE)
    return json.loads(data.decode('ascii'))

def send_file(filename, f):

    res = send_command('send', {'filename': filename})
    print(res)
    if res['res'] != 0:
        return

    m = hashlib.md5()

    while True:
        data = f.read(30 * 1024 * 1024)
        if len(data) == 0:
            break
        m.update(data)
        print(len(data), m.hexdigest())
        send(1, data)

    res = send_command('validate', {'md5': m.hexdigest()})
    if res['res'] != 0:
        return

def send(type, data):
    length = len(data)
    data = struct.pack("!bL", type, length) + data
    s.send(data)

main()
