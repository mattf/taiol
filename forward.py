#!/bin/python

import json
import optparse
import socket

from proton import *

parser = optparse.OptionParser(
    "usage: %prog <address>",
    description="Forward AMQP 1.0 message properties to a listener as JSON")
parser.add_option("-p", "--port", default=1984,
                  help="Port to listen on")
opts, args = parser.parse_args()

mng = Messenger()
mng.incoming_window = 2048
mng.start()

for a in args:
    print 'subscribing to:', a
    mng.subscribe(a)

msg = Message()
sock = socket.socket()
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.bind(('0.0.0.0', int(opts.port)))
print 'listening at:', sock.getsockname()
sock.listen(1)
while True:
    (client, client_addr) = sock.accept()
    print 'connection from:', client_addr
    try:
        while True:
            mng.recv()
            while mng.incoming:
                mng.get(msg)
                client.sendall(json.dumps(msg.properties) + "\n")
    except Exception, e:
        client.close()
        print e
