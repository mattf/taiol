#!/bin/python

import json
import optparse
import socket

from proton import *

parser = optparse.OptionParser(
    "usage: %prog <address>",
    description="Forward AMQP 1.0 message properties to a listener as JSON")
opts, args = parser.parse_args()

mng = Messenger()
mng.start()

for a in args:
    print 'subscribing to:', a
    mng.subscribe(a)

msg = Message()
sock = socket.socket()
sock.bind(('0.0.0.0', 1984))
print 'listening at:', sock.getsockname()
sock.listen(1)
(client, client_addr) = sock.accept()
print 'connection from:', client_addr
while True:
    mng.recv()
    while mng.incoming:
        try:
            mng.get(msg)
            client.sendall(json.dumps(msg.properties) + "\n")
        except Exception, e:
            print e
