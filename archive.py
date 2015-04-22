#!/bin/python

import sys
import json
import argparse

from proton import *

parser = argparse.ArgumentParser(
    usage="%(prog)s <address> [<file>]",
    description="Archive message properties from <address> to <file> as a JSON object per line")
parser.add_argument("address", default=None,
                    help="Source of messages, e.g. amqp://0.0.0.0/name")
parser.add_argument("file", nargs="?",
                    type=argparse.FileType('a'),
                    default=sys.stdout,
                    help="File to append message properties to, default: stdout")

args = parser.parse_args()

mng = Messenger()
mng.incoming_window = 2048
mng.start()

mng.subscribe(args.address)

msg = Message()
try:
    while True:
        mng.recv()
        while mng.incoming:
            mng.get(msg)
            args.file.write(json.dumps(msg.properties) + '\n')
except Exception, e:
    print e
