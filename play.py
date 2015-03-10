#!/bin/python

import json

from proton import *

mng = Messenger()
mng.start()

for line in open('data.json'):
    event = json.loads(line)
    msg = Message()
    msg.address = 'amqp://1.2.3.4/donotuse'
    msg.properties = event
    mng.put(msg)
    mng.send()
    print event

mng.stop()
