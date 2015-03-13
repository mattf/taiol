#!/bin/python

import json
import sys
import time

from proton import *

datafile = sys.argv[1]
address = sys.argv[2]

mng = Messenger()
mng.start()

first = last = None
delta = 0
lc = 0
ec = 0
while True:
    print "loop count:", lc, "event count:", ec
    for line in open(datafile):
        event = json.loads(line)
        if not first:
            first = event['time']
        last = event['time']
        msg = Message()
        msg.address = address
        event['time'] = int(event['time']) + delta
        msg.properties = event
#        print time.ctime(event['time'] / 1000), event
        mng.put(msg)
        mng.send()
        time.sleep(0.01)
        ec += 1
    delta += 10000 + (last - first)
    lc += 1

mng.stop()
