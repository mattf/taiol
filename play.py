#!/bin/python

import json
import sys
import time

from proton import *

datafile = sys.argv[1]
address = sys.argv[2]

mng = Messenger()
mng.start()

lc = 0
ec = 0
while True:
    print "loop count:", lc, "event count:", ec
    for line in open(datafile):
        event = json.loads(line)
        msg = Message()
        msg.address = address
        msg.properties = event
        mng.put(msg)
        mng.send()
        time.sleep(0.01)
        ec += 1
    lc += 1

mng.stop()
