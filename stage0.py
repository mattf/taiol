#!/usr/share/spark/bin/pyspark

import optparse
from sys import argv
from time import ctime, time

from collections import deque, namedtuple, defaultdict

import numpy

from null import Null

from proton import *

from pyspark.context import SparkContext
from pyspark.storagelevel import StorageLevel
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext


parser = optparse.OptionParser(
  "usage: %prog [options]",
  description="Process a stream of events and emit results to stdout or an AMQP 1.0 address")
parser.add_option("-a", "--address", default=None,
                  help="AMQP 1.0 address, e.g. amqp://0.0.0.0/name")
parser.add_option("-r", "--remote", default=None,
                  help="Read data from a remote endpoint, e.g. localhost:1984")
opts, args = parser.parse_args()

if not opts.remote:
  parser.error("required --remote missing")
  exit(1)

messenger = opts.address and Messenger() or Null()
messenger.start()

message = Message()

sc = SparkContext("local[4]", appName="stage0")

sqlCtx = SQLContext(sc)

def calc_dist(event):
  if event.rssi == 0:
    return -1.0
  ratio = event.rssi*1.0/event.calibratedPower
  if ratio < 1.0:
    return pow(ratio, 10)
  else:
    return 0.89976*pow(ratio, 7.7095) + 0.111


class Beacon:
  def __init__(self,
               location = 'Unknown',
               last_location = 'Unknown',
               missed = 0,
               present = False,
               changed = True,
               retransmit_countdown = 10):
    self.location = location
    self.missed = missed
    self.present = present
    self.changed = changed
    self.retransmit_countdown = retransmit_countdown

beacons = defaultdict(lambda: Beacon())

samples = deque(maxlen=25)

BeaconScanner = namedtuple('BeaconScanner', ['beacon', 'scanner'])
DistanceScanner = namedtuple('DistanceScanner', ['distance', 'scanner'])

def _emit(type, beacon, location, retransmit=False):
  event = {"type": type,
           "user_id": beacon,
           "location_id": location}
  if retransmit:
    event["retransmit"] = True
  print event['user_id'], event['type'], event['location_id'], retransmit and "retransmit" or ""
  message.address = opts.address
  message.properties = event
  messenger.put(message)
  messenger.send()

def emit_enter(beacon, state, retransmit=False):
  _emit('check-in', beacon, state.location, retransmit)

def emit_exit(beacon, state, retransmit=False):
  _emit('check-out', beacon, state.last_location, retransmit)

def process(rdd):
  mark0 = time()

  for state in beacons.values():
    state.changed = False # don't repeat check-in events for beacon that is missing
    state.present = False
    state.retransmit_countdown -= 1

  # filter: focus only on SCANNER_READ events, messageType=0
  # map:    format: ((beacon, scanner), distance)
  # group:  aggregate distance by (beacon, scanner)
  # map:    format: (beacon, (median distance, scanner))
  # reduce: select min distance by beacon
  for (beacon, (distance, scanner)) in \
      sqlCtx.jsonRDD(rdd) \
            .filter(lambda e: e.messageType == 0) \
            .map(lambda e: (BeaconScanner(e.minor, e.scannerID), calc_dist(e))) \
            .groupByKey() \
            .map(lambda (bs, ls):
                 (bs.beacon,
                  DistanceScanner(float(numpy.median(list(ls))),
                                  bs.scanner))) \
            .reduceByKey(min) \
            .collect():
    state = beacons[beacon]
    state.changed = state.location != scanner
    if state.changed:
      state.last_location = state.location
      state.location = scanner
    state.missed = 0
    state.present = True

  delete = []
  for beacon, state in beacons.iteritems():
    if not state.present:
      state.missed += 1
      if state.missed >= 5: # can miss 5 windows
        delete.append(beacon)
  for beacon in delete:
    # todo: emit something when a beacon goes missing, likely exit of last non-x location
    del beacons[beacon]

  for beacon, state in beacons.iteritems():
    if state.changed or state.retransmit_countdown == 0:
      if state.location[-1] == 'x':
        emit_exit(beacon, state, state.retransmit_countdown == 0)
      else:
        if (state.last_location[-1] != 'x' and
            state.retransmit_countdown > 0):
          emit_exit(beacon, state)
        emit_enter(beacon, state, state.retransmit_countdown == 0)
      state.retransmit_countdown = 10 # resend location at least every 10 windows

  mark1 = time()

  samples.append(mark1-mark0)
  print "perf:", mark1-mark0, numpy.mean(samples), numpy.var(samples)

def protect(func):
  def _protect(rdd):
    if rdd.take(1):
      func(rdd)
  return _protect

host, port = opts.remote.split(':')
ssc = StreamingContext(sc, 5)
data = ssc.socketTextStream(host, int(port), StorageLevel.MEMORY_ONLY)
data.foreachRDD(protect(process))
ssc.start()
ssc.awaitTermination()

messenger.stop()
