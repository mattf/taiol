#!/usr/share/spark/bin/pyspark

import optparse
from sys import argv
from time import ctime, time

from collections import deque, namedtuple

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

UNKNOWN = ('Unknown', 0)

locations = {}
missing = {}
retransmit = {}
samples = deque(maxlen=25)

BeaconScanner = namedtuple('BeaconScanner', ['beacon', 'scanner'])
DistanceScanner = namedtuple('DistanceScanner', ['distance', 'scanner'])

def process(rdd):
  global locations

  mark0 = time()

  message = Message()
  changed = []
  present = []

  # filter: focus only on SCANNER_READ events, messageType=0
  # map:    format: ((beacon, scanner), distance)
  # group:  aggregate distance by (beacon, scanner)
  # map:    format: (beacon, (median distance, scanner))
  # reduce: select min distance by beacon
  for event in \
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
    (who, (distance, room)) = event
    present.append(who)
    missing[who] = 5 # can miss 5 windows
    if who not in locations:
      locations[who] = UNKNOWN
    if locations[who][0] != room:
      changed.append(who)
    locations[who] = (room, distance)
  for who in locations.keys():
    if who not in present and locations[who] != UNKNOWN:
      missing[who] = missing[who] - 1
      if not missing[who]:
        locations[who] = UNKNOWN
        changed.append(who)
  for who in retransmit.keys():
    retransmit[who] = retransmit[who] - 1
    if not retransmit[who] and who not in changed:
      changed.append(who)
  for who in changed:
    event = {"user_id": who,
             "location_id": locations[who][0],
             "location_distance": locations[who][1]}
    print event['user_id'], event['location_id'], event['location_distance']
    message.address = opts.address
    message.properties = event
    messenger.put(message)
    messenger.send()
    retransmit[who] = 10 # resend location at least every 10 windows

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
