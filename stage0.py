#!/usr/share/spark/bin/pyspark

import optparse
from sys import argv
from time import ctime

from null import Null

from proton import *

from pyspark.context import SparkContext
from pyspark.sql import SQLContext


parser = optparse.OptionParser(
  "usage: %prog [options] [datafile]",
  description="Process a datafile and emit results to stdout or an AMQP 1.0 address")
parser.add_option("-a", "--address", default=None,
                  help="AMQP 1.0 address, e.g. amqp://0.0.0.0/name")
opts, args = parser.parse_args()

datafile = "/data.json"
if args:
  datafile = args.pop(0)

messenger = opts.address and Messenger() or Null()
messenger.start()

sc = SparkContext(appName="stage0")

sqlCtx = SQLContext(sc)

data = sqlCtx.jsonFile(datafile)

def calc_dist(event):
  if event.rssi == 0:
    return -1.0
  ratio = event.rssi*1.0/event.calibratedPower
  if ratio < 1.0:
    return pow(ratio, 10)
  else:
    return 0.89976*pow(ratio, 7.7095) + 0.111

BUCKET_WIDTH_SEC = 5
def ms_to_bucket(ms):
  return ms / (BUCKET_WIDTH_SEC * 1000)
def bucket_to_s(bucket):
  return bucket * BUCKET_WIDTH_SEC

# filter: focus only on SCANNER_READ events, messageType=0
# map:    put events in buckets of 5s width, extracting identity, distance and location
# reduce: find smallest distance, location pair per bucket, identity
# map:    restructure w/ bucket as key, for grouping
# group:  group by buckets, result is: bucket + closest event per identify
# sort:   so we can process buckets in temporal order
d = data.filter(lambda e: e.messageType == 0) \
        .map(lambda e: ((ms_to_bucket(e.time), (e.major, e.minor)), (calc_dist(e), e.scannerID))) \
        .reduceByKey(min) \
        .map(lambda e: (e[0][0], (e[0][1], e[1][0], e[1][1]))) \
        .groupByKey() \
        .sortByKey()
message = Message()
UNKNOWN = ('Unknown', 0)
locations = {}
last_bucket = 0
for moment in d.collect():
  (bucket, events) = moment
  if last_bucket + 1 != bucket:
    print 'missing bucket detected'
    for who in locations.keys():
      locations[who] = UNKNOWN
  changed = []
  present = []
  for event in list(events):
    (who, distance, room) = event
    present.append(who)
    if who not in locations:
      locations[who] = UNKNOWN
    if locations[who][0] != room:
      changed.append(who)
    locations[who] = (room, distance)
  for who in locations.keys():
    if who not in present and locations[who] != UNKNOWN:
      locations[who] = UNKNOWN
      changed.append(who)
  for who in changed:
    event = {"user_id": who,
             "timestamp": bucket_to_s(bucket),
             "location_id": locations[who][0],
             "timestamp_s": ctime(bucket_to_s(bucket)),
             "location_distance": locations[who][1]}
    print event
    message.address = opts.address
    message.properties = event
    messenger.put(message)
    messenger.send()

  last_bucket = bucket

messenger.stop()

# TODO
#  . reduce jitter
