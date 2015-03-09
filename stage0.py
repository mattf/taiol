#!/usr/share/spark/bin/pyspark

from pyspark.context import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext(appName="filter")

sqlCtx = SQLContext(sc)

data = sqlCtx.jsonFile("/data.json")

def calc_dist(event):
  if event.rssi == 0:
    return -1.0
  ratio = event.rssi*1.0/event.calibratedPower
  if ratio < 1.0:
    return pow(ratio, 10)
  else:
    return 0.89976*pow(ratio, 7.7095) + 0.111

# map:    put events in buckets of 5s width, extracting identity, distance and location
# reduce: find smallest distance, location pair per bucket, identity
# map:    restructure w/ bucket as key, for grouping
# group:  group by buckets, result is: bucket + closest event per identify
# sort:   so we can process buckets in temporal order
d = data.map(lambda e: ((e.time/5000, e.minor), (calc_dist(e), e.scannerID))) \
        .reduceByKey(min) \
        .map(lambda e: (e[0][0], (e[0][1], e[1][0], e[1][1]))) \
        .groupByKey() \
        .sortByKey()
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
    print bucket, who, locations[who][0], locations[who][1]
  last_bucket = bucket

# TODO
#  . reduce jitter
