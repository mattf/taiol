#!/bin/sh

SOURCE=$1
DEST=$2

/forward.py $SOURCE & spark-submit /stage0.py -r localhost:1984 -a $DEST &

wait
