#!/bin/sh

SOURCE=$1
DEST=$2

/forward.py $SOURCE & spark-submit /stage0.py -a $DEST &

wait
