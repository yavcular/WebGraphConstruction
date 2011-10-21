#!/bin/bash
FSPATH=/user/yasemin/archive
IN=$1
OUT=$IN-repartitioned

/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rmr $OUT
/home/yavcular/hadoop-0.21.0/bin/hadoop jar /home/yavcular/hadoop-0.21.0/g.jar nyu/cs/webgraph/archiveUK/RepartitionLayers $IN $OUT $2


