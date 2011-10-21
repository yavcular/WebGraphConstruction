#!/bin/bash

BASE=/user/yasemin/delis
INPUT=$1
NUMNOREP=1
/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rmr $BASE/uurls
/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rmr $BASE/sample

echo get-unique-urls
echo $INPUT
/home/yavcular/hadoop-0.21.0/bin/hadoop jar /home/yavcular/hadoop-0.21.0/g.jar nyu/cs/webgraph/main/GetUniqueUrls $INPUT $BASE/uurls $BASE/supersample $BASE/sample $NUMNOREP
