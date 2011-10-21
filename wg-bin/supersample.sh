#!/bin/bash
BASE=/user/yasemin/delis
INPUT=$1
CAPTURES=$2

/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rmr $BASE/supersample

echo supersample

# arguments
/home/yavcular/hadoop-0.21.0/bin/hadoop jar /home/yavcular/hadoop-0.21.0/g.jar nyu/cs/webgraph/main/GenerateSuperSample -inpath $INPUT -outpath $BASE/supersample -numrecords $2 -keyonly true


