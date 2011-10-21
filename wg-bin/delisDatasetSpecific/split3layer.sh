#!/bin/bash
BASE=/user/yasemin/delis
/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rmr $BASE/3Mlayers

#ALL
/home/yavcular/hadoop-0.21.0/bin/hadoop jar /home/yavcular/hadoop-0.21.0/g.jar nyu/cs/webgraph/DELIS/Divide3MonthData $1 $BASE/3Mlayers
