#!/bin/bash
BASE=/user/yasemin/delis
IN_BASE=$BASE/RAW-INPUT
/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rmr $IN_BASE/slinkgraph-id-sample


#ALL
/home/yavcular/hadoop-0.21.0/bin/hadoop jar /home/yavcular/hadoop-0.21.0/g.jar nyu/cs/webgraph/DELIS/GenerateSuperlinks $IN_BASE/ol $IN_BASE/bv $IN_BASE/slinkgraph-id-sample

