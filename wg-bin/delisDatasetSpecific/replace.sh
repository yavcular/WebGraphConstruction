#!/bin/bash
BASE=/user/yasemin/delis
IN_BASE=$BASE/RAW-INPUT


#echo -n "do you wanna delete tmplinks and finalSLinks?: "
#read decision

#if [ "$decision" == "y" ]; then

/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rmr $BASE/linkss
/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rmr $BASE/slinkgraph200610
#fi

/home/yavcular/hadoop-0.21.0/bin/hadoop jar /home/yavcular/hadoop-0.21.0/g.jar nyu/cs/webgraph/main/GenericReplace $IN_BASE/slinkgraph-id/200610 $IN_BASE/urlidmap $BASE/linkss $BASE/slinkgraph200610 true 1 /samplepath/ false


/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rmr $BASE/linkss
/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rmr $BASE/slinkgraph200611
/home/yavcular/hadoop-0.21.0/bin/hadoop jar /home/yavcular/hadoop-0.21.0/g.jar nyu/cs/webgraph/main/GenericReplace $IN_BASE/slinkgraph-id/200611 $IN_BASE/urlidmap $BASE/linkss $BASE/slinkgraph200611 true 1 /samplepath/ false

/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rmr $BASE/linkss
/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rmr $BASE/slinkgraph200612
/home/yavcular/hadoop-0.21.0/bin/hadoop jar /home/yavcular/hadoop-0.21.0/g.jar nyu/cs/webgraph/main/GenericReplace $IN_BASE/slinkgraph-id/200612 $IN_BASE/urlidmap $BASE/linkss $BASE/slinkgraph200612 true 1 /samplepath/ false






