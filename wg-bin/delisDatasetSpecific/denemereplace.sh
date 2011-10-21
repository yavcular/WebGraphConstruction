#!/bin/bash
BASE=/user/yasemin/delis
IN_BASE=$BASE/RAW-INPUT


#echo -n "do you wanna delete tmplinks and finalSLinks?: "
#read decision

#if [ "$decision" == "y" ]; then

/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rmr $BASE/links-deneme
/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rmr $BASE/slinkgraph-str-deneme
#fi

# arguments
# graphPath | mapPath | outputPath
/home/yavcular/hadoop-0.21.0/bin/hadoop jar /home/yavcular/hadoop-0.21.0/g.jar nyu/cs/webgraph/optional/GenericReplaceHashSlinkid $IN_BASE/slinkgraph-id-sample-2006/ $IN_BASE/urlidmap $BASE/links-deneme $BASE/slinkgraph-str-deneme true 1
