#!/bin/bash
BASE=/user/yasemin/delis
INPUT=$1
H_DIR=/home/yavcular/hadoop-0.21.0


/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rmr $BASE/links
/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rmr $BASE/slinkgraph-id

echo replace

#arguments: graphPath | mapPath | outputPath | finaloutputPath | isKeyFirstInMapFile | numOfSpaceSeperaredNoReplaceValues | samplepath
/home/yavcular/hadoop-0.21.0/bin/hadoop jar /home/yavcular/hadoop-0.21.0/g.jar nyu/cs/webgraph/optional/GenericReplaceHashSlinkid $INPUT $BASE/map $BASE/links $BASE/slinkgraph-id false 1 $BASE/sample 

$H_DIR/wg-bin/layers.sh $BASE/slinkgraph-id

