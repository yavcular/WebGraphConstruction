#!/bin/bash

# returns sorted unique urls

BASE=/user/yasemin/delis
/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rmr $BASE/map/

echo assignids

#Default file paths are in use\nUsage: <uurls_path> <urlidmapping_path> <fileoffset_path> <partitionerSapmle_path> ")

/home/yavcular/hadoop-0.21.0/bin/hadoop jar /home/yavcular/hadoop-0.21.0/g.jar nyu/cs/webgraph/main/AssignIdsMapRed $BASE/uurls  $BASE/map $BASE/sample

