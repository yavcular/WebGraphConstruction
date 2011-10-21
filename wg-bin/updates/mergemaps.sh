#!/bin/bash
BASE=/user/yasemin/delis
U_BASE=$BASE/3M-updates
CAPTURES=$3

/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rmr $U_BASE/supersample

echo supersample

# arguments
/home/yavcular/hadoop-0.21.0/bin/hadoop jar /home/yavcular/hadoop-0.21.0/g.jar nyu/cs/webgraph/main/GenerateSuperSample -inpath $1 -inpath $2 -outpath $U_BASE/supersample -numrecords $CAPTURES -keyonly false

sleep 5

echo merge url-id mapings 
/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rmr $U_BASE/merged-uurls

#arguments: graphPath | mapPath | outputPath | finaloutputPath | isKeyFirstInMapFile | numOfSpaceSeperaredNoReplaceValues | samplepath
/home/yavcular/hadoop-0.21.0/bin/hadoop jar /home/yavcular/hadoop-0.21.0/g.jar nyu/cs/webgraph/updates/MergeUrlIdMaps $1 false $2 false $U_BASE/merged-uurls $U_BASE/supersample $U_BASE/sample 

sleep 5

echo assign-ids for merged maps
/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rmr $U_BASE/merged-map

# uurls_path = args[0], urlidmap_path = args[1], path_sampleforpartitioner = args[2];
/home/yavcular/hadoop-0.21.0/bin/hadoop jar /home/yavcular/hadoop-0.21.0/g.jar nyu/cs/webgraph/updates/AssignIdsMapRedMerged $U_BASE/merged-uurls  $U_BASE/merged-map $U_BASE/sample


sleep 5

echo extract ttables and mapping

/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rmr $U_BASE/tts-and-map
/home/yavcular/hadoop-0.21.0/bin/hadoop jar /home/yavcular/hadoop-0.21.0/g.jar nyu/cs/webgraph/updates/TTables $U_BASE/merged-map $U_BASE/tts-and-map $U_BASE/sample $U_BASE/merged-uurls-offsets

/home/yavcular/hadoop-0.21.0/bin/hadoop fs -mkdir $U_BASE/tts-and-map/tt-current
/home/yavcular/hadoop-0.21.0/bin/hadoop fs -mkdir $U_BASE/tts-and-map/tt-new
/home/yavcular/hadoop-0.21.0/bin/hadoop fs -mkdir $U_BASE/tts-and-map/map

/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rm $U_BASE/tts-and-map/part*

/home/yavcular/hadoop-0.21.0/bin/hadoop fs -mv $U_BASE/tts-and-map/ttnew* $U_BASE/tts-and-map/tt-new/
/home/yavcular/hadoop-0.21.0/bin/hadoop fs -mv $U_BASE/tts-and-map/ttcurrent* $U_BASE/tts-and-map/tt-current/
/home/yavcular/hadoop-0.21.0/bin/hadoop fs -mv $U_BASE/tts-and-map/newmap* $U_BASE/tts-and-map/map/



