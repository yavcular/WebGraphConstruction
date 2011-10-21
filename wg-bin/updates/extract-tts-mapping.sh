#!/bin/bash

U_BASE=/user/yasemin/delis/updates
echo extract ttables and mapping

/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rmr $U_BASE/tts-and-map
/home/yavcular/hadoop-0.21.0/bin/hadoop jar /home/yavcular/hadoop-0.21.0/g.jar nyu/cs/webgraph/updates/TTables $U_BASE/merged-map $U_BASE/tts-and-map $U_BASE/numericsample $U_BASE/merged-uurls-offsets

/home/yavcular/hadoop-0.21.0/bin/hadoop fs -mkdir $U_BASE/tts-and-map/map
/home/yavcular/hadoop-0.21.0/bin/hadoop fs -mkdir $U_BASE/tts-and-map/tt-new
/home/yavcular/hadoop-0.21.0/bin/hadoop fs -mkdir $U_BASE/tts-and-map/tt-current

/home/yavcular/hadoop-0.21.0/bin/hadoop fs -mv $U_BASE/tts-and-map/newmap* $U_BASE/tts-and-map/map/
/home/yavcular/hadoop-0.21.0/bin/hadoop fs -mv $U_BASE/tts-and-map/ttnew* $U_BASE/tts-and-map/tt-new/
/home/yavcular/hadoop-0.21.0/bin/hadoop fs -mv $U_BASE/tts-and-map/ttcurrent* $U_BASE/tts-and-map/tt-current/

/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rm $U_BASE/tts-and-map/part*



