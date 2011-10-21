#!/bin/bash

U_BASE=/user/yasemin/delis/updates
echo replace ts layer with new ids

INFILE=$1
OUTFILE=$INFILE-updated
MAPPING=$2

echo $OUTFILE

/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rmr $OUTFILE
/home/yavcular/hadoop-0.21.0/bin/hadoop jar /home/yavcular/hadoop-0.21.0/g.jar nyu/cs/webgraph/updates/SourceOnlyReplace $INFILE  $MAPPING false $OUTFILE $U_BASE/numericsample


