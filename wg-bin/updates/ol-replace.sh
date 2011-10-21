#!/bin/bash

U_BASE=/user/yasemin/delis/updates
echo replace ts layer with new ids

INFILE=$1
OUTFILETMP=$INFILE-tmp
OUTFILE=$INFILE-updated
MAPPING=$2

/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rmr $OUTFILETMP
/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rmr $OUTFILE
/home/yavcular/hadoop-0.21.0/bin/hadoop jar /home/yavcular/hadoop-0.21.0/g.jar nyu/cs/webgraph/updates/IntKeyReplace $INFILE 0 $MAPPING false $OUTFILETMP $OUTFILE $U_BASE/numericsample


