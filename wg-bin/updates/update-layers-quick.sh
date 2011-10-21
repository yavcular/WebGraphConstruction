#!/bin/bash

U_BASE=/user/yasemin/delis/updates
echo replace ts layer with new ids

LAYERS=$1
MAPPING=$2


INFILE=$LAYERS/ol
OUTFILETMP=$INFILE-tmp
OUTFILE=$INFILE-updated

/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rmr $OUTFILETMP
/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rmr $OUTFILE
/home/yavcular/hadoop-0.21.0/bin/hadoop jar /home/yavcular/hadoop-0.21.0/g.jar nyu/cs/webgraph/updates/IntKeyReplace $INFILE 0 $MAPPING false $OUTFILETMP $OUTFILE $U_BASE/numericsample

INFILE=$LAYERS/bv
OUTFILE=$INFILE-updated
/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rmr $OUTFILE
/home/yavcular/hadoop-0.21.0/bin/hadoop jar /home/yavcular/hadoop-0.21.0/g.jar nyu/cs/webgraph/updates/SourceOnlyReplace $INFILE  $MAPPING false $OUTFILE $U_BASE/numericsample

INFILE=$LAYERS/ts
OUTFILE=$INFILE-updated
/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rmr $OUTFILE
/home/yavcular/hadoop-0.21.0/bin/hadoop jar /home/yavcular/hadoop-0.21.0/g.jar nyu/cs/webgraph/updates/SourceOnlyReplace $INFILE  $MAPPING false $OUTFILE $U_BASE/numericsample





