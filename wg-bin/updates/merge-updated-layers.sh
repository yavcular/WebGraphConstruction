#!/bin/bash

U_BASE=/user/yasemin/delis/updates
echo replace ts layer with new ids

H_DIR='/home/yavcular/hadoop-0.21.0'
INFILE_C=$1
INFILE_N=$2
OUTFILE=$3

/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rmr $OUTFILE
/home/yavcular/hadoop-0.21.0/bin/hadoop jar /home/yavcular/hadoop-0.21.0/g.jar nyu/cs/webgraph/updates/MergeLayers $INFILE_C/ol-updated $INFILE_C/ts-updated $INFILE_C/bv-updated $INFILE_N/ol-updated  $INFILE_N/ts-updated  $INFILE_N/bv-updated  $OUTFILE $U_BASE/numericsample

# sample run
#./wg-bin/updates/merge-updated-layers.sh /user/yasemin/delis/layers-f8m /user/yasemin/delis/layers-l4m /user/yasemin/delis/updates/merged-layers

echo  $OUT_FILE/ol

$H_DIR/bin/hadoop fs -rmr $OUTFILE/part*
$H_DIR/bin/hadoop fs -mkdir $OUTFILE/ol
$H_DIR/bin/hadoop fs -mkdir $OUTFILE/ts
$H_DIR/bin/hadoop fs -mkdir $OUTFILE/bv

$H_DIR/bin/hadoop fs -mv  $OUTFILE/out*   $OUTFILE/ol/
$H_DIR/bin/hadoop fs -mv  $OUTFILE/bit*   $OUTFILE/bv/
$H_DIR/bin/hadoop fs -mv  $OUTFILE/tim*   $OUTFILE/ts/

 
