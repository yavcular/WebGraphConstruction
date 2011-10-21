#!/bin/bash
# using cach 
# if dont wanna use cach, comment out last part of jar execution
# usage: LinkGraphUrlIdReplacement <in graph path> <in map path> <intermediate output path> <final output path> <use cache (true/false)> <cache path (optional)>

H_DIR=/home/yavcular/hadoop-0.21.0
WG_OUT_DIR=/user/yasemin/delis/

$H_DIR/bin/hadoop fs -rmr $WG_OUT_DIR/layers
$H_DIR/bin/hadoop jar $H_DIR/g.jar nyu/cs/webgraph/main/PrepareFinalLayerFiles $1 $WG_OUT_DIR/layers $WG_OUT_DIR/uurls-offsets

$H_DIR/bin/hadoop fs -rmr $WG_OUT_DIR/layers/part*
$H_DIR/bin/hadoop fs -mkdir $WG_OUT_DIR/layers/ol
$H_DIR/bin/hadoop fs -mkdir $WG_OUT_DIR/layers/ts
$H_DIR/bin/hadoop fs -mkdir $WG_OUT_DIR/layers/bv

$H_DIR/bin/hadoop fs -mv  $WG_OUT_DIR/layers/out*   $WG_OUT_DIR/layers/ol/
$H_DIR/bin/hadoop fs -mv  $WG_OUT_DIR/layers/bit*   $WG_OUT_DIR/layers/bv/
$H_DIR/bin/hadoop fs -mv  $WG_OUT_DIR/layers/tim*   $WG_OUT_DIR/layers/ts/

