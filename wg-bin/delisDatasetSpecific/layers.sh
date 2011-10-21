#!/bin/bash
# using cach 
# if dont wanna use cach, comment out last part of jar execution
# usage: LinkGraphUrlIdReplacement <in graph path> <in map path> <intermediate output path> <final output path> <use cache (true/false)> <cache path (optional)>

H_DIR=/home/yavcular/hadoop-0.21.0
WG_OUT_DIR=/user/yasemin/wg/output

$H_DIR/bin/hadoop fs -rmr $WG_OUT_DIR/layers
$H_DIR/bin/hadoop jar $H_DIR/g.jar nyu/cs/webgraph/main/PrepareFinalLayerFiles $WG_OUT_DIR/final-lg/ $WG_OUT_DIR/layers

$H_DIR/bin/hadoop fs -rmr $WG_OUT_DIR/layers/part*

# cache dir
# $WG_HOME/output/foucusedcrawl/cache/ 


