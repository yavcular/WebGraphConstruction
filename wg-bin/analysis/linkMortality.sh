#!/bin/bash
# using cach 
# if dont wanna use cach, comment out last part of jar execution
# usage: LinkGraphUrlIdReplacement <in graph path> <in map path> <intermediate output path> <final output path> <use cache (true/false)> <cache path (optional)>

H_DIR=/home/yavcular/hadoop-0.21.0
WG_OUT_DIR=/user/yasemin/delis/

$H_DIR/bin/hadoop fs -rmr $WG_OUT_DIR/analysis-mortality
$H_DIR/bin/hadoop jar $H_DIR/g.jar nyu/cs/webgraph/analysis/AnalyseLinkMortality $1 $2 $WG_OUT_DIR/analysis-mortality

