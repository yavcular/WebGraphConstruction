#!/bin/bash
# using cach 
# if dont wanna use cach, comment out last part of jar execution
# usage: LinkGraphUrlIdReplacement <in graph path> <in map path> <intermediate output path> <final output path> <use cache (true/false)> <cache path (optional)>

H_DIR=/home/yavcular/hadoop-0.21.0
WG_OUT_DIR=/user/yasemin/delis/

$H_DIR/bin/hadoop jar $H_DIR/g.jar nyu/cs/webgraph/analysis/MortalityCount /user/yasemin/delis/analysis-mortality/count0-r-00000 >> linkMortality
$H_DIR/bin/hadoop jar $H_DIR/g.jar nyu/cs/webgraph/analysis/MortalityCount /user/yasemin/delis/analysis-mortality/count1-r-00000 >> linkMortality
$H_DIR/bin/hadoop jar $H_DIR/g.jar nyu/cs/webgraph/analysis/MortalityCount /user/yasemin/delis/analysis-mortality/count2-r-00000 >> linkMortality
$H_DIR/bin/hadoop jar $H_DIR/g.jar nyu/cs/webgraph/analysis/MortalityCount /user/yasemin/delis/analysis-mortality/count3-r-00000 >> linkMortality
$H_DIR/bin/hadoop jar $H_DIR/g.jar nyu/cs/webgraph/analysis/MortalityCount /user/yasemin/delis/analysis-mortality/count4-r-00000 >> linkMortality
$H_DIR/bin/hadoop jar $H_DIR/g.jar nyu/cs/webgraph/analysis/MortalityCount /user/yasemin/delis/analysis-mortality/count5-r-00000 >> linkMortality
$H_DIR/bin/hadoop jar $H_DIR/g.jar nyu/cs/webgraph/analysis/MortalityCount /user/yasemin/delis/analysis-mortality/count6-r-00000 >> linkMortality
$H_DIR/bin/hadoop jar $H_DIR/g.jar nyu/cs/webgraph/analysis/MortalityCount /user/yasemin/delis/analysis-mortality/count7-r-00000 >> linkMortality
$H_DIR/bin/hadoop jar $H_DIR/g.jar nyu/cs/webgraph/analysis/MortalityCount /user/yasemin/delis/analysis-mortality/count8-r-00000 >> linkMortality
$H_DIR/bin/hadoop jar $H_DIR/g.jar nyu/cs/webgraph/analysis/MortalityCount /user/yasemin/delis/analysis-mortality/count9-r-00000 >> linkMortality
$H_DIR/bin/hadoop jar $H_DIR/g.jar nyu/cs/webgraph/analysis/MortalityCount /user/yasemin/delis/analysis-mortality/count10-r-00000 >> linkMortality

