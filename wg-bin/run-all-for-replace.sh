#!/bin/bash

H_DIR=/home/yavcular/hadoop-0.21.0
INPUT=$1
NUM_OF_CAPTURES=$2

$H_DIR/wg-bin/compile-graph-jar.sh

lg=`date`

echo   
echo supersample ..  
$H_DIR/wg-bin/supersample.sh $INPUT $NUM_OF_CAPTURES
sleep 5

echo   
echo get-unique-urls ..  
$H_DIR/wg-bin/get-unique-urls.sh $INPUT
sleep 5

echo   
echo assignning ids ..  
$H_DIR/wg-bin/assign-ids-mapred.sh
sleep 5

rep=`date`

echo   
echo replace ..  
$H_DIR/wg-bin/replace.sh $INPUT

end=`date`
 
echo $lg linkgraphstarted >> results
echo $uu unique urls started >> results
echo $rep replace ids started >> results
echo $end completed >> results
