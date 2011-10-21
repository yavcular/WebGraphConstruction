#!/bin/bash

H_DIR=/home/yavcular/hadoop-0.21.0

echo 
echo
echo starting all

$H_DIR/wg-bin/compile-graph-jar.sh


lg=`date`

echo   
echo creating the link graph ..  
echo  
$H_DIR/wg-bin/make-linkgraph.sh 
sleep 10

uu=`date`

echo   
echo getting unique urls ..  
echo   
#$H_DIR/wg-bin/get-unique-urls.sh 
sleep 10

ai=`date`

echo   
echo assignning ids ..  
#$H_DIR/wg-bin/assign-ids.sh 
sleep 10

rep=`date`

echo   
echo replacing ..  
$H_DIR/wg-bin/replace.sh 


layers=`date`

echo   
echo finallayers ..  
$H_DIR/wg-bin/layers.sh




end=`date`




 
echo $lg linkgraphstarted >> results
echo $uu unique urls started >> results
echo $ai assignids started >> results
echo $rep replace ids started >> results
echo $final final layers started >> results
echo $end completed >> results
