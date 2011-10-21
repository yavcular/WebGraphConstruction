#!/bin/bash

H_DIR=/home/yavcular/hadoop-0.21.0

$H_DIR/wg-bin/compile-graph-jar.sh

lg=`date`

echo   
echo creating the link graph ..  
echo  
$H_DIR/wg-bin/make-linkgraphNuniqueUrls.sh 
sleep 10

uu=`date`

echo   
echo assignning ids ..  
$H_DIR/wg-bin/assign-ids.sh 
sleep 10

rep=`date`

echo   
echo replacing ..  
$H_DIR/wg-bin/replace.sh 

end=`date`
 
echo $lg linkgraphstarted >> results
echo $uu unique urls started >> results
echo $rep replace ids started >> results
echo $end completed >> results
