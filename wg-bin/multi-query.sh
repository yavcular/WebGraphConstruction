#!/bin/bash
HADOOP_COMMON_HOME="/home/yavcular/hadoop-0.21.0"

function get()
{
	i="10"
	while [ $i -lt 20 ]
	do
 	  echo $i
	  $HADOOP_COMMON_HOME/bin/hadoop fs -text /user/yasemin/webgraph/output/foucusedcrawl/linkgraph/part-r-000$i | wc -l 
	  i=$[$i+1]
	done
}

function size()
{

      while read line   
      do
        #pdsh -R ssh -w $line ' df -h /scratch/yasemin/'
	#hebe=`eval pdsh -R ssh -w $line 'ls /scratch/yasemin/WGoutput/local-data/ | wc -l'`
        #hebe=`eval pdsh -R ssh -w $line ' grep yyyyyyyy  /scratch/yasemin/WGoutput/local-data/link-partition-idreplaced-0 | wc -l'`
        echo $hebe
        echo "-"
      done <$FILE

}




if [ ! -f $FILE ]; then
  	echo "$FILE : does not exists"
  	exit 1
   elif [ ! -r $FILE ]; then
  	echo "$FILE: can not read"
  	exit 2
fi


if [ -n "$1" ]; then
   if [ "$1" = "get" ]; then
	get

  elif [ "$1" = "size" ]; then
        size
  else
       echo "incorrect argument!"
    fi
else
    now=`date`
    echo $now hebele
    echo "should provide an argument!"
fi

