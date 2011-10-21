#!/bin/bash
FSPATH=/user/yasemin/wg/output
INPATH=/user/yasemin/wg/input
/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rmr $FSPATH/linkgraph
/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rmr $FSPATH/linkgraph-nosort



#ALL
echo MakeLinkGraph - inputpath: $INPATH
/home/yavcular/hadoop-0.21.0/bin/hadoop jar /home/yavcular/hadoop-0.21.0/g.jar nyu/cs/webgraph/main/CreateLinkGraph $INPATH/ $FSPATH/linkgraph


#FOCUSEDCRAWL
#echo MakeLinkGraph - inputpath: $INPATH/NLI-IE-FOCUSEDCRAWL/
#/home/yavcular/hadoop-0.21.0/bin/hadoop jar /home/yavcular/hadoop-0.21.0/g.jar nyu/cs/webgraph/main/CreateLinkGraph $INPATH/NLI-IE-FOCUSEDCRAWL/ $FSPATH/linkgraph

# $INPATH/NLI-IE-HISTORICAL-IE-DOMAIN
#echo MakeLinkGraph - inputpath: $INPATH/NLI-IE-HISTORICAL-IE-DOMAIN
#/home/yavcular/hadoop-0.21.0/bin/hadoop jar /home/yavcular/hadoop-0.21.0/g.jar nyu/cs/webgraph/main/CreateLinkGraph $INPATH/NLI-IE-HISTORICAL-IE-DOMAIN $FSPATH/linkgraph 

#NLI-IE-CRAWL
#echo MakeLinkGraph - inputpath $INPATH/NLI-IE-CRAWL
#/home/yavcular/hadoop-0.21.0/bin/hadoop jar /home/yavcular/hadoop-0.21.0/g.jar nyu/cs/webgraph/main/CreateLinkGraph $INPATH/NLI-IE-CRAWL  $FSPATH/linkgraph 


/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rmr $FSPATH/linkgraph/part*
/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rmr $FSPATH/sample-merged
/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rmr $FSPATH/sample
