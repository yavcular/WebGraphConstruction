#!/bin/bash
FSPATH=/user/yasemin/webgraph/output/ie-historical
/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rmr $FSPATH/linkgraph
/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rmr $FSPATH/linkgraph-nosort
/home/yavcular/hadoop-0.21.0/bin/hadoop fs -rmr $FSPATH/urlmap

# /user/yasemin/webgraph/NLI-IE-HISTORICAL-IE-DOMAIN
/home/yavcular/hadoop-0.21.0/bin/hadoop jar /home/yavcular/hadoop-0.21.0/g.jar nyu/cs/webgraph/main/CreateLinkGraphNUniqueUrls /user/yasemin/webgraph/NLI-IE-HISTORICAL-IE-DOMAIN $FSPATH/linkgraph $FSPATH/urlmap

#NLI-IE-FOCUSEDCRAWL.13sites
#/home/yavcular/hadoop-0.21.0/bin/hadoop jar /home/yavcular/hadoop-0.21.0/g.jar nyu/cs/webgraph/main/CreateLinkGraphNUniqueUrls /user/yasemin/webgraph/NLI-IE-FOCUSEDCRAWL.13sites  $FSPATH/linkgraph $FSPATH/urlmap

#4sites
#/home/yavcular/hadoop-0.21.0/bin/hadoop jar /home/yavcular/hadoop-0.21.0/g.jar nyu/cs/webgraph/main/CreateLinkGraphNUniqueUrls /user/yasemin/webgraph/4sites  $FSPATH/linkgraph $FSPATH/urlmap

#NLI-IE-FOCUSEDCRAWL
#/home/yavcular/hadoop-0.21.0/bin/hadoop jar /home/yavcular/hadoop-0.21.0/g.jar nyu/cs/webgraph/main/CreateLinkGraphNUniqueUrls /user/yasemin/webgraph/NLI-IE-FOCUSEDCRAWL  $FSPATH/linkgraph $FSPATH/urlmap


