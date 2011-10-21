#!/bin/bash
H_DIR=/home/yavcular/hadoop-0.21.0
rm $H_DIR/g.jar
rm -rf $H_DIR/yasemin-classpath/*
#$H_DIR/omalley-hadoop-gpl-compression-LZO/

#javac -classpath $H_DIR/hadoop-hdfs-0.21.0:$H_DIR/hadoop-common-0.21.0.jar:$H_DIR/hadoop-mapred-0.21.0.jar:$H_DIR/hadoop-mapred-tools-0.21.0.jar:$H_DIR/lib/commons-cli-1.2.jar:$H_DIR/lib/commons-logging-1.1.1.jar -d $H_DIR/yasemin-classpath/ $H_DIR/examples/nyu/cs/webgraph/main/*.java $H_DIR/examples/nyu/cs/webgraph/MRhelpers/*.java $H_DIR/examples/nyu/cs/webgraph/optional/*.java $H_DIR/examples/nyu/cs/webgraph/structures/*.java $H_DIR/examples/nyu/cs/webgraphframework/*.java $H_DIR/omalley-hadoop-gpl-compression-LZO/*

#javac -sourcepath $H_DIR/kevinweil-hadoop-lzo-927df21/ -classpath $H_DIR/hadoop-hdfs-0.21.0:$H_DIR/hadoop-common-0.21.0.jar:$H_DIR/hadoop-mapred-0.21.0.jar:$H_DIR/hadoop-mapred-tools-0.21.0.jar:$H_DIR/lib/commons-cli-1.2.jar:$H_DIR/lib/commons-logging-1.1.1.jar -d $H_DIR/yasemin-classpath/ $H_DIR/examples/nyu/cs/webgraph/main/*.java $H_DIR/examples/nyu/cs/webgraph/MRhelpers/*.java $H_DIR/examples/nyu/cs/webgraph/optional/*.java $H_DIR/examples/nyu/cs/webgraph/structures/*.java $H_DIR/examples/nyu/cs/webgraph/DELIS/*.java $H_DIR/examples/nyu/cs/webgraph/archiveUK/*.java

javac -sourcepath $H_DIR/kevinweil-hadoop-lzo-927df21/:$H_DIR/examples/:$H_DIR/fast-md5/ -classpath $H_DIR/hadoop-hdfs-0.21.0:$H_DIR/hadoop-common-0.21.0.jar:$H_DIR/hadoop-mapred-0.21.0.jar:$H_DIR/hadoop-mapred-tools-0.21.0.jar:$H_DIR/lib/commons-cli-1.2.jar:$H_DIR/lib/commons-logging-1.1.1.jar -d $H_DIR/yasemin-classpath/ $H_DIR/examples/nyu/cs/webgraph/main/*.java $H_DIR/examples/nyu/cs/webgraph/DELIS/*.java $H_DIR/examples/nyu/cs/webgraph/archiveUK/*.java $H_DIR/examples/nyu/cs/webgraph/optional/*.java  $H_DIR/examples/nyu/cs/webgraph/updates/*.java  $H_DIR/examples/nyu/cs/webgraph/analysis/*.java


jar -cvf g.jar -C $H_DIR/yasemin-classpath/ .
