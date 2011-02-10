#!/bin/bash

mydir=`dirname "$0"`
mydir=`cd "$mydir"; pwd`
cd $mydir

HADOOP_DIR=/home/mrhadoop/hadoop-0.21.0
STREAMING_JAR=$HADOOP_DIR/mapred/contrib/streaming/hadoop-0.21.0-streaming.jar

hadoop fs -rmr colsums-output.vseq

before="$(date +%s)"
hadoop jar $STREAMING_JAR -input $1 -output colsums-output.vseq \
  -jobconf mapreduce.job.name=colsums_cxx \
  -io typedbytes \
  -outputformat 'org.apache.hadoop.mapred.SequenceFileOutputFormat' \
  -inputformat 'org.apache.hadoop.streaming.AutoInputFormat' \
  -file $mydir/colsums -mapper "./colsums map" -reducer './colsums reduce' \
  -jobconf mapreduce.input.fileinputformat.split.minsize=536870912
after="$(date +%s)"
elapsed_seconds="$(expr $after - $before)"  

echo
echo
echo
echo Column sums of $1
echo Total time: $elapsed_seconds
echo
echo
echo

