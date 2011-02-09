#!/bin/bash

HADOOP_DIR=/home/mrhadoop/hadoop-0.21.0
STREAMING_JAR=$HADOOP_DIR/mapred/contrib/streaming/hadoop-0.21.0-streaming.jar

hadoop fs -rmr colsums-output.vseq

hadoop jar $STREAMING_JAR -input $1 -output colsums-output.vseq \
  -jobconf mapreduce.job.name=colsums_cxx \
  -io typedbytes \
  -outputformat 'org.apache.hadoop.mapred.SequenceFileOutputFormat' \
  -inputformat 'org.apache.hadoop.streaming.AutoInputFormat' \
  -file colsums -mapper "./colsums map" -reducer './colsums reduce' \
  -jobconf mapreduce.input.fileinputformat.split.minsize=536870912

hadoop jar $STREAMING_JAR dumptb colsums-output.vseq | ./dump_typedbytes_info -  
