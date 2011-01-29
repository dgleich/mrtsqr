#!/bin/bash

HADOOP_DIR=/home/mrhadoop/hadoop-0.21.0
STREAMING_JAR=$HADOOP_DIR/mapred/contrib/streaming/hadoop-0.21.0-streaming.jar

hadoop fs -rmr matfiles/huge-test-qrr.mseq
hadoop fs -rmr matfiles/huge-test-qrr.mseq_iter1

hadoop jar $STREAMING_JAR -input matfiles/huge-test.mseq -output matfiles/huge-test-qrr.mseq_iter1 \
  -jobconf mapreduce.job.name='tsqr_huge_cxx (1/2)' \
  -jobconf 'stream.map.input=typedbytes' -jobconf 'stream.reduce.input=typedbytes' \
  -jobconf 'stream.map.output=typedbytes' -jobconf 'stream.reduce.output=typedbytes' \
  -outputformat 'org.apache.hadoop.mapred.SequenceFileOutputFormat' \
  -inputformat 'org.apache.hadoop.streaming.AutoInputFormat' \
  -file tsqr -mapper './tsqr map' -reducer './tsqr reduce' \
  -numReduceTasks 250
  
hadoop jar $STREAMING_JAR -input matfiles/huge-test-qrr.mseq_iter1 -output matfiles/huge-test-qrr.mseq \
  -jobconf mapreduce.job.name='tsqr_huge_cxx (2/2)' \
  -jobconf 'stream.map.input=typedbytes' -jobconf 'stream.reduce.input=typedbytes' \
  -jobconf 'stream.map.output=typedbytes' -jobconf 'stream.reduce.output=typedbytes' \
  -outputformat 'org.apache.hadoop.mapred.SequenceFileOutputFormat' \
  -inputformat 'org.apache.hadoop.streaming.AutoInputFormat' \
  -file tsqr -mapper './tsqr map' -reducer './tsqr reduce' \
  -numReduceTasks 1  

