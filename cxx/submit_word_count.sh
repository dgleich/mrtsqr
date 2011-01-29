#!/bin/bash

HADOOP_DIR=/home/mrhadoop/hadoop-0.21.0
STREAMING_JAR=$HADOOP_DIR/mapred/contrib/streaming/hadoop-0.21.0-streaming.jar

hadoop fs -rmr test-wc-cc

hadoop jar $STREAMING_JAR -input gutenberg-small/132.txt -output test-wc-cc \
  -jobconf mapreduce.job.name=word_count_cxx \
  -jobconf 'stream.map.input=typedbytes' -jobconf 'stream.reduce.input=typedbytes' \
  -jobconf 'stream.map.output=typedbytes' -jobconf 'stream.reduce.output=typedbytes' \
  -outputformat 'org.apache.hadoop.mapred.SequenceFileOutputFormat' \
  -inputformat 'org.apache.hadoop.streaming.AutoInputFormat' \
  -file word_count -mapper "./word_count map" -reducer "./word_count reduce" \


  
hadoop jar $STREAMING_JAR dumptb test-wc-cc | ./dump_typedbytes_info -  
