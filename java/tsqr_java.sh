#!/bin/bash

curdir=`pwd`;
mydir=`dirname "$0"`
mydir=`cd "$mydir"; pwd`

hadoop jar $mydir/build/jar/tsqr-hadoop.jar gov.sandia.dfgleic.TSQR -libjars ${HADOOP_STREAMING_JAR} $@ 
