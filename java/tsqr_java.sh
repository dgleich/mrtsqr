#!/bin/bash

curdir=`pwd`;
mydir=`dirname "$0"`
mydir=`cd "$mydir"; pwd`

sofiles=`ls $NETLIB_JAVA_JNI/*.so`
netlib_java_files=""
for file in $sofiles; do
    netlib_java_files+="$file,"
done

hadoop jar $mydir/build/jar/tsqr-hadoop.jar gov.sandia.dfgleic.TSQR \
    -libjars ${HADOOP_STREAMING_JAR} -files $netlib_java_files  $@ 
