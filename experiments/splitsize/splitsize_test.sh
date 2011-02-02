#!/bin/bash

mydir=`dirname "$0"`
mydir=`cd "$bin"; pwd`
cd $mydir

dumbo_dir=`cd ../../dumbo; pwd`
hadoopy_dir=`cd ../../hadoopy; pwd`
cxx_dir=`cd ../../cxx; pwd`

matbase=tsqr-mr/test/mat-500g


reduce_schedule="1 250,1"
splitsize="67108864 268435456 102410241024"

cd $cxx_dir

for rs in $reduce_schedule; do
  for ss in $splitsize; do
    python tsqr_cxx.py -mat $matbase-50.mseq -blocksize 100 \
      -reduce_schedule $rs -split_size $ss
    python tsqr_cxx.py -mat $matbase-1000.mseq -blocksize 5 \
      -reduce_schedule $rs -split_size $ss
  done
done  
