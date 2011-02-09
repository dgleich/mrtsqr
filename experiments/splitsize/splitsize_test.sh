#!/bin/bash

mydir=`dirname "$0"`
mydir=`cd "$bin"; pwd`
cd $mydir

dumbo_dir=`cd ../../dumbo; pwd`
hadoopy_dir=`cd ../../hadoopy; pwd`
cxx_dir=`cd ../../cxx; pwd`

logfile=$mydir/experiment.log

matbase=tsqr-mr/test/mat-500g


reduce_schedule="1 250,1"
splitsize="67108864 268435456 102410241024"

echo "new run" >> $logfile
date >> $logfile

cd $cxx_dir

for rs in $reduce_schedule; do
  for ss in $splitsize; do
    echo -mat $matbase-50.mseq -blocksize 100 -reduce_schedule $rs -split_size $ss >> $logfile
    
    before="$(date +%s)"
    python tsqr_cxx.py -mat $matbase-50.mseq -blocksize 100 \
      -reduce_schedule $rs -split_size $ss
    after="$(date +%s)"
    elapsed_seconds="$(expr $after - $before)"
    echo Elapsed: $elapsed_seconds >> $logfile
   
    echo Checking $matbase-50-qrr.mseq >> $logfile
    dumbo convert $dumbo_dir/check_test_problem.py $matbase-50-qrr.mseq >> $logfile
    
    
    echo -mat $matbase-1000.mseq -blocksize 5 -reduce_schedule $rs -split_size $ss -big_mem 5g >> $logfile
    
    before="$(date +%s)"
    python tsqr_cxx.py -mat $matbase-1000.mseq -blocksize 5 \
      -reduce_schedule $rs -split_size $ss -big_mem 10g
    after="$(date +%s)"
    elapsed_seconds="$(expr $after - $before)"
    echo Elapsed: $elapsed_seconds >> $logfile
   
    echo Checking $matbase-1000-qrr.mseq >> $logfile
    dumbo convert $dumbo_dir/check_test_problem.py $matbase-1000-qrr.mseq >> $logfile
  done
done  
