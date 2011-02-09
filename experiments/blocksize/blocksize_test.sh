#!/bin/bash

mydir=`dirname "$0"`
mydir=`cd "$bin"; pwd`
cd $mydir

dumbo_dir=`cd ../../dumbo; pwd`
hadoopy_dir=`cd ../../hadoopy; pwd`
cxx_dir=`cd ../../cxx; pwd`

logfile=$mydir/experiment.log

matbase=tsqr-mr/test/mat-500g
reduce_schedule=250,1

matcols="50 100 1000"
blocksize="2 3 5 10 20"
reduce_schedule="250,1"

echo >> $logfile
echo >> $logfile
echo >> $logfile
echo >> $logfile
echo "new run" >> $logfile
date >> $logfile

cd $cxx_dir

function run_tsqr {
    args="-mat $1.mseq -blocksize $2 -reduce_schedule $3"
    echo $args >> $logfile
    
    before="$(date +%s)"
    python tsqr_cxx.py $args
    if [ $? -ne 0 ]; then
      result="ERROR"
    else
      result="SUCCESS"
    fi
    after="$(date +%s)"
    elapsed_seconds="$(expr $after - $before)"
    echo Result: $result >> $logfile
    echo Elapsed: $elapsed_seconds >> $logfile
   
    echo Checking $1-qrr.mseq >> $logfile
    dumbo convert $dumbo_dir/check_test_problem.py $1-qrr.mseq >> $logfile
    echo >> $logfile
    echo >> $logfile
}

for size in $matcols; do
  for bs in $blocksize; do
    run_tsqr $matbase-$size $bs $reduce_schedule
  done
done  


#splitsize="67108864 268435456 102410241024"


# extra points
run_tsqr $matbase-50 50 $reduce_schedule
run_tsqr $matbase-50 100 $reduce_schedule
run_tsqr $matbase-50 200 $reduce_schedule
