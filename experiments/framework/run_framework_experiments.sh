#!/bin/bash

mydir=`dirname "$0"`
mydir=`cd "$bin"; pwd`
cd $mydir/..

. tsqr_common.sh

logfile=$mydir/experiment.log

matbase=tsqr-mr/test/mat-500g-500
#matbase=tsqr-mr/test/mat-5g-100-bigblock
test_matrix=$matbase.mseq
check_matrix=$matbase-qrr.mseq
blocksize=3
reduce_schedule=250,1




function run_tsqr {
    cmd=$1
    echo "Running: $1" >> $logfile
    
    before="$(date +%s)"
    $cmd
    if [ $? -ne 0 ]; then
      result="ERROR"
    else
      result="SUCCESS"
    fi
    after="$(date +%s)"
    elapsed_seconds="$(expr $after - $before)"
    echo Result: $result >> $logfile
    echo Elapsed: $elapsed_seconds >> $logfile
   
    echo Checking $check_matrix  >> $logfile
    dumbo convert $dumbo_dir/check_test_problem.py $check_matrix  >> $logfile
    echo >> $logfile
    echo >> $logfile
}

args="-mat $test_matrix -blocksize $blocksize -reduce_schedule $reduce_schedule"

cd $dumbo_dir
run_tsqr "dumbo start tsqr.py $args"

cd $hadoopy_dir
run_tsqr "python tsqr.py $args"

cd $cxx_dir
run_tsqr "python tsqr_cxx.py $args"


