#!/bin/bash

mydir=`dirname "$0"`
mydir=`cd "$bin"; pwd`
cd $mydir

dumbo_dir=`cd ../../dumbo; pwd`
hadoopy_dir=`cd ../../hadoopy; pwd`
cxx_dir=`cd ../../cxx; pwd`

matbase=tsqr-mr/test/mat-500g-500
#matbase=tsqr-mr/test/mat-5g-100-bigblock
test_matrix=$matbase.mseq
check_matrix=$matbase-qrr.mseq
blocksize=3
reduce_schedule=250,1

args="-mat $test_matrix -blocksize $blocksize -reduce_schedule $reduce_schedule"

cd $dumbo_dir
dumbo start tsqr.py $args
dumbo convert check_test_problem.py $check_matrix 

cd $hadoopy_dir
python tsqr.py $args
dumbo convert $dumbo_dir/check_test_problem.py $check_matrix

cd $cxx_dir
python tsqr_cxx.py $args
dumbo convert $dumbo_dir/check_test_problem.py $check_matrix


