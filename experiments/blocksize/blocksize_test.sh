#!/bin/bash

mydir=`dirname "$0"`
mydir=`cd "$bin"; pwd`
cd $mydir

dumbo_dir=`cd ../../dumbo; pwd`
hadoopy_dir=`cd ../../hadoopy; pwd`
cxx_dir=`cd ../../cxx; pwd`

matbase=tsqr-mr/test/mat-500g
reduce_schedule=250,1

matcols="50 100 1000"
blocksize="2 3 5 10 20"
reduce_schedule="250,1"

cd $cxx_dir

for size in $matcols; do
  for bs in $blocksize; do
    python tsqr_cxx.py -mat $matbase-$size.mseq -blocksize $bs -reduce_schedule $reduce_schedule
  done
done  


#splitsize="67108864 268435456 102410241024"


# extra points
python tsqr_cxx.py -mat $matbase-50.mseq -blocksize 50 -reduce_schedule $reduce_schedule
python tsqr_cxx.py -mat $matbase-50.mseq -blocksize 100 -reduce_schedule $reduce_schedule
python tsqr_cxx.py -mat $matbase-50.mseq -blocksize 200 -reduce_schedule $reduce_schedule
