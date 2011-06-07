#!/bin/bash
mydir=`dirname "$0"`
mydir=`cd "$mydir"; pwd`
cd $mydir

../../java/tsqr_java.sh -mat tsqr-mr/test/mat-500g-500.mseq -reduce_schedule 250,1 -blocksize 3
