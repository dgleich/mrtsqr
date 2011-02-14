#!/usr/bin/env bash

# find the directory of this script
mydir=`dirname "$0"` # get relative path
mydir=`cd "$mydir"; pwd` # get absolute path

cd $mydir/.. # go to directory with tsqr_common.sh
. tsqr_common.sh

cd $mydir
export PYTHONPATH=$dumbo_dir:$PYTHONPATH
echo $PYTHONPATH
dumbo start ti_regress.py -reduce_schedule 250,1

