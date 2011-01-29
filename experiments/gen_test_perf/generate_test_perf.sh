
mydir=`dirname "$0"`
mydir=`cd "$bin"; pwd`
cd $mydir
cd ../../dumbo


cat $mydir/test-50-a.tb | \
  nrows=1000000000 ncols=50 maprows=1000000 maxlocal=$1 nstages=3 \
  stream_map_output=typedbytes stream_map_input=typedbytes \
  dumbo_mrbase_class=dumbo.backends.common.MapRedBase \
  dumbo_jk_class=dumbo.backends.common.JoinKey \
  dumbo_runinfo_class=dumbo.backends.streaming.StreamingRunInfo \
  python2.6 -m generate_test_problems map 1 4294967296 &> /dev/null

