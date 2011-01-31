#!/usr/bin/env python

"""
tsqr_cxx.py
===========

Submit the C++ version of tsqr to the hadoop streaming system.
"""
__author__ = 'David F. Gleich'

"""
History
-------
:2011-01-30: Initial coding

Todo
----
* Add code to find hadoop
* Add code to find hadoop streaming
* Add reduce schedule
* Handle directory offset
* Handle updating tsqr executable
"""

import sys
import os

import subprocess

import hadoopy

def get_args(argv):
    args = {}
    for i,arg in enumerate(argv):
        if arg[0] == '-':
            if i+1 < len(argv):
                val = argv[i+1]
            else:
                val = None
            args[arg[1:]] = val
    return args

if __name__=='__main__':
    hadoop_dir = '/home/mrhadoop/hadoop-0.21.0/'
    streaming_jar = hadoop_dir + 'mapred/contrib/streaming/hadoop-0.21.0-streaming.jar'
    
    args = get_args(sys.argv[1:])
    
    hadoop_args = []
    
    if 'mat' in args:
        mat = args['mat']
    else:
        print >>sys.stderr, "Error: -mat not specified"
        
    input = mat
    matname,matext = os.path.splitext(mat)
    
    if 'output' in args:
        output = args['output']
    else:
        output = '%s-qrr%s'%(matname,matext)
        
    jobname = 'tsqr_cxx ' + matname
    
    if 'block_size' in args:
        blocksize = int(args['block_size'])
    else:
        blocksize = 3

    if 'split_size' in args:
        hadoop_args.extend(['-jobconf',
            'mapreduce.input.fileinputformat.split.minsize='+
            args['split_size']])
    
    hadoop_args.extend(['-io', 'typedbytes'])
    hadoop_args.extend(['-file', 'tsqr'])
    hadoop_args.extend(['-mapper', "'./tsqr map %i'"%(blocksize)])
    hadoop_args.extend(['-reducer', "'./tsqr reduce %i'"%(blocksize)])
    hadoop_args.extend(['-outputformat', "'org.apache.hadoop.mapred.SequenceFileOutputFormat'"])
    hadoop_args.extend(['-inputformat', "'org.apache.hadoop.streaming.AutoInputFormat'"])
    
    
    # now we would handle the reduce schedule, or whatever else is
    hadoop_args.extend(['-jobconf',"'mapreduce.job.name="+jobname+"'"])
    hadoop_args.extend(['-input',"'"+input+"'"])
    hadoop_args.extend(['-output',"'"+output+"'"])
    
    cmd = ['hadoop','jar',streaming_jar]
    cmd.extend(hadoop_args)
    
    print "Running Hadoop Command:"
    print
    print ' '.join(cmd) 
    print
    print "End Hadoop Command"
    
                
    if hadoopy.exists(output):
        print "Removing %s"%(output)
        hadoopy.rm(output)

    subprocess.check_call(' '.join(cmd),shell=True)


