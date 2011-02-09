#!/usr/bin/env python

"""
tsqr.py
=======

Tall and Skinny QR using hadoopy.

Usage
-----

    # ensure that the hadoop command executes the correct hadoop
    export HADOOP_HOME=/path/to/hadoop/dir
    python tsqr.py -mat <hdfspath> \
        [-output <hdfspath> -blocksize <int> -reduce_schedule <string>]
    
      -mat <path> : the path to a matrix stored in HDFS where the
        row is an array of values.  
      
      -output <path> : the output path.  If matpath is mydir/mymatrix.mseq
        then the default output value is mydir/mymatrix-qrr.mseq.
        This default perserves the name and the extension.
      
      -blocksize <int> : the number of blocks of rows to read before
        computing a QR compression of the data.  The default is 3 blocks,
        i.e. read three rows for each column.  For matrices with many
        columns ( > 500 ), consider reducing this to 2.  For matrices
        with few columns (< 50), consider increasing this to 4.
        
      -reduce_schedule <string> : This program can use either a single
        Hadoop job (the default) or a multi-stage iteration.  For large
        problems with many mappers, a multi-stage iteration will 
        improve parallelism.  The format is a comma separated list of
        the number of reduers to use for each iteration.
          Default: -reduce_schedule 1
        But, to use a two stage approach, then:
          -reduce_schedule 250,1
        will use 250 reducers for the first iteration, and 1 for the
        final.  The final number of reducers must be one.
        
        There is a special type of command that can be included here too.
        Using 
          -reduce_schedule s100,100,1
        will first use an identity map-reduce operation to spread the
        data over the cluster.  This will increase the number of mappers
        at the next stage, which can dramatically increase speed.
        
      -split_size <int> : the size of splits sent to mappers.  Increasing
        this value reduces the number of mappers launched for large 
        problems.  The default split_size is the HDFS block size 
        (dfs.block.size).  The size of the split is in bytes.
    
History
-------
:2010-01-27: Initial coding
"""

__author__ = 'David F. Gleich'

import sys
import os
import random
import time

import numpy
import numpy.linalg

import hadoopy

import hadoopy_util

# the globally saved options.  The actual mapreduce jobs pickup 
# their saved options from the command line environment.  The 
# source job picks up its options from the command line arguments.
gopts = hadoopy_util.SavedOptions()

class SerialTSQR():
    def __init__(self,blocksize=3,isreducer=False):
        self.blocksize=blocksize
        self.data = []
        if isreducer: self.__call__ = self.reducer
        else: self.__call__ = self.mapper
  
    def array2list(self,row):
        return [float(val) for val in row]

    def QR(self):
        A = numpy.array(self.data)
        return numpy.linalg.qr(A,'r')
        
    def compress(self):
        """ Compute a QR factorization on the data accumulated so far. """
        R = self.QR()
        # reset data and re-initialize to R
        self.data = []
        for row in R:
            self.data.append(self.array2list(row))
    
    def collect(self,key,value):
        self.data.append(value)
        if len(self.data)>self.blocksize:
            self.compress()

    def close(self):
        self.compress()
        for i,row in enumerate(self.data):
            key = random.randint(0,2000000000)
            yield key, row
            
    def mapper(self,key,value):
        self.collect(key,value)
        
    def reducer(self,key,values):
        for value in values:
            self.mapper(key,value)
           
if __name__=='__main__':
    blocksize = 3
    mapper = SerialTSQR(blocksize=blocksize,isreducer=False)
    reducer = SerialTSQR(blocksize=blocksize,isreducer=True)
    hadoopy.run(mapper, reducer)
