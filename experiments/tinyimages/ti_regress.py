#!/usr/bin/env python

"""
History
-------
:2010-02-04: Initial coding
:2010-02-07: Changed to dumbo
"""

__author__ = 'David F. Gleich'

import sys
import os
import random
import time

import numpy
import numpy.linalg

import array

import util

import dumbo
import dumbo.backends.common

import struct


# create the global options structure
gopts = util.GlobalOptions()

class TinyImagesRegression:
    def __call__(self,key,value):
        """ 
        @param key a long for the byte-offset into the tiny-images file
        @param value a byte-string for the current image.
        """
        im = array.array('B',value)
        #keystr = key
        #print >>sys.stderr,"keystr: ", repr(keystr)
        key = struct.unpack('>q',key)[0]
        #print >>sys.stderr,"key: ", key
        key = key/(3*1024)
        
        if key>1000:
            # only work on the first 1000 images
            pass
        else:
            # sum red, green, and blue pixels
            red = im[0:1024]
            green = im[1024:2048]
            blue = im[2048:3072]
            yield key, (sum(red),sum(green),sum(blue))
        

class TSQRLeastSquares(dumbo.backends.common.MapRedBase):
    def __init__(self,blocksize=3,keytype='random',isreducer=False):
        self.blocksize=blocksize
        if keytype=='random':
            self.keyfunc = lambda x: random.randint(0, 4000000000)
        elif keytype=='first':
            self.keyfunc = self._firstkey
        else:
            raise Error("Unkonwn keytype %s"%(keytype))
        self.first_key = None
        self.isreducer=isreducer
        self.nrows = 0
        self.data = []
        self.ncols = None
    
    def _firstkey(self, i):
        if isinstance(self.first_key, (list,tuple)):
            return (util.flatten(self.first_key),i)
        else:
            return (self.first_key,i)
    
    def array2list(self,row):
        return [float(val) for val in row]

    def QR(self):
        A = numpy.array(self.data)
        b = numpy.array(self.rhs)
        Q, R = numpy.linalg.qr(A,'full')
        c = numpy.dot(Q.T, b)
        nb = numpy.norm(b)
        nc = numpy.norm(c)
        self.resid += nb - nc
        return R, c
        
    def compress(self):
        """ Compute a QR factorization on the data accumulated so far. """
        t0 = time.time()
        R, c = self.QR()
        dt = time.time() - t0
        self.counters['numpy time (millisecs)'] += int(1000*dt)
        
        # reset data and re-initialize to R
        self.data = []
        for row in R:
            self.data.append(self.array2list(row))
           
        self.rhs = []
        for entry in c:
            self.rhs.append(entry)
            
            
    def collect(self,key,row,entry):
        """
        @param key the key for the row, rhs entry pair
        @param row the row of the matrix
        @param entry the right hand side entry for the least squares problem
        """
        if len(self.data) == 0:
            self.first_key = key
            
        if self.ncols == None:
            self.ncols = len(row)
            print >>sys.stderr, "Matrix size: %i columns"%(self.ncols)
        else:
            # TODO should we warn and truncate here?
            # No. that seems like something that will introduce
            # bugs.  Maybe we could add a "liberal" flag
            # for that.
            assert(len(row) == self.ncols)
        
        self.data.append(row)
        self.rhs.append(entry)
        self.nrows += 1
        
        if len(self.data)>self.blocksize*self.ncols:
            self.counters['QR Compressions'] += 1
            # compress the data
            self.compress()
            
        # write status updates so Hadoop doesn't complain
        if self.nrows%50000 == 0:
            self.counters['rows processed'] += 50000
            
    def __call__(self,data):
        if self.isreducer == False:
            # map job
            for key,value in data:
                if isinstance(value, str):
                    # handle conversion from string
                    value = [float(p) for p in value.split()]
                self.collect(key,value)
                
        else:
            for key,values in data:
                for value in values:
                    self.collect(key,value)
        # finally, output data
        self.compress()
        for i,row in enumerate(self.data):
            key = self.keyfunc(i)
            yield key, row
    
def runner(job):
    #niter = int(os.getenv('niter'))
    
    blocksize = gopts.getintkey('blocksize')
    schedule = gopts.getstrkey('reduce_schedule')
    
    schedule = schedule.split(',')
    for iter,part in enumerate(schedule):
        if iter > 0:
            nreducers = int(part)
            job.additer(mapper=SerialTSQR(blocksize=blocksize,isreducer=False),
                    reducer=SerialTSQR(blocksize=blocksize,isreducer=True),
                    opts=[('numreducetasks',str(nreducers))])
        else:
            nreducers = int(part)
            job.additer(mapper=TinyImagesRegression,
                    #reducer=SerialTSQR(blocksize=blocksize,isreducer=True),
                    reducer = dumbo.lib.identityreducer,
                    opts=[('numreducetasks',str(nreducers)),
                          ('inputformat','org.apache.hadoop.mapred.lib.FixedLengthInputFormat'),
                          ('jobconf','mapreduce.input.fixedlengthinputformat.record.length=3072'),
                          ('libjar','../../java/build/jar/hadoop-lib.jar')])

def starter(prog):
    
    print "running starter!"
    
    # set the global opts
    gopts.prog = prog
    
    prog.addopt('memlimit','4g')
    prog.addopt('libegg','numpy')
    prog.addopt('file','util.py')
    
    input = '/data/tinyimages/original/tiny_images.bin'
    output = 'tsqr-mr/ti/test-output'
    
    gopts.getintkey('blocksize',3)
    gopts.getstrkey('reduce_schedule','1')
    
    
    prog.addopt('input',input)
    prog.addopt('output',output)
    prog.addopt('overwrite','yes')
    
    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)





