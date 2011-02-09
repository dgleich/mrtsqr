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
import tsqr

import dumbo
import dumbo.backends.common

import struct


# create the global options structure
gopts = util.GlobalOptions()

class TinyImages:
    def unpack_key(self,key):
        #keystr = key
        #print >>sys.stderr,"keystr: ", repr(keystr)
        key = struct.unpack('>q',key)[0]
        if key%(3*1024) is not 0:
            print >>sys.stderr,"Warning, unpacking invalid key %i\n"%(key)
        #print >>sys.stderr,"key: ", key
        key = key/(3*1024)
        return key
        
    def unpack_value(self,value):
        im = array.array('B',value)
        red = im[0:1024]
        green = im[1024:2048]
        blue = im[2048:3072]
        
        return (red,green,blue)
        
    def sum_rgb(self,value):
        (red,green,blue) = self.unpack_value(value)
        return (sum(red),sum(green),sum(blue))
        
    def togray(self,value):
        """ Convert an input value in bytes into a floating point grayscale array. """
        (red,green,blue) = self.unpack_value(value)
        
        gray = []
        for i in xrange(1024):
            graypx = (0.299*float(red[i]) + 0.587*float(green[i]) +
                0.114*float(blue[i]))/255.
            gray.append(graypx)
            
        return gray
        
class TinyImagesPCA(tsqr.SerialTSQR, TinyImages):
    def __init__(self,blocksize):
        tsqr.SerialTSQR.__init__(self,blocksize=blocksize,isreducer=False)
        
    def __call__(self,data):
        """ 
        @param key a long for the byte-offset into the tiny-images file
        @param value a byte-string for the current image.
        """
        
        firstkey = True
        
        for key,value in data:
            key = self.unpack_key(key)
            

            gray = self.togray(value)
            
            mean = sum(gray)/float(len(gray))
            for i in xrange(len(gray)):
                gray[i] -= mean # center the pixels
                
            # supply to TSQR
            self.collect(key,gray)
            
            #if firstkey:
                #print >>sys.stderr, "key: %i, sumrgb="%(key), self.sum_rgb(value)
                #print >>sys.stderr, "key: %i, sumgray=%18.16e"%(key,sum(gray)) 
                #print >>sys.stderr, "key: %i, maxgray=%18.16e"%(key,max(gray))
                #print >>sys.stderr, "key: %i, mingray=%18.16e"%(key,min(gray))  
                #print >>sys.stderr, "key: %i, lengray=%18.16e"%(key,len(gray))
                
            firstkey = False
                
                #yield key, gray
                
        # finally, output data
        for k,v in self.close():
            yield k,v
    
def runner(job):
    #niter = int(os.getenv('niter'))
    
    blocksize = gopts.getintkey('blocksize')
    schedule = gopts.getstrkey('reduce_schedule')
    
    schedule = schedule.split(',')
    for iter,part in enumerate(schedule):
        if iter > 0:
            nreducers = int(part)
            job.additer(mapper='org.apache.hadoop.mapred.lib.IdentityMapper',
                    reducer=tsqr.SerialTSQR(blocksize=blocksize,isreducer=True),
                    opts=[('numreducetasks',str(nreducers))])
        else:
            nreducers = int(part)
            job.additer(mapper=TinyImagesPCA(blocksize=blocksize),
                    reducer=tsqr.SerialTSQR(blocksize=blocksize,isreducer=True),
                    #reducer = dumbo.lib.identityreducer,
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
    prog.addopt('file','../../dumbo/util.py')
    prog.addopt('file','../../dumbo/tsqr.py')
    
    input = '/data/tinyimages/original/tiny_images.bin'
    output = 'tsqr-mr/ti/pca-R.mseq'
    
    gopts.getintkey('blocksize',3)
    gopts.getstrkey('reduce_schedule','1')
    
    # determine the split size
    splitsize = prog.delopt('split_size')
    if splitsize is not None:
        prog.addopt('jobconf',
            'mapreduce.input.fileinputformat.split.minsize='+str(splitsize))
    
    prog.addopt('input',input)
    prog.addopt('output',output)
    prog.addopt('overwrite','yes')
    
    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)





