#!/usr/bin/env dumbo

"""
only_one_record.py
------------------
Check that only one record comes out of the test.

You can check that all records come out by making sure the number
of mapped records is equal
"""


import sys
import struct
import dumbo
import dumbo.lib
import array

def unpack_key(key,reclen):
    key = struct.unpack('>q',key)[0]
    if key%(reclen) is not 0:
        print >>sys.stderr,"Warning, unpacking invalid key %i\n"%(key)
    key = key/(reclen)
    return key

def mapper(data):
    npairs = 0
    for key,val in data:
        key = unpack_key(key,3*1024)
            
        if npairs==0:
            im = array.array('B',val)
            print >>sys.stderr, "val len = %i"%(len(val))
        
            for i in xrange(min(len(im),10)):
                print >>sys.stderr, "val[%i] = %i"%(i, im[i])
        
        yield key,1
        npairs += 1
        
def reducer(key,values):

    nval = 0
    for val in values:
        nval += 1
    if nval>1:
        print >>sys.stderr,"key %i has %i values"%(key)
        yield key, nval
    
                
    
def starter(prog):
    prog.addopt('inputformat','org.apache.hadoop.mapred.lib.FixedLengthInputFormat')
    prog.addopt('jobconf','mapreduce.input.fixedlengthinputformat.record.length=3072')
    prog.addopt('input','/data/tinyimages/original/tiny_images.bin')
    prog.addopt('output','tinyimages/test')
    prog.addopt('overwrite','yes')
    prog.addopt('libjar','build/jar/hadoop-lib.jar')

def runner(job):    
    job.additer(mapper,reducer)
    
if __name__=='__main__':
    dumbo.main(runner, starter)
