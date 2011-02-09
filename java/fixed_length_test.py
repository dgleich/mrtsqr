#!/usr/bin/env dumbo


import sys
import struct
import dumbo
import dumbo.lib
import array

def mapper(data):
    npairs = 0
    for key,val in data:
        if npairs>1:
            continue
        print >>sys.stderr, "key len = %i"%(len(key))
        print >>sys.stderr, "val len = %i"%(len(val))
        
        im = array.array('B',val)
        
        for i in xrange(min(len(im),10)):
            print >>sys.stderr, "val[%i] = %i"%(i, im[i])
        
        yield key,val
        npairs += 1
        
    
def starter(prog):
    prog.addopt('inputformat','org.apache.hadoop.mapred.lib.FixedLengthInputFormat')
    prog.addopt('jobconf','mapreduce.input.fixedlengthinputformat.record.length=3072')
    prog.addopt('input','/data/tinyimages/original/tiny_images.bin')
    prog.addopt('output','tinyimages/test')
    prog.addopt('overwrite','yes')
    prog.addopt('libjar','build/jar/hadoop-lib.jar')

def runner(job):    
    job.additer(mapper,dumbo.lib.identityreducer)
    
if __name__=='__main__':
    dumbo.main(runner, starter)
