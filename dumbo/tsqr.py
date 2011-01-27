#!/usr/bin/env dumbo

"""
tsqr.py
===========

Implement a tsqr algorithm using dumbo and numpy
"""

import sys
import os
import numpy
import numpy.linalg

import util
import random

import dumbo
import dumbo.backends.common

class SerialTSQR(dumbo.backends.common.MapRedBase):
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
        return numpy.linalg.qr(A,'r')
        
    def compress(self):
        """ Compute a QR factorization on the data accumulated so far. """
        R = self.QR()
        
        # reset data and re-initialize to R
        self.data = []
        for row in R:
            self.data.append(self.array2list(row))
            
            
    
    def collect(self,key,value):
        if len(self.data) == 0:
            self.first_key = key
        
        if self.ncols == None:
            self.ncols = len(value)
            print >>sys.stderr, "Matrix size: %i columns"%(self.ncols)
        else:
            # TODO should we warn and truncate here?
            # No. that seems like something that will introduce
            # bugs.  Maybe we could add a "liberal" flag
            # for that.
            assert(len(value) == self.ncols)
        
        self.data.append(value)
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
    
opts = {
    'reduce_schedule': '1'
}
        
def runner(job):
    #niter = int(os.getenv('niter'))
    
    job.additer(mapper=SerialTSQR(isreducer=False),
            reducer=SerialTSQR(isreducer=True),
            opts=[('numreducetasks','1')])
    
    

def starter(prog):
    global opts
    mat = prog.delopt('mat')
    if not mat:
        return "'mat' not specified'"
        
    #schedule = prog.delopt('schedule')
    #if niter:
    #    opts['niter'] = int(niter)
    
    #prog.addopt('param','niter='+str(opts['niter']))
    #os.putenv('niter',str(opts['niter']))
    
    prog.addopt('libegg','numpy')
    prog.addopt('file','util.py')
    
    prog.addopt('input',mat)
    matname,matext = os.path.splitext(mat)
    
    prog.addopt('memlimit','4g')
    
    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s-qrr%s'%(matname,matext))
        
    prog.addopt('overwrite','yes')


if __name__ == '__main__':
    dumbo.main(runner, starter)
