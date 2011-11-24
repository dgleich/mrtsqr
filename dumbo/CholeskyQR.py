#!/usr/bin/env dumbo

"""
Cholesky.py
===========

Implement a Cholesky QR algorithm using dumbo and numpy.

Assumes that matrix is stored as a typedbytes vector and that
the user knows how many columns are in the matrix.
"""

import sys
import os
import time
import random
import struct

import numpy
import numpy.linalg

import util

import dumbo
import dumbo.backends.common

# create the global options structure
gopts = util.GlobalOptions()

class Cholesky(dumbo.backends.common.MapRedBase):
    def __init__(self,ncols=10):
        self.data = range(ncols)
        self.ncols = ncols
    
    def array2list(self,row):
        return [float(val) for val in row]

    def close(self):
        L = numpy.linalg.cholesky(self.data)
        M = numpy.mat(L.T)
        for ind, row in enumerate(M.getA()):
            yield ind, row

    def __call__(self,data):
        for key,values in data:
            for value in values:
                self.data[key] = list(struct.unpack('d'*self.ncols, value))
                
        for key,val in self.close():
            yield key, val

class AtA(dumbo.backends.common.MapRedBase):
    def __init__(self,blocksize=3,keytype='random',isreducer=False,ncols=10):
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
        self.ncols = ncols
        self.A_curr = None
        self.row = None
    
    def _firstkey(self, i):
        if isinstance(self.first_key, (list,tuple)):
            return (util.flatten(self.first_key),i)
        else:
            return (self.first_key,i)
    
    def array2list(self,row):
        return [float(val) for val in row]

    def compress(self):
        # Compute AtA on the data accumulated so far
        if self.ncols is None:
            return
        if len(self.data) < self.ncols:
            return
            
        t0 = time.time()
        A_mat = numpy.mat(self.data)
        A_flush = A_mat.T*A_mat
        dt = time.time() - t0
        self.counters['numpy time (millisecs)'] += int(1000*dt)

        # reset data and add flushed update to local copy
        self.data = []
        if self.A_curr == None:
            self.A_curr = A_flush
        else:
            self.A_curr = self.A_curr + A_flush

    
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
            if not len(value) == self.ncols:
                return
        
        self.data.append(value)
        self.nrows += 1
        
        if len(self.data)>self.blocksize*self.ncols:
            self.counters['AtA Compressions'] += 1
            # compress the data
            self.compress()
            
        # write status updates so Hadoop doesn't complain
        if self.nrows%50000 == 0:
            self.counters['rows processed'] += 50000

    def close(self):
        self.counters['rows processed'] += self.nrows%50000
        self.compress()
        for ind, row in enumerate(self.A_curr.getA()):
            r = self.array2list(row)
            yield ind, struct.pack('d'*len(r),*r)

            
    def __call__(self,data):
        if self.isreducer == False:
            # map job
            for key,value in data:
                value = list(struct.unpack('d'*self.ncols, value))
                self.collect(key,value)

        else:
            for key,values in data:
                for value in values:
                    val = list(struct.unpack('d'*self.ncols, value))
                    if self.row == None:
                        self.row = numpy.array(val)
                    else:
                        self.row = self.row + numpy.array(val)                        
                yield key, struct.pack('d'*len(self.row),*self.row)

        # finally, output data
        if self.isreducer == False:
            for key,val in self.close():
                yield key, val
    
def runner(job):
    blocksize = gopts.getintkey('blocksize')
    schedule = gopts.getstrkey('reduce_schedule')
    ncols = gopts.getintkey('ncols')
    if ncols <= 0:
       sys.exit('ncols must be a positive integer')
    
    schedule = schedule.split(',')
    for i,part in enumerate(schedule):
        if part.startswith('s'):
            nreducers = int(part[1:])
            # these tasks should just spray data and compress
            job.additer(mapper="org.apache.hadoop.mapred.lib.IdentityMapper",
                reducer="org.apache.hadoop.mapred.lib.IdentityReducer",
                opts=[('numreducetasks',str(nreducers))])
            job.additer(mapper, reducer, opts=[('numreducetasks',str(nreducers))])
            
        else:
            nreducers = int(part)
            if i==0:
                mapper = AtA(blocksize=blocksize,isreducer=False,ncols=ncols)
                reducer = AtA(blocksize=blocksize,isreducer=True,ncols=ncols)
            else:
                mapper = 'org.apache.hadoop.mapred.lib.IdentityMapper'
                reducer = Cholesky(ncols=ncols)
            job.additer(mapper=mapper, reducer=reducer, opts=[('numreducetasks',str(nreducers))])
    

def starter(prog):
    
    print "running starter!"
    
    mypath =  os.path.dirname(__file__)
    print "my path: " + mypath
    
    # set the global opts
    gopts.prog = prog
    
   
    mat = prog.delopt('mat')
    if not mat:
        return "'mat' not specified'"
        
    prog.addopt('memlimit','2g')
    
    nonumpy = prog.delopt('use_system_numpy')
    if nonumpy is None:
        print >> sys.stderr, 'adding numpy egg: %s'%(str(nonumpy))
        prog.addopt('libegg', 'numpy')
        
    prog.addopt('file',os.path.join(mypath,'util.py'))


    numreps = prog.delopt('replication')
    if not numreps:
        numreps = 1
    for i in range(int(numreps)):
        prog.addopt('input',mat)

    #prog.addopt('input',mat)
    matname,matext = os.path.splitext(mat)
    
    gopts.getintkey('blocksize',3)
    gopts.getstrkey('reduce_schedule','1')
    gopts.getintkey('ncols', -1)
    
    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s-chol-qrr%s'%(matname,matext))
        
    splitsize = prog.delopt('split_size')
    if splitsize is not None:
        prog.addopt('jobconf',
            'mapreduce.input.fileinputformat.split.minsize='+str(splitsize))
        
    prog.addopt('overwrite','yes')
    prog.addopt('jobconf','mapred.output.compress=true')
    
    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)
