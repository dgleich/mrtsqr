#!/usr/bin/env dumbo

"""
svd.py
===========

Implement a svd algorithm using dumbo and numpy using tsqr.py
"""

import pprint

import sys
import os
import time
import random

import numpy
import numpy.linalg

import util

import dumbo
import dumbo.util
import dumbo.backends.common

import tsqr

# create the global options structure
gopts = util.GlobalOptions()

class TextMatrixConverter:
    """ Convert from Hadoop typed bytes to a textual matrix """
    def __init__(self,filename):
        self.filename = filename
    def __call__(self,data):
        file = open(self.filename, 'w')
        for key,value in data:
            for entry in value:
                file.write("%18.16e "%(entry))
            file.write('\n');
        file.close()

def setup_left_svd(backend, fs, opts):
    """ Setup the left-sided SVD after a TSQR.
    
    This function is called by the host python command right
    before starting the SVDLeft map-reduce iteration.
    """
    #print >>sys.stderr, str(opts)
    input = dumbo.util.getopt(opts,'input',delete=False)[0]
    iter = dumbo.util.getopt(opts,'iteration',delete=False)[0]
    output = dumbo.util.getopt(opts,'output',delete=False)[0]
    
    # here, the input is the matrix name
    lastiter = output + "_pre%s"%(iter)
    
    localR = gopts.getstrkey('tsqr_R_filename')
    
    #localR = os.path.splitext(input)[0] + "-R.tmat"
    #localR = 'svd-R.tmat'
    print >>sys.stderr
    print >>sys.stderr, "Copying %s to %s"%(lastiter,localR)
    print >>sys.stderr
    
    conv = TextMatrixConverter(localR)
    fs.convert(lastiter, opts, conv)
    
    opts.append(('file',localR))

class ComputeSVDLeft(dumbo.backends.common.MapRedBase):
    """ Compute the left factor in the SVD given the other two factors.
    
    This function requies a few matrices to be distributed to
    each processor.  Usually by the distributed cache.
    """

    def __init__(self,Rfilename,blocksize=3):
        self.blocksize=blocksize
        self.nrows = 0
        self.data = []
        self.keys = []
        self.ncols = None
        self.Rfilename = Rfilename
        #self.V = numpy.loadtxt('svd-V.tmat')
        #self.S = numpy.loadtxt('svd-S.tmat')
 
    def collect(self,key, value):
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
            
        self.keys.append(key)
        self.data.append(value)
        self.nrows += 1
        
    def output(self,final=False):
        if final or len(self.data)>=self.blocksize*self.ncols:
            self.counters['Blocks Output'] += 1
            # compress the data
            
            if self.ncols is None:
                return
            if len(self.data) < self.ncols:
                return
                
            t0 = time.time()
            A = numpy.array(self.data)
            U = self.compute_U(A)
            dt = time.time() - t0
            self.counters['numpy time (millisecs)'] += int(1000*dt)
            
            assert(U.shape[0] == len(self.keys))
            
            
            for i,row in enumerate(U):
                yield self.keys[i], util.array2list(row)
                
            self.data = []
            self.keys = []
            
    
    def compute_U(self,A):
        """ Compute AR^{+} for the pseudo-inverse """
        #print >>sys.stderr, "A.shape: " + str(A.shape)
        #print >>sys.stderr, "self.V.shape: " + str(self.V.shape)
        AR = numpy.dot(A,self.V)
        #print >>sys.stderr, "AR.shape: " + str(AR.shape)
        #print >>sys.stderr, "diag(self.Sinv).shape: " + str(numpy.diag(self.Sinv).shape)
        AR = numpy.dot(AR,numpy.diag(self.Sinv))
        return AR
    
    def __call__(self,data):
        # startup
        R = numpy.loadtxt(self.Rfilename)
        U,S,Vt = numpy.linalg.svd(R)
        #self.Ut = U.transpose()
        self.V = Vt.transpose()
        tol = max(R.shape)*numpy.finfo(float).eps*max(S)
        r = numpy.sum(S>tol)
        self.Sinv = 1./S[0:r]
        if len(self.Sinv) < self.V.shape[1]:
            self.Sinv = numpy.hstack((self.Sinv,
                numpy.zeros(self.V.shape[1] - len(self.Sinv))))
        # map job
        for key,value in data:
            if isinstance(value, str):
                # handle conversion from string
                value = [float(p) for p in value.split()]
                
            self.collect(key,value)
            for key,value in self.output():
                yield key, value
     
        # finally, output data
        for key,value in self.output():
            yield key,value
    
def runner(job):
    #niter = int(os.getenv('niter'))
    
    blocksize = gopts.getintkey('blocksize')
    schedule = gopts.getstrkey('reduce_schedule')
    finalreduce = gopts.getstrkey('final_reduce')
    
    schedule = schedule.split(',')
    for i,part in enumerate(schedule):
        if part.startswith('s'):
            nreducers = int(part[1:])
            # these tasks should just spray data and compress
            job.additer(mapper="org.apache.hadoop.mapred.lib.IdentityMapper",
                reducer="org.apache.hadoop.mapred.lib.IdentityReducer",
                opts=[('numreducetasks',str(nreducers))])
        else:
            nreducers = int(part)
            if i==0:
                mapper = tsqr.SerialTSQR(blocksize=blocksize,isreducer=False)
            else:
                mapper = 'org.apache.hadoop.mapred.lib.IdentityMapper'
            job.additer(mapper=mapper,
                    reducer=tsqr.SerialTSQR(blocksize=blocksize,isreducer=True),
                    opts=[('numreducetasks',str(nreducers))])

    Rfile = gopts.getstrkey('tsqr_R_filename')
        
    job.additer(mapper=ComputeSVDLeft(Rfile,blocksize=blocksize),
        input=-1,
        premapper=setup_left_svd,
        opts=[('numreducetasks',str(finalreduce))])


def starter(prog):
    
    print "running starter!"
    
    mypath =  os.path.dirname(__file__)
    print "my path: " + mypath
    
    # set the global opts
    gopts.prog = prog
    
   
    mat = prog.delopt('mat')
    if not mat:
        return "'mat' not specified'"
        
    prog.addopt('memlimit','4g')
    
    nonumpy = prog.delopt('use_system_numpy')
    pprint.pprint(nonumpy)
    if nonumpy is not None:
        pass
    else:
        prog.addopt('libegg','numpy')
        
    prog.addopt('file',os.path.join(mypath,'util.py'))
    prog.addopt('file',os.path.join(mypath,'tsqr.py'))
    
    prog.addopt('input',mat)
    matname,matext = os.path.splitext(mat)
    
    gopts.getintkey('blocksize',3)
    gopts.getstrkey('reduce_schedule','1')
    gopts.getstrkey('final_reduce','1')
    gopts.setkey('input',mat)
    
    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s-svd-U%s'%(matname,matext))
        
    splitsize = prog.delopt('split_size')
    if splitsize is not None:
        prog.addopt('jobconf',
            'mapreduce.input.fileinputformat.split.minsize='+str(splitsize))
        
    prog.addopt('overwrite','yes')
    prog.addopt('jobconf','mapred.output.compress=true')
    
    gopts.setkey('tsqr_R_filename',os.path.split(matname)[1]+'-R.tmat')
    
    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)
