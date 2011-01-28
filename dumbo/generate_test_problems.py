#!/usr/bin/env dumbo
""" Generate TSQR test problems. 

These problem use a constant R factor.  R is just the upper triangle
of the all ones matrix.

The output of this script is a Hadoop distributed sequence file, 
where each key is a random number, and each value is a row of
the matrix.


History
-------
:2011-01-26: Initial coding
:2011-01-27: Added maprows to let mappers handle more than ncols of data.
"""

__author__ = 'David F. Gleich'


import sys
import os
import re
import math
import random

import dumbo
import dumbo.util
import dumbo.lib
import numpy

import util

# create the global options structure
gopts = util.GlobalOptions()


def first_mapper(data):
    """ This mapper doesn't take any input, and generates the R factor. """
    hostname = os.uname()[1]
    print >>sys.stderr, hostname, "is a mapper"
    
    # suck up all the data so Hadoop doesn't complain
    for key,val in data:
        pass
    
    n = gopts.getintkey('ncols')
    m = int(os.getenv('nrows'))
    k = int(os.getenv('maprows'))/n
    s = float(m)/float(n)
    util.setstatus(
        "generating %i-by-%i R matrix with scale factor %i/%i=%s"%(
        n, n, m, n, s))
    
    R = numpy.triu(numpy.ones((n,n)))/math.sqrt(s)
    
    for i in xrange(k):
        util.setstatus(
            'step %i/%i: generating local %i-by-%i Q matrix'%(i+1,k,n,n))
        
        Q = numpy.linalg.qr(numpy.random.randn(n,n))[0] # just the Q factor
        A = Q.dot(R)
        util.setstatus('step %i/%i: outputting %i rows'%(i+1,k,A.shape[0]))
        for row in A:
            key = random.randint(0, 4000000000)
            yield key, util.array2list(row)
            
def localQoutput(rows):
    
    util.setstatus('converting to numpy array')
    A = numpy.array(rows)
    localm = A.shape[0]
    
    util.setstatus('generating local Q of size %i-by-%i'%(localm,localm))
    Q = numpy.linalg.qr(numpy.random.randn(localm,localm))[0] # just the Q factor
    util.setstatus(
        'multiplying %i-by-%i A by %i-by-%i Q'%(localm,A.shape[1],localm,localm))
    A = Q.dot(A)
    
    util.setstatus('outputting')
    for row in A:
        yield util.array2list(row)
                
        
def second_mapper(data):
    
    n = gopts.getintkey('ncols')
    m = int(os.getenv('nrows'))
    maxlocal = int(os.getenv('maxlocal'))
    
    rows = []
    util.setstatus('acquiring data with ncols=%i'%(n))
    
    for key,value in data:
        assert(len(value) == n)
        
        rows.append(value)
        
        if len(rows) >= maxlocal:
            dumbo.util.incrcounter('Program','rows acquired',len(rows))
            
            for row in localQoutput(rows):
                key = random.randint(0, 4000000000)
                yield key, row
            
            # reset rows, status
            rows = []
            util.setstatus('acquiring data with ncols=%i'%(n))
            
    if len(rows) > 0:
        for row in localQoutput(rows):
            key = random.randint(0, 4000000000)
            yield key, row
    

def starter(prog):
    """ Start the program with a null input. """
    # get options
    
    # set the global opts
    gopts.prog = prog
    
    prog.addopt('memlimit','4g')
    prog.addopt('file','util.py')
    prog.addopt('libegg','numpy')
    
    
    m = gopts.getintkey('nrows',None) # error with no key
    n = gopts.getintkey('ncols',None) # error with no key
    
    
    maprows = gopts.getintkey('maprows',2*n)
    stages = gopts.getintkey('nstages',2)
    maxlocal = gopts.getintkey('maxlocal',n)
        
    if maprows % n is not 0:
        maprows = (maprows/n)*n
        gopts.setkey('maprows',maprows)
        print "'maprows' adjusted to", maprows, "to ensure integer k in maprows=k*ncols"
    
    if m % maprows is not 0:
        m = ((m/maprows)+1)*maprows
        gopts.setkey('nrows',m)
        print "'nrows' changed to", m, "to ensure scalar integer k in nrows=k*maprows"
    
    print "using", stages, "stages"
    
    gopts.save_params()
   
    prog.addopt('input','IGNORED')
    prog.addopt('libjar','../java/build/jar/hadoop-lib.jar')
    prog.addopt('inputformat','gov.sandia.dfgleic.NullInputFormat')
    
    prog.addopt('jobconf','mapred.output.compress=true')


def runner(job):
    # grab info from environment
    m = gopts.getintkey('nrows')
    maprows = gopts.getintkey('maprows')
    k = m/maprows
    stages = gopts.getintkey('nstages')
    
    print >>sys.stderr, "using %i map tasks"%(k)
    
    for i in xrange(stages):
        if i==0:
            opts = [('numreducetasks',str(k)),
                    ('nummaptasks',str(k))]
            job.additer(first_mapper,
                "org.apache.hadoop.mapred.lib.IdentityReducer",
                opts=opts)
        else:
            job.additer(second_mapper,"org.apache.hadoop.mapred.lib.IdentityReducer",
                opts=[('numreducetasks',str(k))])
            
if __name__=='__main__':
    # find the hadoop examples jar
    dumbo.main(runner, starter)

    
