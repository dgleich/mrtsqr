#!/usr/bin/env dumbo
""" Generate TSQR test problems. 

These problem use a constant R factor.  R is just the upper triangle
of the all ones matrix.

The output of this script is a Hadoop distributed sequence file, 
where each key is a random number, and each value is a row of
the matrix.

An artificial limitation of this script is that the number of 
rows of the matrix must be a scalar multiple of the number of columns.


History
-------
:2011-01-26: Initial coding
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

def first_mapper(data):
    """ This mapper doesn't take any input, and generates the R factor. """
    hostname = os.uname()[1]
    print >>sys.stderr, hostname, "is a mapper"
    
    # suck up all the data so Hadoop doesn't complain
    for key,val in data:
        pass
    
    n = int(os.getenv('ncols'))
    m = int(os.getenv('nrows'))
    s = float(m)/float(n)
    util.setstatus(
        "generating %i-by-%i R matrix with scale factor %i/%i=%s"%(
        n, n, m, n, s))
    
    
    R = numpy.triu(numpy.ones((n,n)))/math.sqrt(s)
    
    util.setstatus('generating local %i-by-%i Q matrix'%(n,n))
    Q = numpy.linalg.qr(numpy.random.randn(n,n))[0] # just the Q factor
    A = Q.dot(R)
    util.setstatus('outputting %i rows'%(A.shape[0]))
    for row in A:
        key = random.randint(0, 4000000000)
        yield key, util.array2list(row)
        
def second_mapper(data):
    
    n = int(os.getenv('ncols'))
    m = int(os.getenv('nrows'))
    rows = []
    util.setstatus('acquiring data with ncols=%i'%(n))
    for key,value in data:
        assert(len(value) == n)
        rows.append(value)
    dumbo.util.incrcounter('Program','rows acquired',len(rows))
    
    util.setstatus('converting to numpy array')
    A = numpy.array(rows)
    localm = A.shape[0]
    
    util.setstatus('generating local Q of size %i-by-%i'%(localm,localm))
    Q = numpy.linalg.qr(numpy.random.randn(localm,localm))[0] # just the Q factor
    A = Q.dot(A)
    
    util.setstatus('outputting')
    for row in A:
        key = random.randint(0, 4000000000)
        yield key, util.array2list(row)
    
    
def starter(prog):
    """ Start the program with a null input. """
    # get options
    
    prog.addopt('file','util.py')
    prog.addopt('libegg','numpy')
    
    m = prog.delopt('nrows')
    n = prog.delopt('ncols')
    if m is None or n is None:
        return "'nrows' or 'ncols' not specified'"
        
    m = int(m)
    n = int(n)
    
    stages = prog.delopt('nstages')
    if stages is None:
        stages = 2
    else:
        stages = int(stages)
    
    if m % n is not 0:
        m = ((m/n) + 1)*n
        print "'nrows' changed to", m, "to ensure scalar nrows=k*ncols"
    
    print "using", stages, "stages"
    
    prog.addopt('param','nrows='+str(m))
    os.putenv('nrows',str(m))
    prog.addopt('param','ncols='+str(n))
    os.putenv('ncols',str(n))
    prog.addopt('param','nstages='+str(stages))
    os.putenv('nstages',str(stages))
    
    prog.addopt('input','IGNORED')
    prog.addopt('libjar','../java/build/jar/hadoop-lib.jar')
    prog.addopt('inputformat','gov.sandia.dfgleic.NullInputFormat')


def runner(job):
    # grab info from environment
    m = int(os.getenv('nrows'))
    n = int(os.getenv('ncols'))
    k = m/n
    stages = int(os.getenv('nstages'))
    
    print >>sys.stderr, "using %i map tasks"%(k)
    
    for i in xrange(stages):
        if i==0:
            opts = [('numreducetasks',str(k)),
                    ('nummaptasks',str(k))]
            job.additer(first_mapper,dumbo.lib.identityreducer,opts=opts)
        else:
            job.additer(second_mapper,dumbo.lib.identityreducer,
                opts=[('numreducetasks',str(k))])
            
if __name__=='__main__':
    # find the hadoop examples jar
    dumbo.main(runner, starter)

    
