#!/usr/bin/env dumbo convert

"""
pca_svd.py
==========

Take the output from a TSQR for a PCA problem, and output
the actual principal components.
"""

import sys
import os

import numpy
import numpy.linalg
import time

class Converter:
    def __init__(self,opts):
        pass
    def __call__(self,data):
        filename = sys.argv[1]
        print
        print "Computing SVD of PCA R matrix %s"%(filename)
        print 
        print "Reading data..."
        item = 0
        mat = []
        ncols = None
        nrows = 0
        t0 = time.time()
        for key,value in data:
            if ncols is None:
                ncols = len(value)
                print "  ncols=%i"%(ncols)
            if len(value) != ncols:
                print >>sys.stderr, "error on row %i: rowlen=%i != ncols=%i"%(
                    nrows+1, len(value), ncols)
            mat.append(value)
            nrows += 1
        dt = time.time() - t0
        print "  nrows=%i (done! %.1f sec)"%(nrows, dt)
        if nrows != ncols:
            print >>sys.stderr, "warning: matrix is not square (%i-by-%i)"%(
                nrows, ncols)
        
        print "Computing SVD ..."
        t0 = time.time()
        A = numpy.array(mat)
        U,S,V = numpy.linalg.svd(A)
        dt = time.time() - t0
        print "  (done! %.1f sec)"%(dt)
        # output S, V
        
        path,filename = os.path.split(filename)
        base,ext = os.path.splitext(filename)
        
        Vfilename = base + "-V.tmat"
        Sfilename = base + "-S.tmat"
        Rfilename = base + ".tmat"
        print "Writing V matrix to %s"%(Vfilename)
        Vf = open(Vfilename,'wt')
        for row in V:
            for entry in row:
                Vf.write("%18.16e "%(entry))
            Vf.write("\n")
        Vf.close()
        
        print "Writing S diagonal to %s"%(Vfilename)
        Sf = open(Sfilename,'wt')
        for entry in S:
            Sf.write("%18.16e\n"%(entry))
        Sf.close()
        
        print "Writing R matrix to %s"%(Rfilename)
        Rf = open(Rfilename, 'wt')
        for row in A:
            for entry in row:
                Rf.write("%18.16e "%(entry))
            Rf.write("\n")
        Rf.close()
    
        
    
if __name__ == '__main__':
    import dumbo.cmd
    import dumbo.util
    dumbo.cmd.convert(__file__, sys.argv[1], dumbo.util.parseargs(sys.argv[2:]))

    
