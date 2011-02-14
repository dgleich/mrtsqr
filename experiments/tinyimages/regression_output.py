#!/usr/bin/env dumbo convert

"""
regression_output.py
==========

Take the output from a TSQR Least Squares problem and output
the regression coefficients.
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
        print "Computing regression solution to "
        print "TSQR Least Squares output in %s"%(filename)
        print 
        print "Reading data..."
        item = 0
        mat = []
        b = []
        ncols = None
        nrows = 0
        t0 = time.time()
        for key,value in data:
            b.append(value[0])
            row = value[1]
            if ncols is None:
                ncols = len(row)
                print "  ncols=%i"%(ncols)
            if len(row) != ncols:
                print >>sys.stderr, "error on row %i: rowlen=%i != ncols=%i"%(
                    nrows+1, len(row), ncols)
            mat.append(row)
            nrows += 1
        dt = time.time() - t0
        print "  nrows=%i (done! %.1f sec)"%(nrows, dt)
        if nrows != ncols:
            print >>sys.stderr, "error: matrix is not square (%i-by-%i)"%(
                nrows, ncols)
            sys.exit(1)
        
        print "Solving system"
        R = numpy.array(mat)
        y = numpy.array(b)
        x = numpy.linalg.solve(R,y)
        t0 = time.time()
        
        dt = time.time() - t0
        print "  (done! %.1f sec)"%(dt)
        # output S, V
        
        path,filename = os.path.split(filename)
        base,ext = os.path.splitext(filename)
        
        Rfilename = base + "-R.tmat"
        yfilename = base + "-y.tmat"
        solfilename = base + "-sol.tmat"
        print "Writing R matrix to %s"%(Rfilename)
        Rf = open(Rfilename,'wt')
        for row in R:
            for entry in row:
                Rf.write("%18.16e "%(entry))
            Rf.write("\n")
        Rf.close()
        
        print "Writing y vector to %s"%(yfilename)
        yf = open(yfilename,'wt')
        for entry in y:
            yf.write("%18.16e\n"%(entry))
        yf.close()
        
        print "Writing solution vector to %s"%(solfilename)
        xf = open(solfilename, 'wt')
        for entry in x:
            xf.write("%18.16e\n"%(entry))
        xf.close()
    
        
    
if __name__ == '__main__':
    import dumbo.cmd
    import dumbo.util
    dumbo.cmd.convert(__file__, sys.argv[1], dumbo.util.parseargs(sys.argv[2:]))

    
