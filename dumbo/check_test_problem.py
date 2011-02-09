#!/usr/bin/env dumbo

"""

"""

import sys
import math
import numpy
    
class Converter:
    def __init__(self,opts):
        self.rows = []
    def __call__(self,data):
        item = 0
        for key,value in data:
            self.rows.append(value)
        if len(self.rows) == 0:
            print >>sys.stderr, "ERROR: the file is empty."
            sys.exit(-1)
            
        ncols = len(self.rows[0])
        eps = numpy.finfo('float').eps
        nerrs = 0
        
        for i,row in enumerate(self.rows):
            if len(row) != ncols:
                print >>sys.stderr, \
                    "ERROR: row %i has %i cols but row 1 had %i cols"%(
                    i+1, len(row), ncols)
                sys.exit(-1)
            
            r = numpy.array(row)
            r = r*numpy.sign(r[i]) # scale by the sign of the diagonal
            
            for j in xrange(i,ncols):
                if abs(r[i]-1.) > 10*eps:
                    nerrs += 1
                    if nerrs <= 10:
                        print >> sys.stderr, \
                            "[%2i] INCORRECT entry (%i,%i) diff=%18.16e"%(
                            nerrs, i+1, j+1, abs(r[i]-1.))
                        if nerrs == 10:
                            print >> sys.stderr, \
                                "  ... skipping further errors ... "
        if nerrs > 0:
            print "INCORRECT: total incorrect entries %i\n"%(nerrs)
            sys.exit(1)
        else:
            print "CORRECT"
            sys.exit(0)
        
    
if __name__ == '__main__':
    import dumbo.cmd
    import dumbo.util
    dumbo.cmd.convert(__file__, sys.argv[1], dumbo.util.parseargs(sys.argv[2:]))


    
