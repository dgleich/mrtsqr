#!/usr/bin/env dumbo convert

"""
dump_regression.py
==========

Write out the information from a regression problem for Matlab.
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
        
        for key,value in data:
            bi = value[0]
            row = value[1]
            print "%18.16e "%(bi),
            for val in row:
                print "%18.16e "%(val),
            print 
            
    
if __name__ == '__main__':
    import dumbo.cmd
    import dumbo.util
    dumbo.cmd.convert(__file__, sys.argv[1], dumbo.util.parseargs(sys.argv[2:]))

    
