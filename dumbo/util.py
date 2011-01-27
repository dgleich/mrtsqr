"""
util.py
=======

Utility routines for the tsqr and regression code.
"""

import sys
import dumbo.util

def setstatus(msg):
    print >>sys.stderr, "Status: ", msg
    dumbo.util.setstatus(msg)

def array2list(row):
    return [float(val) for val in row]

""" A utility to flatten a lists-of-lists. """
def flatten(l, ltypes=(list, tuple)):
    ltype = type(l)
    l = list(l)
    i = 0
    while i < len(l):
        while isinstance(l[i], ltypes):
            if not l[i]:
                l.pop(i)
                i -= 1
                break
            else:
                l[i:i + 1] = l[i]
        i += 1
    return ltype(l)
