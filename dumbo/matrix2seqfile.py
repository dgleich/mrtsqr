#!/usr/bin/env dumbo

"""
Convert a textual matrix file into a sequence file of typed bytes
"""

import sys

"""
Map lines of a matrix to a sequence file:
  Key=<lineno>, Value=[row_i]
"""
def mapper(key,value):
    valarray = [float(v) for v in value.split()]
    if len(valarray) == 0:
        return
    yield key, valarray
    
class Converter:
    def __init__(self,opts):
        pass
    def __call__(self,data):
        item = 0
        for key,value in data:
            for entry in value:
                print entry, 
            print
            item += 1
    
if __name__ == '__main__':
    import dumbo
    import dumbo.lib
    dumbo.run(mapper,dumbo.lib.identityreducer)

    
