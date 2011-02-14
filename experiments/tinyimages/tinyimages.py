#!/usr/bin/env python

"""
tinyimages.py
=============

This file contains a class: TinyImages that makes it easy to manipulate
the tinyimages data when read via a TypedBytes wrapper around the 
FixedLengthRecordReader from hadoop.  The class provides many nice
convinence functions.  An easy way to use it just to make your
class inherit from TinyImages too.
"""

import sys
import array
import struct

class TinyImages:
    def unpack_key(self,key):
        #keystr = key
        #print >>sys.stderr,"keystr: ", repr(keystr)
        key = struct.unpack('>q',key)[0]
        if key%(3*1024) is not 0:
            print >>sys.stderr,"Warning, unpacking invalid key %i\n"%(key)
        #print >>sys.stderr,"key: ", key
        key = key/(3*1024)
        return key
        
    def unpack_value(self,value):
        im = array.array('B',value)
        red = im[0:1024]
        green = im[1024:2048]
        blue = im[2048:3072]
        
        return (red,green,blue)
        
    def sum_rgb(self,value):
        (red,green,blue) = self.unpack_value(value)
        return (sum(red),sum(green),sum(blue))
        
    def togray(self,value):
        """ Convert an input value in bytes into a floating point grayscale array. """
        (red,green,blue) = self.unpack_value(value)
        
        gray = []
        for i in xrange(1024):
            graypx = (0.299*float(red[i]) + 0.587*float(green[i]) +
                0.114*float(blue[i]))/255.
            gray.append(graypx)
            
        return gray
