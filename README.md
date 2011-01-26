Mister TSQR aka Mr. TSQR aka MRTSQR aka MRQR
===========

The QR factorization is a standard matrix factorization used to solve
many problems.  Probably the most famous is linear regression:

    minimize || Ax - b ||,

where _A_ is an _m-by-n_ matrix, and _b_ is an _m-by-1_ vector.
When the number of rows of the matrix _A_ is much larger than
the number of columns, then _A_ is called a _tall-and-skinny_
matrix because of its shape.  

The MrTSQR codes implement a routine to compute a QR factorization
of a tall-and-skinny matrix using Hadoop's implementation of the
MapReduce computational platform (cite Dean).  The underlying
algorithm for this implementation is due to Demmel et al. (cite
communciation avoiding linear algebra).

The codes are written in Python, and use the NumPy library
for the numerical routines.  This introduces a mild-ineffiency
into the code, which we explore by studying three different 
packages to use Hadoop with Python: dumbo, pydoop, and hadoopy.

This package describes the code and experiments used in our 
paper: A tall-and-skinny QR factorization in MapReduce.

Synopsis
--------

### Using Dumbo

    mrtsqr$ hadoop fs -putFromLocal demo/mini.tmat mrtsqr-demo/
    mrtsqr$ dumbo start dumbo/tsqr.py -matrix mrtsqr-demo/mini.tmat
    mrtsqr$ dumbo cat mrtsqr-demo/mini-qrr.mseq | head
 
    mrtsqr$ dumbo fs -putFromLocal demo/mini.rprob mrtsqr-demo/
    mrtsqr$ dumbo start dumbo/regress.py -prob mrtsqr-demo/mini.rprob
    mrtsqr$ dumbo cat mrtsqr-demo/mini.sol | head


