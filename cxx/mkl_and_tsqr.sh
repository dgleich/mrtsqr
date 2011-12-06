#!/bin/bash -l
# This is the script used to run the C++ script on NERSC machines
# usage: ./mkl_and_tsqr map|reduce [blocksize]
module load mkl && chmod a+rwx tsqr
./tsqr $@

