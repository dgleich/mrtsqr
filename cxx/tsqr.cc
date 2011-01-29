/**
 * @file tsqr.cc
 * Implement TSQR in C++ using atlas and hadoop streaming with typedbytes.
 * @author David F. Gleich
 */

/**
 * History
 * -------
 * :2011-01-28: Initial coding
 */

#include <stdio.h>
#include <stdlib.h>
#include <string>

#include <vector>

#include "typedbytes.h"

void mapper(TypedBytesInFile& in) {
    
}

void usage() {
    fprintf(stderr, "usage: tsqr [map|reduce]\n");
    exit(-1);
}
extern "C" {
double dnrm2_(int*, double*, int*);
}

int main(int argc, char** argv) 
{
    // try blas
    std::vector<double> vec(5);
    vec.push_back(1.0);
    vec.push_back(2.0);
    vec.push_back(3.0);
    vec.push_back(4.0);
    vec.push_back(5.0);
    int vecsize = vec.size();
    int ione = 1;
    double norm = dnrm2_(&vecsize, &vec[0], &ione);
    printf("norm = %lf\n", norm);
    return (0);
    
    // create typed bytes files
    TypedBytesInFile in(stdin);
    TypedBytesOutFile out(stdout);
    
    if (argc < 2) {
        usage();
    }
    
    char* operation = argv[1];
    if (strcmp(operation,"map")==0) {
        // mapper
    } else if (strcmp(operation,"reduce") == 0) {
        // reducer
    } else {
        usage();
    }
    
    return (0);
}
