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
#include <stdarg.h>
#include <string>

#include <vector>

#include "typedbytes.h"
#include "sparfun_util.h"

/** Write a message to stderr
 */
void hadoop_message(const char* format, ...) {
    va_list args;
    va_start(args, format);
    vfprintf(stderr, format, args);
    va_end (args);
}

/** Write a status message to hadoop.
 *
 */
void hadoop_status(const char* format, ...) {
    va_list args;
    va_start (args, format);
    
    // output for hadoop
    fprintf(stderr, "reporter:status:");
    vfprintf(stderr, format, args);
    fprintf(stderr, "\n");
    
    // also print to stderr
    fprintf(stderr, "status: ");
    vfprintf(stderr, format, args);
    fprintf(stderr, "\n");
    
    va_end (args);
}

void hadoop_counter(const char* name, int val) {
    fprintf(stderr, "reporter:counter:Program,%s,%i\n", name, val);
}

extern "C" {
void dgeqrf_(int *m, int *n, double *a, int *lda, double *tau,
    double *work, int *lwork, int *info);
}    

/** Run a LAPACK qr with local memory allocation.
 * @param nrows the number of rows of A allocated
 * @param ncols the number of columns of A allocated
 * @param urows the number of rows of A used.
 * In LAPACK parlance, nrows is the stride, and urows is
 * the size
 */
bool lapack_qr(double* A, size_t nrows, size_t ncols, size_t urows)
{
    int info = -1;
    int n = ncols;
    int m = urows;
    int stride = nrows;
    int minsize = std::min(urows,ncols);
    
    // allocate space for tau's
    std::vector<double> tau(minsize);
    
    // do a workspace query
    double worksize;
    int lworkq = -1;
    
    
    dgeqrf_(&m, &n, A, &stride, &tau[0], &worksize, &lworkq, &info);
    if (info == 0) {
        int lwork = (int)worksize;
        std::vector<double> work(lwork);
        dgeqrf_(&m, &n, A, &stride, &tau[0], &work[0], &lwork, &info);
        if (info == 0) {
            // zero out the lower triangle of A
            size_t rsize = (size_t)minsize;
            for (size_t j=0; j<rsize; ++j) {
                for (size_t i=j+1; i<rsize; ++i) {
                    A[i+j*nrows] = 0.;
                }
            }
            return true;
        } else {
            return false;
        }
    } else {
        return false;
    }
    
}

class SerialTSQR {
public:
    TypedBytesInFile& in;
    TypedBytesOutFile& out;
    
    size_t blocksize;
    size_t ncols; // the number of columns of the local matrix
    size_t nrows; // the maximum number of rows of the local matrix
    size_t currows; // the current number of local rows
    size_t totalrows; // the total number of rows processed
    
    double tempval;
    
    std::vector<double> local; // the local matrix
    SerialTSQR(TypedBytesInFile& in_, TypedBytesOutFile& out_,
        size_t blocksize_)
    : in(in_), out(out_), 
      blocksize(blocksize_), ncols(0), nrows(0), totalrows(0),
      tempval(0.)
    {}
    
    /** Allocate the local matrix and set to zero
     * @param nr the number of rows
     * @param nc the number of cols
     */
    void alloc(size_t nr, size_t nc) {
        local.resize(nr*nc);
        for (size_t i = 0; i<nr*nc; ++i) {
            local[i] = 0.;
        }
        nrows = nr;
        ncols = nc;
        currows = 0;
    }
    
    void read_full_row(std::vector<double>& row)
    {
        TypedBytesType code=in.next_type();
        row.clear();
        if (code == TypedBytesVector) {
            typedbytes_length len=in.read_typedbytes_sequence_length();
            row.reserve((size_t)len);
            for (size_t i=0; i<(size_t)len; ++i) {
                TypedBytesType nexttype=in.next_type();
                if (in.can_be_double(nexttype)) {
                    row.push_back(in.convert_double());
                } else {
                    fprintf(stderr, 
                        "error: row %zi, col %zi has a non-double-convertable type\n",
                        totalrows, row.size());
                    exit(-1);
                }
            }
        } else if (code == TypedBytesList) {
            TypedBytesType nexttype=in.next_type();
            while (nexttype != TypedBytesListEnd) {
                if (in.can_be_double(nexttype)) {
                    row.push_back(in.convert_double());
                } else {
                    fprintf(stderr, 
                        "error: row %zi, col %zi has a non-double-convertable type\n",
                        totalrows, row.size());
                    exit(-1);
                }
                nexttype=in.next_type();
            }
        } else {
            fprintf(stderr,
                "error: row %zi is a not a list or vector\n", totalrows);
            exit(-1);
        }
    }
    
    /** Handle the first input row.
     * The first row of the input is special, and so we handle
     * it differently.
     */
    void first_row()
    {
        in.skip_next(); // skip the key
        std::vector<double> row;
        read_full_row(row);
        ncols = row.size();
        hadoop_message("matrix size: %zi ncols, up to %i localrows\n", 
            ncols, blocksize*ncols);
        alloc(blocksize*ncols, ncols);
        add_row(row);
    }
        
    
    // read in a row and add it to the local matrix
    void add_row(const std::vector<double>& row) {    
        assert(row.size() == ncols);
        assert(currows < nrows);
        // store by column
        for (size_t j=0; j<ncols; ++j) {
            local[currows + j*nrows] = row[j];
        }
        // increment the number of local rows
        currows++;
        totalrows++;
    }
    
    // compress the local QR factorization
    void compress() {
        // compute a QR factorization
        double t0 = sf_time();
        if (lapack_qr(&local[0], nrows, ncols, currows)) {
            double dt = sf_time() - t0;
            hadoop_counter("lapack time (millisecs)", (int)(dt*1000.));
        } else {
            hadoop_message("lapack error\n");
            exit(-1);
        }
        if (ncols < currows) {
            currows = ncols;
        }
    }
    
    void mapper() {
        std::vector<double> row;
        first_row(); // handle the first row
        while (!feof(in.stream)) {
            if (in.skip_next() == false) {
                if (feof(in.stream)) {
                    break;
                } else {
                    hadoop_message("invalid key: row %i\n", totalrows);
                    exit(-1);
                }
            }
            read_full_row(row);
            add_row(row);
            
            if (currows == nrows) {
                compress();
                hadoop_counter("compress", 1);
            }
        }
        compress();
        hadoop_status("final output");
        output();
    }
    
    /** Output the matrix with random keys for the rows.
     */
    void output() {
        for (size_t i=0; i<currows; ++i) {
            int rand_int = sf_randint(0,2000000000);
            out.write_int(rand_int);
            //out.write_vector_start(ncols);
            out.write_list_start();
            for (size_t j=0; j<ncols; ++j) {
                out.write_double(local[i+j*nrows]);
            }
            out.write_list_end();
        }
    }
    
};

void raw_mapper(TypedBytesInFile& in, TypedBytesOutFile& out, size_t blocksize)
{
    SerialTSQR map(in, out, blocksize);
    map.mapper();
}

void usage() {
    fprintf(stderr, "usage: tsqr map|reduce [blocksize]\n");
    exit(-1);
}

int main(int argc, char** argv) 
{  
    // initialize the random number generator
    unsigned long seed = sf_randseed();
    hadoop_message("seed = %u\n", seed);
    
    // create typed bytes files
    TypedBytesInFile in(stdin);
    TypedBytesOutFile out(stdout);
    
    if (argc < 2) {
        usage();
    }
    
    size_t blocksize = 3;
    if (argc > 2) {
        int bs = atoi(argv[2]);
        if (bs <= 0) {
            fprintf(stderr, 
              "Error: blocksize \'%s\' is not a positive integer\n",
              argv[2]);
        }
        blocksize = (size_t)bs;
    }
    
    char* operation = argv[1];
    if (strcmp(operation,"map")==0) {
        raw_mapper(in, out, blocksize);
    } else if (strcmp(operation,"reduce") == 0) {
        raw_mapper(in, out, blocksize);
    } else {
        usage();
    }
    
    return (0);
}
