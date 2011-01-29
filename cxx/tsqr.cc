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
        // TEMP
        // just sum squared
        // for all columns
        for (size_t j = 0; j<ncols; ++j) {
            for (size_t i=0; i<currows; ++i) {
                tempval += local[i + j*nrows]*local[i + j*nrows];
            }
        }
        currows = 0;
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
    
    // output 
    void output() {
        // TEMP
        // just write total value
        out.write_long(0);
        out.write_double(tempval);
    }
    
};

void raw_mapper(TypedBytesInFile& in, TypedBytesOutFile& out) 
{
    SerialTSQR map(in, out, 3);
    map.mapper();
}

void usage() {
    fprintf(stderr, "usage: tsqr [map|reduce]\n");
    exit(-1);
}

int main(int argc, char** argv) 
{  
    // create typed bytes files
    TypedBytesInFile in(stdin);
    TypedBytesOutFile out(stdout);
    
    if (argc < 2) {
        usage();
    }
    
    char* operation = argv[1];
    if (strcmp(operation,"map")==0) {
        raw_mapper(in, out);
    } else if (strcmp(operation,"reduce") == 0) {
        // reducer
    } else {
        usage();
    }
    
    return (0);
}
