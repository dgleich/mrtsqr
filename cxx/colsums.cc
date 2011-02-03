/**
 * @file colsums.cc
 * Check how long it takes just to access the matrix and compute colum sums.
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

class StreamingColumnSums {
public:
    TypedBytesInFile& in;
    TypedBytesOutFile& out;
    
    size_t ncols; // the number of columns of the local matrix
    std::vector<double> colsums;
    size_t totalrows;
    
    std::vector<double> local; // the local matrix
    StreamingColumnSums(TypedBytesInFile& in_, TypedBytesOutFile& out_)
    : in(in_), out(out_), ncols(0), totalrows(0)
    {}
    
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
        totalrows += 1;
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
        hadoop_message("matrix size: %zi ncols\n", ncols);
        colsums = row;
    }
        
    
    // read in a row and add it to the local matrix
    void add_row(const std::vector<double>& row) {    
        assert(row.size() == ncols);
        for (size_t j=0; j<ncols; ++j) {
            colsums[j] += row[j];
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
        }
        hadoop_status("final output");
        output();
    }
    
    void reducer() {
        bool first_key = true;
        bool more_data = false;
        int cur_key = 0;
        double rval = 0.;
        while (!feof(in.stream)) {
            if (in.skip_next() == false) {
                if (feof(in.stream)) {
                    break;
                } else {
                    hadoop_message("invalid key: row %i\n", totalrows);
                    exit(-1);
                }
            }
            TypedBytesType keytype = in.next_type();
            assert(keytype == TypedBytesInteger);
            int key = in.read_int();
            TypedBytesType valtype = in.next_type();
            assert(valtype == TypedBytesDouble);
            double val = in.read_double();
            if (first_key) {
                cur_key = key;
                rval = 0.;
            }
            if (key == cur_key) {
                rval += val;
            } else {
                out.write_int(cur_key);
                out.write_double(rval);
                cur_key = key;
                rval = val;
            }
            more_data = true;
        }
        if (more_data) {
            out.write_int(cur_key);
            out.write_double(rval);
        }
    }
    
    /** Output the column sums.
     */
    void output() {
        for (size_t j=0; j<ncols; ++j) {
            out.write_int((int)j);
            out.write_double((double)colsums[j]);
        }
    }
};


void usage() {
    fprintf(stderr, "usage: colsums map|reduce\n");
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
    
    StreamingColumnSums prog(in, out);

    char* operation = argv[1];
    if (strcmp(operation,"map")==0) {
        prog.mapper();
    } else if (strcmp(operation,"reduce") == 0) {
        prog.reducer();
    } else {
        usage();
    }
    
    return (0);
}
