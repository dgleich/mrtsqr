/**
 * @file write_typedbytes_test.cc
 * A test program for the typedbytes writer in C++
 */

/**
 * History
 * -------
 * :2010-01-28: Initial coding
 */

#include <stdio.h>
#include <math.h>

#include <string>

#include "typedbytes.h"


int main(int argc, char **argv) {
    if (argc != 2) {
        printf("usage: write_typedbytes_test filename\n");
        return (-1);
    }
    FILE *f = fopen(argv[1], "wb");
    if (!f) {
        printf("cannot open %s\n", argv[1]);
        return (1);
    }
    TypedBytesOutFile tbf(f);
    
    int byteseq = 0xdeadbeef;
    tbf.write_byte_sequence((unsigned char*)&byteseq, sizeof(int));
    char longbyteseq[10] = "abcdefghi";
    tbf.write_byte_sequence((unsigned char*)&longbyteseq, 10);
    
    tbf.write_byte(0);
    tbf.write_byte(-1);
    tbf.write_byte(1);
    
    tbf.write_bool(false);
    tbf.write_bool(true);
    
    tbf.write_int(0);
    tbf.write_int(-1);
    tbf.write_int(1);
    
    tbf.write_long(0);
    tbf.write_long(-1);
    tbf.write_long(1);
    
    tbf.write_float(0.f);
    tbf.write_float(-1.f);
    tbf.write_float(1.f);
    tbf.write_float(1.5f);
    
    tbf.write_double(0.);
    tbf.write_double(-1.);
    tbf.write_double(1.);
    tbf.write_double(1.5);
    
    std::string str("hi there");
    tbf.write_string_stl(str);
    str = "hi\nthere";
    tbf.write_string_stl(str);
    str = "";
    tbf.write_string_stl(str);
    
    tbf.write_list_start();
    tbf.write_double(1.);
    tbf.write_double(2.);
    tbf.write_double(-0.);
    tbf.write_list_end();
    
    tbf.write_map_start(2);
    tbf.write_string("hi",2);
    tbf.write_int(1);
    tbf.write_string("there",5);
    tbf.write_int(2);
    
    tbf.write_vector_start(3);
    tbf.write_string("All",3);
    tbf.write_string("good",4);
    tbf.write_string("men",3);
    
    tbf.write_vector_start(3);
    tbf.write_bool(false);
    tbf.write_int(5);
    tbf.write_double(M_PI);
    
}
