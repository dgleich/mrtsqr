/**
 * @file word_count.cc
 * Implement a working wordcount in C++ using hadoop streaming with typedbytes.
 * @author David F. Gleich
 */

/**
 * History
 * -------
 * :2011-01-28: Initial coding
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <string>
#include <sstream>
#include <vector>

#include "typedbytes.h"

/** Split a string.  
 * Taken from 
 * http://stackoverflow.com/questions/236129/c-how-to-split-a-string
 */
std::vector<std::string>& split(
    const std::string &s, char delim, std::vector<std::string> &elems) {
    std::stringstream ss(s);
    std::string item;
    while(std::getline(ss, item, delim)) {
        elems.push_back(item);
    }
    return elems;
}

/**
 * This function only works with 
 * Key: TypedBytesLong|TypedBytesInt|TypedBytesByte
 * Value: TypedBytesString
 * types.
 */
void mapper(TypedBytesInFile& in, TypedBytesOutFile& out) {
    fprintf(stderr, "starting mapper...\n");
    std::string value;
    while (!feof(in.stream)) {
        // read the key
        TypedBytesType keycode = in.next_type();
        if (keycode == TypedBytesTypeError) {
            if (feof(in.stream)) {
                return;
            }
            else {
                fprintf(stderr, "!eof but typeerror=%i\n", in.lastcode);
                return;
            }
        }
        
        assert(keycode == TypedBytesLong);
        in.convert_long(); // ignore the key
        
        // read the value
        TypedBytesType valcode = in.next_type();
        assert(valcode = TypedBytesString);
        in.read_string(value);
    
        // parse the value
        std::vector<std::string> parts;
        split(value, ' ', parts);
        for (size_t i=0; i<parts.size(); ++i) {
            out.write_string_stl(parts[i]);
            out.write_int(1);
        }
    }
}

/**
 * Key: TypedBytesString
 * Value: TypedBytesLong
 */
void reducer(TypedBytesInFile& in, TypedBytesOutFile& out) {
    std::string curkey;
    std::string nextkey;
    if (feof(in.stream)) {
        return;
    }
    
    in.next_type();
    in.read_string(curkey);
    
    in.next_type();
    int64_t value = in.convert_long();
    
    int64_t reduce_val = value;
    
    while (!feof(in.stream)) {
        TypedBytesType keytype = in.next_type();
        if (keytype==TypedBytesTypeError) {
            if (feof(in.stream)) {
                return;
            }
            else {
                fprintf(stderr, "!eof but typeerror=%i\n", in.lastcode);
                return;
            }
        }
        
        in.read_string(nextkey);
        
        if (curkey != nextkey) {
            out.write_string_stl(curkey);
            out.write_long(reduce_val);
            curkey = nextkey;
            reduce_val = 0;
        }
        
        in.next_type();
        reduce_val += in.convert_long();
    }
    out.write_string_stl(curkey);
    out.write_long(reduce_val);
}

void usage() {
    fprintf(stderr, "usage: word_count [map|reduce]\n");
    exit(-1);
}

int main(int argc, char** argv) 
{
    // create typed bytes files
    TypedBytesInFile in(stdin);
    TypedBytesOutFile out(stdout);
    
    fprintf(stderr, "started program\n");
    
    if (argc < 2) {
        usage();
    }
    
    char* operation = argv[1];
    if (strcmp(operation,"map")==0) {
        mapper(in, out);
    } else if (strcmp(operation,"reduce") == 0) {
        reducer(in, out);
    } else {
        usage();
    }
    
    return (0);
}
