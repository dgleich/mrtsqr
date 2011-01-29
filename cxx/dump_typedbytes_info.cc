/**
 * @file dump_typedbytes_info.cc
 * A test program for the typedbytes reader in C++
 */

/**
 * History
 * -------
 * :2010-01-28: Initial coding
 */

#include <stdio.h>
#include <inttypes.h>

#include "typedbytes.h"

void print_indent(int indent) {
    for (int i=0; i<indent; ++i) {
        printf(" ");
    }
}

void dump_typedbytes_primitive(TypedBytesInFile& f, TypedBytesType t, int indent) {
    std::string s;
    bool printed = false;
    print_indent(indent);
    switch(t) {
        case TypedBytesByteSequence:
            printf("TypedBytesByteSequence: length=%i",
                f.read_byte_sequence_length());
            if (f.length_remaining() == 0) {
                printf("\n");
            }
            // suck up all the data
            while (f.length_remaining() > 0) {
                int64_t tempval=0;
                if (f.length_remaining() >= 8) {
                    f.read_byte_sequence((unsigned char*)&tempval, 
                        (size_t)sizeof(int64_t));
                } else {
                    f.read_byte_sequence((unsigned char*)&tempval, 
                        (size_t)f.length_remaining());
                }
                if (!printed) {
                    unsigned char* buf = (unsigned char*)&tempval;
                    printf(" first 8 bytes: %02x%02x%02x%02x %02x%02x%02x%02x\n",
                        buf[0],buf[1],buf[2],buf[3],buf[4],buf[5],
                        buf[6],buf[7]);
                    printed = true;
                }
            }
            break;
            
        case TypedBytesByte:
            printf("TypedBytesByte: %i\n", (int)f.read_byte());
            break;
            
        case TypedBytesBoolean:
            printf("TypedBytesBoolean: %i\n", (int)f.read_bool());
            break;
            
        case TypedBytesInteger:
            printf("TypedBytesInteger: %i\n", (int)f.read_int());
            break;
            
        case TypedBytesLong:
            printf("TypedBytesLong: %lli\n", (long long)f.read_long());
            break;
            
        case TypedBytesFloat:
            printf("TypedBytesFloat: %f\n", (float)f.read_float());
            break;
            
        case TypedBytesDouble:
            printf("TypedBytesDouble: %lf\n", (double)f.read_double());
            break;
            
        case TypedBytesString:
            if (f.read_string(s)) {
                if (s.size() > 10) {
                    std::string s = s.substr(0,10) + "[...]";
                }
                printf("TypedBytesString: %s\n", s.c_str());
            } else {
                printf("TypedBytesString: ERROR\n");
            }
            break;
            
        default:
            printf("Unknown primitive type: %i\n", t);
            break;
    }
}

void dump_typedbytes_one(TypedBytesInFile& f, int indent) {
    TypedBytesType t = f.next_type();
    if (t == TypedBytesByteSequence || t == TypedBytesByte ||
        t == TypedBytesBoolean || t == TypedBytesInteger || t==TypedBytesLong ||
        t == TypedBytesFloat || t == TypedBytesDouble || 
        t == TypedBytesString) {
        dump_typedbytes_primitive(f, t, indent);
    } else if (t==TypedBytesVector) {
        typedbytes_length len = f.read_typedbytes_sequence_length();
        print_indent(indent);
        printf("TypedBytesVector: length=%i\n",len);
        for (int i=0; i<len; i++) {
            dump_typedbytes_one(f, indent+2);
        }
    } else if(t==TypedBytesMap) {
        typedbytes_length len = f.read_typedbytes_sequence_length();
        print_indent(indent);
        printf("TypedBytesMap: length=%i\n",len);
        for (int i=0; i<len; i++) {
            print_indent(indent+1);
            printf("Key:\n");
            dump_typedbytes_one(f, indent+2);
            print_indent(indent+1);
            printf("Value:\n");
            dump_typedbytes_one(f, indent+2);
        }
    } else if (t==TypedBytesList) {
        print_indent(indent);
        printf("TypedBytesList:\n");
        while (f.lastcode != TypedBytesListEnd) {
            dump_typedbytes_one(f, indent+2);
        }
    } else if (t==TypedBytesListEnd) {
        return;
    }
}
        
void dump_typedbytes_all(TypedBytesInFile& f, int indent) {
    while (!feof(f.stream)) {
        dump_typedbytes_one(f, indent);
    }
}



int main(int argc, char **argv) {
    if (argc != 2) {
        printf("usage: dump_typedbytes_info filename|-\n");
        return (-1);
    }
    FILE *f = NULL;
    if (strcmp(argv[1],"-") == 0) {
        f = stdin;
    } else {
        f = fopen(argv[1], "rb");
        if (!f) {
            printf("cannot open %s\n", argv[1]);
            return (1);
        }
    }
    TypedBytesInFile tbf(f);
    dump_typedbytes_all(tbf, 0);
}
