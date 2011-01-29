#ifndef TYPEDBYTES_H
#define TYPEDBYTES_H

/**
 * @file typedbytes.h
 * The header file and most of the implementation for the
 * typedbytes classes in C++
 */

/**
 * History
 * -------
 * :2010-01-28: Initial coding
 */

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

#if defined(__GNUC__) && __GNUC__ >= 2
#include <byteswap.h>
#define bswap32 bswap_32
#define bswap64 bswap_64
#else
#error no byteswap implemented for your platform yet.
#endif 

#include <string>
#include <vector>

enum TypedBytesType {
    TypedBytesByteSequence = 0,
    TypedBytesByte = 1,
    TypedBytesBoolean = 2,
    TypedBytesInteger = 3,
    TypedBytesLong = 4,
    TypedBytesFloat = 5,
    TypedBytesDouble = 6,
    TypedBytesString = 7,
    TypedBytesVector = 8,
    TypedBytesList = 9,
    TypedBytesMap = 10,
    TypedBytesTypeError = 254, // a sentinal value for errors
    TypedBytesListEnd = 255, // a sentinal value for errors
};

typedef int64_t typedbytes_long;
typedef int32_t typedbytes_length;
typedef std::vector<unsigned char> typedbytes_opaque;

// define this type for asserts on the primitive read operations
#define TYPEDBYTES_STRICT_TYPE

class TypedBytesInFile {
public:    
    FILE* stream;
    TypedBytesType lastcode; // the last typecode read
    
    typedbytes_length lastlength; // the string/byte-seq length read
                                  // (decremented by any reading)
    
    
    TypedBytesInFile(FILE* stream_) 
    : stream(stream_), lastcode(TypedBytesTypeError), lastlength(-1)
    {}
    
    /** Get the next type code as a supported type. */
    TypedBytesType next_type() {
        unsigned char code = next_type_code();
        if (code <= 10 || code == 255) {
            return (TypedBytesType)code;
        } else if (code >= 50 && code <= 200) {
            return TypedBytesByteSequence;
        } else if (code == TypedBytesTypeError) {
            // error flag already set in this case.
            return TypedBytesTypeError;
        } else {
            // TODO set error flag
            return TypedBytesTypeError;
        }
    }
    
    /** Get the next type code as a raw byte.
     * This command is useful if you are seralizing custom types.
     * This command returns TypedBytesTypeError on an error.
     */
    unsigned char next_type_code() {
        int c = fgetc(stream);
        // reset lastlength
        lastlength = -1;
        if (c == EOF) {
            // TODO set error flag
            lastcode = TypedBytesTypeError;
            return (unsigned char)TypedBytesTypeError;
        } else {
            lastcode = (TypedBytesType)c;
            return (unsigned char)c;
        }
    }
    
    /** Read bytes and handle errors.
     * DO NOT call this function directly.
     */
    size_t _read_bytes(void *ptr, size_t nbytes, size_t nelem) {
        size_t nread = fread(ptr, nbytes, nelem, stream);
        if (nread != nelem) {
            // TODO set error flag
            // and determien more intelligent action.
            assert(nelem == nread);
        }
        // reset lastlength
        lastlength = -1;
        return nread;
    }
    
    /** Read a 32-bit integer for the length of a string, vector, or map. */
    int32_t _read_length() {
        int32_t len = 0;
        _read_bytes(&len, sizeof(int32_t), 1);
        len = bswap32(len);
        return len;
    }
    
              
    /** Return the amount of data remaining in a byte-sequence of string */
    typedbytes_length length_remaining() const {
        return lastlength;
    }
    
    bool _read_data_block(unsigned char* data, size_t size) {
        assert(lastlength >= 0);
        typedbytes_length curlen = lastlength;
        if (size <= (size_t)curlen) {
            size_t nread = _read_bytes(
                                data, sizeof(unsigned char), (size_t)size);
            // NOTE _read_bytes resets lastlength, so we have to reset it back
        
            if (nread != size) {
                // TODO update error
                return false;
            } else {
                lastlength = curlen - size;
                assert(lastlength >= 0);
                return true;
            }
        } else {
            return false;
        }
    }
    
#ifdef TYPEDBYTES_STRICT_TYPE
    #define typedbytes_check_type_code(x) (assert((x) == lastcode))
#else
    #define typedbytes_check_type_code(x)
#endif    

    signed char read_byte() {
        typedbytes_check_type_code(TypedBytesByte);
        signed char rval = 0;
        _read_bytes(&rval, sizeof(signed char), 1);
        return (rval);
    }
    
    bool read_bool() {
        typedbytes_check_type_code(TypedBytesBoolean);
        signed char rval = 0;
        _read_bytes(&rval, sizeof(signed char), 1);
        return (bool)rval;
    }
    
    float read_float() {
        typedbytes_check_type_code(TypedBytesFloat);
        int32_t val = 0;
        _read_bytes(&val, sizeof(int32_t), 1);
        val = bswap32(val);
        float rval;
        memcpy(&rval, &val, sizeof(int32_t));
        return rval;
    }
    
    double read_double() {
        typedbytes_check_type_code(TypedBytesDouble);
        int64_t val = 0;
        _read_bytes(&val, sizeof(int64_t), 1);
        val = bswap64(val);
        double rval;
        memcpy(&rval, &val, sizeof(int64_t));
        return rval;
    }
    
    /** Read a byte, bool, int, long, or float and convert to double. */
    double convert_double() {
        if (lastcode == TypedBytesFloat) {
            return (double)read_float();
        } else if (lastcode == TypedBytesDouble) {
            return (double)read_double();
        } else {
            return (double)convert_long();
        }
    }
    
    /** Read a byte, bool, int, or long and convert to long. */
    typedbytes_long convert_long() {
        if (lastcode == TypedBytesLong) {
            return (long)read_long();
        } else {
            return (long)convert_int();
        }
    }
    
    /** Read a byte, bool, int, or long and convert to long. */
    int convert_int() {
        if (lastcode == TypedBytesByte) {
            return (int)read_byte();
        } else if (lastcode == TypedBytesBoolean) {
            return (int)read_bool();
        } else if (lastcode == TypedBytesInteger) {
            return (int)read_int();
        } else {
            assert(lastcode == TypedBytesTypeError);
            return 0;
        }
    }
        
    bool _read_opaque_primitive(typedbytes_opaque& buffer, 
        TypedBytesType typecode);
    bool _read_opaque(typedbytes_opaque& buffer, bool list);
    bool read_opaque(typedbytes_opaque& buffer);
        
    int read_int() {
        typedbytes_check_type_code(TypedBytesInteger);
        int32_t rval = 0;
        _read_bytes(&rval, sizeof(int32_t), 1);
        rval = bswap32(rval);
        return (int)rval;
    }
    
    typedbytes_long read_long() {
        typedbytes_check_type_code(TypedBytesLong);
        int64_t rval = 0;
        _read_bytes(&rval, sizeof(int64_t), 1);
        rval = bswap64(rval);
        return (typedbytes_long)rval;
    }
    
    //
    // sequence types
    //
    
    typedbytes_length read_string_length() {
        typedbytes_check_type_code(TypedBytesString);
        typedbytes_length len = _read_length();
        lastlength = len;
        return len;
    }
    
    /** Must be called after read_string_length 
     * If size < read_string_length(), then you can call
     * this function multiple times sequentially.
     * */
    bool read_string_data(unsigned char* data, size_t size) {
        typedbytes_check_type_code(TypedBytesString);
        return _read_data_block(data, size);
    }
    
    bool read_string(std::string& str) {
        typedbytes_length len = _read_length();
        str.resize(len);
        assert(len >= 0);
        // TODO check for error
        _read_bytes(&str[0], sizeof(unsigned char), (size_t)len);
        return true;
    }
    
    typedbytes_length read_byte_sequence_length() {
#ifdef TYPEDBYTES_STRICT_TYPE        
        if (lastcode == TypedBytesByteSequence || 
            (lastcode >= 50 && lastcode <= 200)) {} // do nothing here
        else { 
            typedbytes_check_type_code(TypedBytesTypeError); }
#endif
        typedbytes_length len = _read_length();
        lastlength = len;
        return len;
    }
    
    /** Must be called after read_byte_sequence_length
     * If size < read_byte_sequence_length(), then you can call
     * this function multiple times sequentially.
     */
    bool read_byte_sequence(unsigned char* data, size_t size) {
#ifdef TYPEDBYTES_STRICT_TYPE        
        if (lastcode == TypedBytesByteSequence || 
            (lastcode >= 50 && lastcode <= 200)) {} // do nothing here
        else { typedbytes_check_type_code(TypedBytesTypeError); }
#endif
        return _read_data_block(data, size);
    }

        
    
    /** The vector and map types are considered sequence types.
     * You are responsible for handling these types yourself.
     */
    typedbytes_length read_typedbytes_sequence_length() {
#ifdef TYPEDBYTES_STRICT_TYPE        
        if (lastcode == TypedBytesVector || lastcode == TypedBytesMap) {} 
        else { typedbytes_check_type_code(TypedBytesTypeError); }
#endif        
        return _read_length();
    }
};

class TypedBytesOutFile {
public:
    FILE* stream;
    
    TypedBytesOutFile(FILE* stream_)
    : stream(stream_)
    {}
    
    bool _write_length(typedbytes_length len) {
        len = bswap32(len);
        if (fwrite(&len, sizeof(typedbytes_length), 1, stream) == 1) {
            return true;
        } else {
            return false;
        }
    }
    
    bool _write_bytes(const void* ptr, size_t nbytes, size_t nelem) {
        if (fwrite(ptr, nbytes, nelem, stream) == nelem) {
            return true;
        } else {
            return false;
        }
    }
    
    bool _write_code(TypedBytesType t) {
        unsigned char code = (unsigned char)t;
        return _write_bytes(&code, 1, 1);
    }
    
    bool write_byte_sequence(unsigned char* bytes, typedbytes_length size) {
        if (_write_code(TypedBytesByteSequence)) {
            if (_write_length(size)) {
                if (_write_bytes(bytes, sizeof(unsigned char), (size_t)size)) {
                    return true;
                }
            }
        }
        return false;
    }
    
    bool write_byte(signed char byte) {
        if (_write_code(TypedBytesByte)) {
            if (_write_bytes(&byte, 1, 1)) {
                return true;
            }
        }
        return false;
    }
    
    bool write_bool(bool val) {
        signed char sval=0;
        if (val) { sval = 1; }
        if (_write_code(TypedBytesBoolean)) {
            if (_write_bytes(&sval, 1, 1)) {
                return true;
            }
        }
        return false;
    }
    
    bool write_int(int val) {
        int32_t sval = bswap32(val);
        if (_write_code(TypedBytesInteger)) {
            if (_write_bytes(&sval, sizeof(int32_t), 1)) {
                return true;
            }
        }
        return false;
    }
    
    bool write_long(typedbytes_long val) {
        val = bswap64(val);
        if (_write_code(TypedBytesLong)) {
            if (_write_bytes(&val, sizeof(typedbytes_long), 1)) {
                return true;
            }
        }
        return false;
    }
    
    bool write_float(float val) {
        int32_t sval = 0;
        memcpy(&sval, &val, sizeof(int32_t));
        sval = bswap32(sval);
        if (_write_code(TypedBytesFloat)) {
            if (_write_bytes(&sval, sizeof(int32_t), 1)) {
                return true;
            }
        }
        return false;
    }
    
    bool write_double(double val) {
        int64_t sval = 0;
        memcpy(&sval, &val, sizeof(int64_t));
        sval = bswap64(sval);
        if (_write_code(TypedBytesDouble)) {
            if (_write_bytes(&sval, sizeof(int64_t), 1)) {
                return true;
            }
        }
        return false;
    }
    
    bool write_string(const char* str, typedbytes_length size) {
        if (_write_code(TypedBytesString)) {
            if (_write_length(size)) {
                if (_write_bytes(str, sizeof(unsigned char), (size_t)size)) {
                    return true;
                }
            }
        }
        return false;
    }
    
    bool write_string_stl(std::string& str) {
        return write_string(str.c_str(), str.size());
    }
    
    bool write_list_start() {
        if (_write_code(TypedBytesList)) {
            return true;
        } 
        return false;
    }
    
    bool write_list_end() {
        if (_write_code(TypedBytesListEnd)) {
            return true;
        } 
        return false;
    }
    
    /** This function just writes the start of the map code.
     * You are responsible for ensuring subsequent output is correct.
     */
    bool write_map_start(typedbytes_length size) {
        if (_write_code(TypedBytesMap)) {
            if (_write_length(size)) {
                return true;
            }
        }
        return false;
    }
    
    bool write_vector_start(typedbytes_length size) {
        if (_write_code(TypedBytesVector)) {
            if (_write_length(size)) {
                return true;
            }
        }
        return false;
    }
        
    /** Write out opaque typedbytes data direct to the stream. 
     * This is just a high level wrapper around fwrite to the stream.
     * */
    bool write_opaque_type(unsigned char* bytes, size_t size) {
        if (_write_bytes(bytes, 1, size)) {
            return true;
        } 
        return false;
    }
    
};
        
#endif
