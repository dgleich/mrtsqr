/**
 * @file typedbytes.cc
 * An implementation of a few of the more complicated typedbytes functions.
 */

#include "typedbytes.h"

static inline void push_opaque_typecode(
    typedbytes_opaque& buffer, TypedBytesType code) 
{
    buffer.push_back((unsigned char)code);
}

static inline void push_opaque_bytes(
    typedbytes_opaque& buffer, unsigned char* bytes, size_t size)
{
    while (size > 0) {
        buffer.push_back(*bytes);
        bytes++;
        size--;
    }
}
    
static inline void push_opaque_length(
    typedbytes_opaque& buffer, typedbytes_length len) 
{
    len = bswap32(len);
    push_opaque_bytes(buffer, (unsigned char*)&len, sizeof(typedbytes_length));
}
    

bool TypedBytesInFile::_read_opaque_primitive(typedbytes_opaque& buffer, 
                                                TypedBytesType t) 
{
    // TODO check the fread commands in this function
    unsigned char bytebuf = 0;
    int32_t intbuf = 0;
    int64_t longbuf = 0;
    typedbytes_length len = 0;
    
    // NOTE the typecode has already been pushed
    
    // translate this type to avoid nastiness in the switch.
    if (t >= 50 && t <= 200) {
        t = TypedBytesByteSequence;
    }
    switch (t) {
        case TypedBytesByte:
        case TypedBytesBoolean:
            fread(&bytebuf, sizeof(unsigned char), 1, stream);
            push_opaque_bytes(buffer, &bytebuf, sizeof(unsigned char));
            break;
            
        case TypedBytesInteger:
        case TypedBytesFloat:
            fread(&intbuf, sizeof(int32_t), 1, stream);
            push_opaque_bytes(buffer, (unsigned char*)&intbuf, sizeof(int32_t));
            break;
            
        case TypedBytesLong:
        case TypedBytesDouble:
            fread(&longbuf, sizeof(int64_t), 1, stream);
            push_opaque_bytes(buffer, (unsigned char*)&longbuf, sizeof(int64_t));
            break;
            
        case TypedBytesString:
        case TypedBytesByteSequence:
            len = _read_length();
            while (len > 0) {
                // stream to buffer in longbuf bytes at a time.
                if (len >= 8) {
                    fread(&longbuf, sizeof(int64_t), 1, stream);
                    push_opaque_bytes(buffer, 
                        (unsigned char*)&longbuf, sizeof(int64_t));
                } else {
                    fread(&longbuf, len, 1, stream);
                    push_opaque_bytes(buffer, 
                        (unsigned char*)&longbuf, len);
                }
            }
            break;
            
        default:
            return false;
    }
    return true;
}

bool TypedBytesInFile::_read_opaque(typedbytes_opaque& buffer, bool list) {
    TypedBytesType t = next_type();
    push_opaque_typecode(buffer, t);
    if (t == TypedBytesByteSequence || t == TypedBytesByte ||
        t == TypedBytesBoolean || t == TypedBytesInteger || t==TypedBytesLong ||
        t == TypedBytesFloat || t == TypedBytesDouble || 
        t == TypedBytesString || (t >= 50 && t <= 200)) {
        _read_opaque_primitive(buffer, t);
    } else if (t==TypedBytesVector) {
        typedbytes_length len = read_typedbytes_sequence_length();
        push_opaque_length(buffer, len);
        for (int i=0; i<len; i++) {
            _read_opaque(buffer,false);
        }
    } else if(t==TypedBytesMap) {
        typedbytes_length len = read_typedbytes_sequence_length();
        push_opaque_length(buffer, len);
        for (int i=0; i<len; i++) {
            _read_opaque(buffer,false);
            _read_opaque(buffer,false);
        }
    } else if (t==TypedBytesList) {
        while (lastcode != TypedBytesListEnd) {
            _read_opaque(buffer, true);
        }
    } else if (list && t==TypedBytesListEnd) {
        return true;
    } else {
        return false;
    }
    return true;
}

bool TypedBytesInFile::read_opaque(typedbytes_opaque& buffer) {
    return _read_opaque(buffer, false);
}


bool TypedBytesInFile::skip_next() {
    // TODO, rewrite these functions to avoid loading into memory
    typedbytes_opaque value;
    return read_opaque(value);
}
