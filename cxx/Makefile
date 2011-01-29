

CC=$(CXX)
CXXFLAGS=-Wall -O3

all: tests

tests: dump_typedbytes_info write_typedbytes_test
	./write_typedbytes_test write.tb
	./dump_typedbytes_info write.tb

typedbytes.o: typedbytes.h
dump_typedbytes_info.o: typedbytes.h
write_typedbytes_test.o: typedbytes.h

dump_typedbytes_info: typedbytes.o dump_typedbytes_info.o

write_typedbytes_test: typedbytes.o write_typedbytes_test.o

.PHONY: all clean tests

clean:
	rm -rf typedbytes.o dump_typedbytes_info.o dump_typedbytes_info


