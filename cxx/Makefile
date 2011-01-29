

CC=$(CXX)
CXXFLAGS=-Wall -O3
LDFLAGS=-L/usr/lib64/atlas -lf77blas43 -latlas43

all: tsqr word_count tests

tests: dump_typedbytes_info write_typedbytes_test
	./write_typedbytes_test write.tb
	./dump_typedbytes_info write.tb > test/dump_test.cur
	diff test/dump_test.cur test/dump_test.out
	rm write.tb
	rm test/dump_test.cur

typedbytes.o: typedbytes.h
dump_typedbytes_info.o: typedbytes.h
write_typedbytes_test.o: typedbytes.h
word_count.o: typedbytes.h
tsqr.o: typedbytes.h

tsqr: tsqr.o

word_count: word_count.o typedbytes.o

dump_typedbytes_info: typedbytes.o dump_typedbytes_info.o

write_typedbytes_test: typedbytes.o write_typedbytes_test.o

.PHONY: all clean tests

clean:
	rm -rf typedbytes.o dump_typedbytes_info.o dump_typedbytes_info


