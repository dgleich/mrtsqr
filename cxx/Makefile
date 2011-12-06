
CXX=g++44

CC=$(CXX)
CXXFLAGS=-Wall -O3  -std=c++0x
LDFLAGS=-L/usr/lib64/atlas -llapack43 -lf77blas43 -latlas43

# note: NERSC machines need mkl and gcc modules
NERSC := $(shell uname -r)
ifneq (, $(filter %nersc,$(NERSC)))
  CXX=g++
  LDFLAGS=$(MKL) -lpthread
endif

all: tsqr word_count colsums tests

tests: dump_typedbytes_info write_typedbytes_test
	./write_typedbytes_test write.tb
	./dump_typedbytes_info write.tb > test/dump_test.cur
	diff test/dump_test.cur test/dump_test.out
	rm write.tb
	rm test/dump_test.cur

test_tsqr: tsqr 
	cat test/test.mseq | ./tsqr map 2> /dev/null | ./tsqr reduce &> /dev/null

typedbytes.o: typedbytes.h
dump_typedbytes_info.o: typedbytes.h
write_typedbytes_test.o: typedbytes.h
word_count.o: typedbytes.h

colsums.o: typedbytes.h
tsqr.o: typedbytes.h sparfun_util.h
sf_util.o: sparfun_util.h

colsums: colsums.o typedbytes.o

tsqr: tsqr.o typedbytes.o sf_util.o

word_count: word_count.o typedbytes.o

dump_typedbytes_info: typedbytes.o dump_typedbytes_info.o

write_typedbytes_test: typedbytes.o write_typedbytes_test.o

.PHONY: all clean tests test_tsqr

clean:
	rm -rf typedbytes.o dump_typedbytes_info.o dump_typedbytes_info


