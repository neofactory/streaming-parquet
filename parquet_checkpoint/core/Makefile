CC ?= cc
CFLAGS ?= -std=c11 -O2 -Wall -Wextra -fPIC
# C11 alone hides POSIX (nanosleep, clock_gettime) on glibc; the Go host
# sets the same define for its Linux builds (#cgo linux CFLAGS).
ifeq ($(shell uname -s),Linux)
CFLAGS += -D_POSIX_C_SOURCE=200809L
endif

libpqc.dylib: pqc.c pqc.h
	$(CC) $(CFLAGS) -shared -o $@ pqc.c

# AddressSanitizer + UBSan run (pure C, no Python in the loop)
san: pqc.c pqc.h san_test.c
	$(CC) -std=c11 -g -O1 -Wall -Wextra -fsanitize=address,undefined -fno-omit-frame-pointer -o san_test_bin pqc.c san_test.c
	./san_test_bin

# macOS leak check (LeakSanitizer is unavailable on Apple clang)
leaks: pqc.c pqc.h san_test.c
	$(CC) -std=c11 -g -O1 -o san_test_bin pqc.c san_test.c
	leaks --atExit -- ./san_test_bin | tail -3

clean:
	rm -f libpqc.dylib san_test_bin

# libFuzzer harnesses for the untrusted-input parsers (clang only).
FUZZCC ?= clang
FUZZFLAGS = -std=c11 -g -O1 -fsanitize=address,undefined,fuzzer

fuzz: fuzz_reader fuzz_shift

fuzz_reader: pqc.c pqc.h fuzz.c
	$(FUZZCC) $(FUZZFLAGS) -DFUZZ_READER -o fuzz_reader pqc.c fuzz.c

fuzz_shift: pqc.c pqc.h fuzz.c
	$(FUZZCC) $(FUZZFLAGS) -DFUZZ_SHIFT -o fuzz_shift pqc.c fuzz.c

fuzz-smoke: fuzz
	mkdir -p corpus_reader corpus_shift
	./fuzz_reader -runs=200000 -max_len=4096 corpus_reader
	./fuzz_shift  -runs=200000 -max_len=8192 corpus_shift

# standalone mutation driver: no libFuzzer runtime needed (macOS)
fuzz-standln: pqc.c pqc.h fuzz.c
	cc -std=c11 -g -O1 -fsanitize=address,undefined -DFUZZ_STANDALONE -o fuzz_standln pqc.c fuzz.c
	python3 ../make_corpus.py
	./fuzz_standln $${FUZZ_ITERS:-200000} corpus
