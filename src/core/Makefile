# The three C cores: the ring (arrow_ring), the checkpoint format (pqc)
# and the stream's rules (sp). Each is a library a host language loads,
# and each has its own tests in C.
#
#   make                 the three libraries, into OUT (default: here)
#   make san             every core under AddressSanitizer + UBSan
#   make fuzz            the mutation drivers of pqc and sp
#
# A Python package builds its library into its own directory:
#   make -C src/core OUT=<package directory> libsp
# The Go packages do not use this file: cgo compiles the checked-in
# copies of these sources in each package (src/go/sync_core.py).
CC ?= cc
CFLAGS ?= -std=c11 -O2 -Wall -Wextra -fPIC
SAN = -std=c11 -g -O1 -Wall -Wextra -fsanitize=address,undefined -fno-omit-frame-pointer
# C11 alone hides POSIX (nanosleep, clock_gettime) on glibc; the Go host
# sets the same define for its Linux builds (#cgo linux CFLAGS).
ifeq ($(shell uname -s),Linux)
CFLAGS += -D_POSIX_C_SOURCE=200809L
endif
OUT ?= .

# The libraries are named .dylib on every system: the hosts load them by
# path.
all: libarrow_ring libpqc libsp
libarrow_ring: $(OUT)/libarrow_ring.dylib
libpqc: $(OUT)/libpqc.dylib
libsp: $(OUT)/libsp.dylib

$(OUT)/libarrow_ring.dylib: arrow_ring.c arrow_ring.h report.h
	$(CC) $(CFLAGS) -shared -o $@ arrow_ring.c -lpthread

$(OUT)/libpqc.dylib: pqc.c pqc.h report.h
	$(CC) $(CFLAGS) -shared -o $@ pqc.c

# The catalog (sp_catalog.c) is a SQLite database: the system's SQLite
# library and its header (Ubuntu: libsqlite3-dev).
$(OUT)/libsp.dylib: sp.c sp_catalog.c sp.h report.h
	$(CC) $(CFLAGS) -shared -o $@ sp.c sp_catalog.c -lm -lsqlite3

# ---- AddressSanitizer + UBSan runs (pure C, no Python in the loop) ----
san: san-arrow_ring san-pqc san-sp

san-arrow_ring: arrow_ring.c arrow_ring.h report.h arrow_ring_san_test.c
	$(CC) $(SAN) -o arrow_ring_san_test_bin arrow_ring.c arrow_ring_san_test.c -lpthread
	./arrow_ring_san_test_bin

san-pqc: pqc.c pqc.h report.h pqc_test_calls.h pqc_san_test.c
	$(CC) $(SAN) -o pqc_san_test_bin pqc.c pqc_san_test.c
	./pqc_san_test_bin

san-sp: sp.c sp_catalog.c sp.h report.h sp_test_calls.h sp_san_test.c
	$(CC) $(SAN) -o sp_san_test_bin sp.c sp_catalog.c sp_san_test.c -lm -lsqlite3
	./sp_san_test_bin

# macOS leak check (LeakSanitizer is unavailable on Apple clang)
leaks: arrow_ring.c arrow_ring.h arrow_ring_san_test.c pqc.c pqc.h pqc_san_test.c
	$(CC) -std=c11 -g -O1 -o arrow_ring_san_test_bin arrow_ring.c arrow_ring_san_test.c -lpthread
	leaks --atExit -- ./arrow_ring_san_test_bin | tail -3
	$(CC) -std=c11 -g -O1 -o pqc_san_test_bin pqc.c pqc_san_test.c
	leaks --atExit -- ./pqc_san_test_bin | tail -3

# ---- fuzz: mutation drivers, no libFuzzer runtime needed (macOS) ----
fuzz: fuzz-pqc fuzz-sp

# pqc's reader and footer shift take bytes from the store: real
# checkpoints and footers (pqc_corpus/, made by make_corpus.py), mutated.
fuzz-pqc: pqc.c pqc.h report.h pqc_test_calls.h pqc_fuzz.c
	$(CC) $(SAN) -DFUZZ_STANDALONE -o pqc_fuzz_bin pqc.c pqc_fuzz.c
	python3 ../../tests/parquet_checkpoint/make_corpus.py
	./pqc_fuzz_bin $${FUZZ_ITERS:-200000} pqc_corpus

# sp's ticket reader takes text from the store, and its catalog takes
# parquet footers: real tickets and footers (sp_corpus/), mutated.
fuzz-sp: sp.c sp_catalog.c sp.h report.h sp_test_calls.h sp_fuzz.c
	$(CC) $(SAN) -o sp_fuzz_bin sp.c sp_catalog.c sp_fuzz.c -lm -lsqlite3
	./sp_fuzz_bin $${FUZZ_ITERS:-300000} sp_corpus/flat.footer sp_corpus/nested.footer

# libFuzzer harnesses for pqc's parsers (clang only).
FUZZCC ?= clang
FUZZFLAGS = -std=c11 -g -O1 -fsanitize=address,undefined,fuzzer

fuzz-libfuzzer: pqc.c pqc.h pqc_fuzz.c
	$(FUZZCC) $(FUZZFLAGS) -DFUZZ_READER -o pqc_fuzz_reader pqc.c pqc_fuzz.c
	$(FUZZCC) $(FUZZFLAGS) -DFUZZ_SHIFT -o pqc_fuzz_shift pqc.c pqc_fuzz.c
	mkdir -p pqc_corpus_reader pqc_corpus_shift
	./pqc_fuzz_reader -runs=200000 -max_len=4096 pqc_corpus_reader
	./pqc_fuzz_shift  -runs=200000 -max_len=8192 pqc_corpus_shift

clean:
	rm -f $(OUT)/libarrow_ring.dylib $(OUT)/libpqc.dylib $(OUT)/libsp.dylib *_bin pqc_fuzz_reader pqc_fuzz_shift

.PHONY: all libarrow_ring libpqc libsp san san-arrow_ring san-pqc san-sp leaks fuzz fuzz-pqc fuzz-sp fuzz-libfuzzer clean
