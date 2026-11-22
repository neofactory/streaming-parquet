CC ?= cc
CFLAGS ?= -std=c11 -O2 -Wall -Wextra -fPIC
# C11 alone hides POSIX (nanosleep, clock_gettime) on glibc; the Go host
# sets the same define for its Linux builds (#cgo linux CFLAGS).
ifeq ($(shell uname -s),Linux)
CFLAGS += -D_POSIX_C_SOURCE=200809L
endif

libarrow_ring.dylib: arrow_ring.c arrow_ring.h
	$(CC) $(CFLAGS) -shared -o $@ arrow_ring.c -lpthread

# AddressSanitizer + UBSan soak run (pure C, no Python in the loop)
san: arrow_ring.c arrow_ring.h san_test.c
	$(CC) -std=c11 -g -O1 -Wall -Wextra -fsanitize=address,undefined -fno-omit-frame-pointer -o san_test_bin arrow_ring.c san_test.c -lpthread
	./san_test_bin

# macOS leak check (LeakSanitizer is unavailable on Apple clang)
leaks: arrow_ring.c arrow_ring.h san_test.c
	$(CC) -std=c11 -g -O1 -o san_test_bin arrow_ring.c san_test.c -lpthread
	leaks --atExit -- ./san_test_bin | tail -3

clean:
	rm -f libarrow_ring.dylib san_test_bin
