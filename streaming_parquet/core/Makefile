CC ?= cc
CFLAGS ?= -std=c11 -O2 -Wall -Wextra -fPIC

# The catalog (sp_catalog.c) is a SQLite database: the system's SQLite
# library and its header (Ubuntu: libsqlite3-dev).
libsp.dylib: sp.c sp_catalog.c sp.h
	$(CC) $(CFLAGS) -shared -o $@ sp.c sp_catalog.c -lm -lsqlite3

# AddressSanitizer + UBSan run (pure C, no Python in the loop)
san: sp.c sp_catalog.c sp.h san_test.c
	$(CC) -std=c11 -g -O1 -Wall -Wextra -fsanitize=address,undefined -fno-omit-frame-pointer -o san_test_bin sp.c sp_catalog.c san_test.c -lm -lsqlite3
	./san_test_bin

# The ticket reader takes text from the store: mutate real tickets and
# read each, under the sanitizers. No libFuzzer runtime needed (macOS).
fuzz: sp.c sp_catalog.c sp.h fuzz.c
	$(CC) -std=c11 -g -O1 -Wall -Wextra -fsanitize=address,undefined -fno-omit-frame-pointer -o fuzz_bin sp.c sp_catalog.c fuzz.c -lm -lsqlite3
	./fuzz_bin $${FUZZ_ITERS:-300000} corpus/flat.footer corpus/nested.footer

clean:
	rm -f libsp.dylib san_test_bin fuzz_bin
