CC ?= cc
CFLAGS ?= -std=c11 -O2 -Wall -Wextra -fPIC

libsp.dylib: sp.c sp.h
	$(CC) $(CFLAGS) -shared -o $@ sp.c -lm

# AddressSanitizer + UBSan run (pure C, no Python in the loop)
san: sp.c sp.h san_test.c
	$(CC) -std=c11 -g -O1 -Wall -Wextra -fsanitize=address,undefined -fno-omit-frame-pointer -o san_test_bin sp.c san_test.c -lm
	./san_test_bin

# The ticket reader takes text from the store: mutate real tickets and
# read each, under the sanitizers. No libFuzzer runtime needed (macOS).
fuzz: sp.c sp.h fuzz.c
	$(CC) -std=c11 -g -O1 -Wall -Wextra -fsanitize=address,undefined -fno-omit-frame-pointer -o fuzz_bin sp.c fuzz.c -lm
	./fuzz_bin $${FUZZ_ITERS:-300000}

clean:
	rm -f libsp.dylib san_test_bin fuzz_bin
