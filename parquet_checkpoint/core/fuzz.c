/* fuzz.c -- fuzz harnesses for the two untrusted-input parsers.
 *
 * The contract: for ANY input the parser returns cleanly (parsed
 * handle or loud NULL) -- never a crash, OOB read, or UB. ASan+UBSan
 * are the oracle.
 *
 * Two build modes (libFuzzer runtime is absent from Apple clang):
 *   make fuzz          -- libFuzzer (clang w/ the runtime; CI/Linux)
 *   make fuzz-standln  -- STANDALONE seeded mutation driver, plain
 *                         clang+ASan, runs everywhere incl. macOS:
 *                         ./fuzz_reader_s <iters> [seed]
 *                         starts from valid artifacts (built in-proc),
 *                         mutates them millions of times.
 */
#include "pqc.h"
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>

#ifdef FUZZ_READER
int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  char err[512];
  PqcReader *r = pqc_reader_open(data, size, err, sizeof err);
  if (r) {
    /* exercise every accessor on a "valid" (fuzz-accepted) object */
    (void)pqc_reader_name(r);
    (void)pqc_reader_watermark(r);
    uint32_t nc = pqc_reader_n_chunks(r);
    for (uint32_t i = 0; i < nc; i++) {
      const char *nm; uint64_t len; uint32_t crc;
      pqc_reader_chunk(r, i, &nm, &len, &crc);
    }
    uint64_t off, dl;
    pqc_reader_data(r, &off, &dl);
    pqc_reader_free(r);
  }
  return 0;
}
#endif

#ifdef FUZZ_SHIFT
int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  char err[512];
  size_t out_len = 0;
  /* deltas spanning the sign and the rebase range */
  const int64_t deltas[] = { 0, 1, -1, 4, 1000, (int64_t)1 << 40,
                             -((int64_t)1 << 40) };
  for (size_t i = 0; i < sizeof(deltas)/sizeof(deltas[0]); i++) {
    uint8_t *out = pqc_shift_footer(data, size, deltas[i], &out_len,
                                    err, sizeof err);
    if (out) pqc_free(out);
  }
  return 0;
}
#endif


#ifdef FUZZ_STANDALONE
/* ---- self-contained mutation driver (no libFuzzer) ----
 * Seeds are REAL checkpoint/footer files in a corpus dir (written by
 * run_c_compat.py, which has a parquet encoder); mutation explores the
 * accept boundary. Usage: ./fuzz_standln <iters> <corpus_dir> [seed] */
#include <stdio.h>
#include <string.h>
#include <dirent.h>

static uint64_t s_rng;
static uint32_t xr(void) {
  s_rng ^= s_rng << 13; s_rng ^= s_rng >> 7; s_rng ^= s_rng << 17;
  return (uint32_t)(s_rng >> 11);
}

static uint8_t **g_seed; static size_t *g_slen; static int g_n;

static void load_corpus(const char *dir) {
  DIR *d = opendir(dir);
  if (!d) return;
  struct dirent *e;
  int cap = 8; g_seed = malloc(cap * sizeof *g_seed);
  g_slen = malloc(cap * sizeof *g_slen);
  char path[1024];
  while ((e = readdir(d))) {
    if (e->d_name[0] == '.') continue;
    snprintf(path, sizeof path, "%s/%s", dir, e->d_name);
    FILE *f = fopen(path, "rb");
    if (!f) continue;
    fseek(f, 0, SEEK_END); long sz = ftell(f); fseek(f, 0, SEEK_SET);
    if (sz <= 0) { fclose(f); continue; }
    if (g_n == cap) { cap *= 2;
      g_seed = realloc(g_seed, cap * sizeof *g_seed);
      g_slen = realloc(g_slen, cap * sizeof *g_slen); }
    g_seed[g_n] = malloc(sz);
    if (fread(g_seed[g_n], 1, sz, f) == (size_t)sz) { g_slen[g_n] = sz; g_n++; }
    fclose(f);
  }
  closedir(d);
}

static void mutate(uint8_t *b, size_t n) {
  int ops = 1 + (xr() % 6);
  for (int i = 0; i < ops && n; i++) {
    uint32_t op = xr() % 3;
    size_t at = xr() % n;
    if (op == 0) b[at] ^= (uint8_t)(1 + xr() % 255);
    else if (op == 1) b[at] = (uint8_t)xr();
    else b[at] = (xr() & 1) ? 0x00 : 0xFF;
  }
}

static void hit(const uint8_t *buf, size_t n) {
  char err[512];
  PqcReader *r = pqc_reader_open(buf, n, err, sizeof err);
  if (r) {
    (void)pqc_reader_name(r); (void)pqc_reader_watermark(r);
    uint32_t nc = pqc_reader_n_chunks(r);
    for (uint32_t i = 0; i < nc; i++) {
      const char *nm; uint64_t ln; uint32_t crc;
      pqc_reader_chunk(r, i, &nm, &ln, &crc);
    }
    uint64_t off, dl; pqc_reader_data(r, &off, &dl);
    pqc_reader_free(r);
  }
  size_t ol = 0;
  uint8_t *o = pqc_shift_footer(buf, n, (int64_t)xr() - 1000, &ol,
                                err, sizeof err);
  if (o) pqc_free(o);
}

int main(int argc, char **argv) {
  long iters = argc > 1 ? atol(argv[1]) : 200000;
  const char *dir = argc > 2 ? argv[2] : "corpus";
  s_rng = argc > 3 ? (uint64_t)atoll(argv[3]) : 0x9E3779B97F4A7C15ull;
  load_corpus(dir);
  if (g_n == 0) { fprintf(stderr, "no corpus in %s\n", dir); return 2; }
  size_t maxlen = 0;
  for (int i = 0; i < g_n; i++) if (g_slen[i] > maxlen) maxlen = g_slen[i];
  uint8_t *buf = malloc(maxlen + 64);
  long accepted_clean = 0;
  /* every unmutated seed must parse (accepted) */
  for (int i = 0; i < g_n; i++) {
    char err[512];
    PqcReader *r = pqc_reader_open(g_seed[i], g_slen[i], err, sizeof err);
    if (r) { accepted_clean++; pqc_reader_free(r); }
  }
  for (long it = 0; it < iters; it++) {
    int s = xr() % g_n;
    size_t n = g_slen[s];
    memcpy(buf, g_seed[s], n);
    mutate(buf, n);
    hit(buf, n);
  }
  printf("fuzz-standalone: %ld iters clean over %d seeds "
         "(%ld seeds parse valid)\n", iters, g_n, accepted_clean);
  return 0;
}
#endif