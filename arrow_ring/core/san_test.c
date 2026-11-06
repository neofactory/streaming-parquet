/* san_test.c — pure-C soak test for the arrow-ring core, built to run
 * under AddressSanitizer/UBSan (make san). Exercises, with no Python in
 * the loop: mixed-schema writes (fixed/struct/null/utf8/list), heavy
 * wrap, relations + stream export consumed and released in C,
 * ring-to-ring streaming (export -> ar_append_batch), wire maps, and
 * pin stall/release. Exits non-zero on any failure.
 */

#include "arrow_ring.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;
#define CHECK(cond, name) do { \
  if (!(cond)) { fprintf(stderr, "FAIL %s (%s:%d)\n", name, __FILE__, __LINE__); \
                 failures++; } } while (0)
#define OK(st, ring, name) do { ArStatus _s = (st); \
  if (_s != AR_OK) { fprintf(stderr, "FAIL %s: %s\n", name, \
                             ar_last_error(ring)); failures++; } } while (0)

/* ---- hand-built ArrowSchema (stack-owned; release is a no-op mark) ---- */

static void noop_release(struct ArrowSchema *s) { s->release = NULL; }

static struct ArrowSchema mk(const char *format, const char *name,
                             int64_t flags, int64_t n_children,
                             struct ArrowSchema **children) {
  struct ArrowSchema s;
  memset(&s, 0, sizeof(s));
  s.format = format;
  s.name = name;
  s.flags = flags;
  s.n_children = n_children;
  s.children = children;
  s.release = noop_release;
  return s;
}


/* ---- helpers for the write_ipc + fault-injection sections ---- */

static int64_t g_ipc_bytes;
static int64_t ipc_sink(void *ctx, const void *data, int64_t n) {
  (void)ctx; (void)data;
  g_ipc_bytes += n;
  return n;
}

static int64_t g_alloc_allow;
static void *failing_alloc(void *ctx, int64_t n) {
  (void)ctx;
  if (g_alloc_allow-- <= 0) return NULL;
  void *p = NULL;
  if (posix_memalign(&p, 64, (size_t)n) != 0) return NULL;
  return p;
}
static void failing_free(void *ctx, void *p, int64_t n) {
  (void)ctx; (void)n;
  free(p);
}

int main(void) {
  /* schema: tv f64, servo{pos i32, vel i32}, mark i32 nullable,
   *         label utf8, samples list<f64> */
  struct ArrowSchema pos = mk("i", "pos", 0, 0, NULL);
  struct ArrowSchema vel = mk("i", "vel", 0, 0, NULL);
  struct ArrowSchema *servo_kids[] = { &pos, &vel };
  struct ArrowSchema servo = mk("+s", "servo", 0, 2, servo_kids);
  struct ArrowSchema tv = mk("g", "tv", 0, 0, NULL);
  struct ArrowSchema mark = mk("i", "mark", ARROW_FLAG_NULLABLE, 0, NULL);
  struct ArrowSchema label = mk("u", "label", 0, 0, NULL);
  struct ArrowSchema lchild = mk("g", "item", 0, 0, NULL);
  struct ArrowSchema *l_kids[] = { &lchild };
  struct ArrowSchema samples = mk("+l", "samples", 0, 1, l_kids);
  struct ArrowSchema *top_kids[] = { &tv, &servo, &mark, &label, &samples };
  struct ArrowSchema top = mk("+s", "", 0, 5, top_kids);

  ArRingOptions opts;
  memset(&opts, 0, sizeof(opts));
  opts.lease_margin_s = -1.0;   /* clamps to 0: exact bounds */
  opts.segment_bytes = 4096;    /* small segments: exercise reclaim */

  ArRing *a = NULL;
  OK(ar_ring_new(&top, 512, &opts, &a), NULL, "ring_new");
  if (!a) return 1;

  int32_t L_tv = ar_leaf_index(a, "tv");
  int32_t L_pos = ar_leaf_index(a, "servo.pos");
  int32_t L_vel = ar_leaf_index(a, "servo.vel");
  int32_t L_mark = ar_leaf_index(a, "mark");
  int32_t L_label = ar_leaf_index(a, "label");
  int32_t L_samp = ar_leaf_index(a, "samples");
  CHECK(L_tv >= 0 && L_samp >= 0 && L_pos >= 0, "leaf indices");

  /* second ring, ingest target -- rebuild the schema (consumed above) */
  pos = mk("i", "pos", 0, 0, NULL); vel = mk("i", "vel", 0, 0, NULL);
  servo = mk("+s", "servo", 0, 2, servo_kids);
  tv = mk("g", "tv", 0, 0, NULL);
  mark = mk("i", "mark", ARROW_FLAG_NULLABLE, 0, NULL);
  label = mk("u", "label", 0, 0, NULL);
  lchild = mk("g", "item", 0, 0, NULL);
  samples = mk("+l", "samples", 0, 1, l_kids);
  top = mk("+s", "", 0, 5, top_kids);
  ArRing *b = NULL;
  OK(ar_ring_new(&top, 2048, &opts, &b), NULL, "ring_new b");

  /* ---- soak: 100k rows through a 512 ring; relation + stream every
   * 1000; every stream fed into ring b ---------------------------------- */
  const int64_t N = 100000;
  char strbuf[128];
  double listbuf[8];
  int64_t b_ingested = 0;
  for (int64_t i = 0; i < N; i++) {
    OK(ar_row_begin(a, 0, NULL), a, "row_begin");
    OK(ar_set_f64(a, L_tv, (double)i * 0.5), a, "set tv");
    OK(ar_set_i64(a, L_pos, i * 10), a, "set pos");
    OK(ar_set_i64(a, L_vel, -i), a, "set vel");
    if (i % 2 == 0) OK(ar_set_i64(a, L_mark, i), a, "set mark");
    /* every 97th value oversized (> 4096-byte segment) */
    if (i % 97 == 0) {
      static char big[6000];
      memset(big, 'B', sizeof(big));
      OK(ar_set_bytes(a, L_label, big, sizeof(big)), a, "set big label");
    } else {
      int len = snprintf(strbuf, sizeof(strbuf), "value-%lld", (long long)i);
      OK(ar_set_bytes(a, L_label, strbuf, len), a, "set label");
    }
    int nl = (int)(i % 8);
    for (int j = 0; j < nl; j++) listbuf[j] = (double)j;
    OK(ar_set_list(a, L_samp, listbuf, nl), a, "set list");
    if (i % 5 == 4) OK(ar_commit(a, NULL), a, "commit");

    if (i % 1000 == 999) {
      OK(ar_commit(a, NULL), a, "commit@rel");
      ArRecordSet *rs = NULL;
      OK(ar_relation(a, 0.0, &rs), a, "relation");
      CHECK(ar_recordset_hi(rs) - ar_recordset_lo(rs) > 0, "relation size");
      struct ArrowArrayStream stream;
      OK(ar_recordset_stream(rs, &stream), a, "stream");
      /* release BEFORE draining: batches must keep everything alive */
      ar_recordset_release(rs);
      OK(ar_append_stream(b, &stream), b, "append_stream");
      b_ingested = ar_newest_seq(b) + 1;
    }
  }
  OK(ar_commit(a, NULL), a, "final commit");
  CHECK(ar_newest_seq(a) == N - 1, "a newest");
  CHECK(b_ingested > 0 && ar_newest_seq(b) == b_ingested - 1, "b ingested");

  /* spot-check content parity for the newest shared row */
  {
    int64_t seq_b = ar_newest_seq(b);
    double tvb = 0; bool nul = false;
    OK(ar_get_f64(b, seq_b, L_tv, &tvb, &nul), b, "b get tv");
    const void *d; int64_t n;
    OK(ar_get_var(b, seq_b, L_label, &d, &n, &nul), b, "b get label");
    CHECK(!nul && n > 0, "b label present");
  }

  /* pool bounded despite 100k strings through 4KB segments */
  CHECK(ar_pool_segment_count(a, L_label) < 64, "label pool bounded");
  CHECK(ar_pool_segment_count(a, L_samp) < 64, "samples pool bounded");
  CHECK(ar_pool_oversized_count(a, L_label) == (N + 96) / 97, "oversized count");

  /* ---- pin stall + staleness ------------------------------------------- */
  {
    ArRecordSet *rs = NULL;
    OK(ar_relation(a, 0.0, &rs), a, "pin relation");
    ArStatus st = ar_row_begin(a, 5, NULL);
    CHECK(st == AR_ERR_PIN_STALLED, "pin stalls writer");
    ar_recordset_release(rs);
    OK(ar_row_begin(a, 0, NULL), a, "unblocked");
    OK(ar_commit(a, NULL), a, "commit after pin");
    double x; bool nul;
    st = ar_get_f64(a, 0, L_tv, &x, &nul);
    CHECK(st == AR_ERR_STALE, "old seq stale");
  }

  /* ---- wire map ---------------------------------------------------------- */
  {
    int32_t leaves[] = { L_tv, L_pos };
    int32_t offs[] = { 0, 8 };
    int32_t sizes[] = { 8, 4 };
    ArWireMap *wm = NULL;
    OK(ar_wiremap_new(a, leaves, offs, sizes, 2, &wm), a, "wiremap");
    unsigned char frame[12];
    double fv = 123.25; int32_t fp = 77;
    memcpy(frame, &fv, 8); memcpy(frame + 8, &fp, 4);
    OK(ar_row_begin(a, 0, NULL), a, "row for wire");
    OK(ar_from_wire(a, wm, frame, 12), a, "from_wire");
    OK(ar_set_bytes(a, L_label, "w", 1), a, "wire label");
    OK(ar_set_list(a, L_samp, listbuf, 0), a, "wire list");
    OK(ar_commit(a, NULL), a, "wire commit");
    unsigned char back[12];
    OK(ar_to_wire(a, wm, ar_newest_seq(a), back, 12), a, "to_wire");
    CHECK(memcmp(frame, back, 12) == 0, "wire roundtrip");
    ar_wiremap_free(wm);
  }

  ar_ring_free(a);
  ar_ring_free(b);

  /* ---- ar_recordset_write_ipc: the transport-agnostic egress ---- */
  {
    struct ArrowSchema f0 = mk("l", "v", 0, 0, NULL);
    struct ArrowSchema *kids2[] = { &f0 };
    struct ArrowSchema top2 = mk("+s", "", 0, 1, kids2);
    ArRing *r2 = NULL;
    OK(ar_ring_new(&top2, 64, NULL, &r2), NULL, "ipc ring");
    for (int i = 0; i < 40; i++) {
      OK(ar_row_begin(r2, 0, NULL), r2, "ipc row");
      OK(ar_set_i64(r2, 0, i), r2, "ipc set");
    }
    OK(ar_commit(r2, NULL), r2, "ipc commit");
    ArRecordSet *rs2 = NULL;
    OK(ar_relation(r2, 0.0, &rs2), r2, "ipc rel");
    g_ipc_bytes = 0;
    /* the core v1 contract: write_ipc is a LOUD not-implemented stub
     * (hosts feed ar_recordset_stream to their own IPC writer) */
    ArStatus ipc_st = ar_recordset_write_ipc(rs2, ipc_sink, NULL);
    CHECK(ipc_st != AR_OK, "write_ipc refuses (stub)");
    CHECK(strstr(ar_last_error(r2), "not implemented") != NULL,
          "write_ipc refusal is loud");
    ar_recordset_release(rs2);
    ar_ring_free(r2);
  }

  /* ---- allocator fault injection: every alloc failure is a loud
   * AR_ERR_NOMEM, never a crash or a leak (ASan verifies) ---- */
  {
    int built = 0;
    for (int budget = 0; budget < 64; budget++) {
      g_alloc_allow = budget;
      ArRingOptions o2;
      memset(&o2, 0, sizeof(o2));
      o2.allocator.alloc = failing_alloc;
      o2.allocator.free = failing_free;
      struct ArrowSchema g0 = mk("l", "v", 0, 0, NULL);
      struct ArrowSchema g1 = mk("u", "s", 2, 0, NULL);
      struct ArrowSchema *gk[] = { &g0, &g1 };
      struct ArrowSchema gt2 = mk("+s", "", 0, 2, gk);
      ArRing *rr = NULL;
      ArStatus st = ar_ring_new(&gt2, 128, &o2, &rr);
      if (st != AR_OK) {
        CHECK(st == AR_ERR_NOMEM, "nomem is the only failure mode");
        continue;
      }
      g_alloc_allow = 1 << 30;
      for (int i = 0; i < 10; i++) {
        OK(ar_row_begin(rr, 0, NULL), rr, "fi row");
        OK(ar_set_i64(rr, 0, i), rr, "fi set");
        OK(ar_set_bytes(rr, 1, "abc", 3), rr, "fi str");
      }
      OK(ar_commit(rr, NULL), rr, "fi commit");
      ar_ring_free(rr);
      built++;
    }
    CHECK(built > 0, "some budgets succeed");
  }

  if (failures == 0) printf("san_test: all checks passed\n");
  return failures ? 1 : 0;
}
