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
/* The one Report of this test (report.h): every call is given it. */
static Report R;
#define OK(st, ring, name) do { int _s = (st); (void)(ring); \
  if (_s != REPORT_OK || R.status != REPORT_OK) { \
    fprintf(stderr, "FAIL %s: %s\n", name, R.error); failures++; } } while (0)

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
  opts.segment_bytes = 4096;    /* small segments: exercise reclaim */

  ArRing *a = NULL;
  OK(ar_ring_new(&top, 512, &opts, &a, &R), NULL, "ring_new");
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
  OK(ar_ring_new(&top, 2048, &opts, &b, &R), NULL, "ring_new b");

  /* ---- soak: 100k rows through a 512 ring; relation + stream every
   * 1000; every stream fed into ring b ---------------------------------- */
  const int64_t N = 100000;
  char strbuf[128];
  double listbuf[8];
  int64_t b_ingested = 0;
  for (int64_t i = 0; i < N; i++) {
    OK(ar_row_begin(a, NULL, &R), a, "row_begin");
    OK(ar_set_f64(a, L_tv, (double)i * 0.5, &R), a, "set tv");
    OK(ar_set_i64(a, L_pos, i * 10, &R), a, "set pos");
    OK(ar_set_i64(a, L_vel, -i, &R), a, "set vel");
    if (i % 2 == 0) OK(ar_set_i64(a, L_mark, i, &R), a, "set mark");
    /* every 97th value oversized (> 4096-byte segment) */
    if (i % 97 == 0) {
      static char big[6000];
      memset(big, 'B', sizeof(big));
      OK(ar_set_bytes(a, L_label, big, sizeof(big), &R), a, "set big label");
      /* the oversized value is taken, and the call's report says so in
       * one warning line: the core wrote nothing anywhere */
      CHECK(R.lines == 1 && R.dropped == 0 &&
            R.line[0].level == REPORT_WARNING &&
            strstr(R.line[0].text, "oversized value: 6000 elements") != NULL &&
            R.line[0].mono > 0 && R.line[0].wall > 1e9,
            "an oversized value makes one warning line");
    } else {
      int len = snprintf(strbuf, sizeof(strbuf), "value-%lld", (long long)i);
      OK(ar_set_bytes(a, L_label, strbuf, len, &R), a, "set label");
      CHECK(R.lines == 0, "a value that fits makes no line");
    }
    int nl = (int)(i % 8);
    for (int j = 0; j < nl; j++) listbuf[j] = (double)j;
    OK(ar_set_list(a, L_samp, listbuf, nl, &R), a, "set list");
    if (i % 5 == 4) OK(ar_commit(a, NULL, &R), a, "commit");

    if (i % 1000 == 999) {
      OK(ar_commit(a, NULL, &R), a, "commit@rel");
      ArRecordSet *rs = NULL;
      OK(ar_pin(a, 0, INT64_MAX, &rs, &R), a, "relation");
      CHECK(ar_recordset_hi(rs) - ar_recordset_lo(rs) > 0, "relation size");
      struct ArrowArrayStream stream;
      OK(ar_recordset_stream(rs, &stream, &R), a, "stream");
      /* release BEFORE draining: batches must keep everything alive */
      ar_recordset_release(rs);
      OK(ar_append_stream(b, &stream, &R), b, "append_stream");
      b_ingested = ar_newest_seq(b) + 1;
    }
  }
  OK(ar_commit(a, NULL, &R), a, "final commit");
  CHECK(ar_newest_seq(a) == N - 1, "a newest");
  CHECK(b_ingested > 0 && ar_newest_seq(b) == b_ingested - 1, "b ingested");

  /* spot-check content parity for the newest shared row */
  {
    int64_t seq_b = ar_newest_seq(b);
    double tvb = 0; bool nul = false;
    OK(ar_get_f64(b, seq_b, L_tv, &tvb, &nul, &R), b, "b get tv");
    const void *d; int64_t n;
    OK(ar_get_var(b, seq_b, L_label, &d, &n, &nul, &R), b, "b get label");
    CHECK(!nul && n > 0, "b label present");
  }

  /* pool bounded despite 100k strings through 4KB segments */
  CHECK(ar_pool_segment_count(a, L_label) < 64, "label pool bounded");
  CHECK(ar_pool_segment_count(a, L_samp) < 64, "samples pool bounded");
  CHECK(ar_pool_oversized_count(a, L_label) == (N + 96) / 97, "oversized count");

  /* ---- pin stall + staleness ------------------------------------------- */
  {
    ArRecordSet *rs = NULL;
    OK(ar_pin(a, 0, INT64_MAX, &rs, &R), a, "pin relation");
    /* the writer does not wait in the core: the call answers
     * REPORT_BLOCKED at once, names the pin and its age by the core's
     * clock, and changes nothing */
    int64_t newest = ar_newest_seq(a);
    ar_test_set_clock(1, 2e9, 1e9 + 12.5);
    int st = ar_row_begin(a, NULL, &R);
    ar_test_set_clock(0, 0, 0);
    CHECK(st == REPORT_BLOCKED && R.status == REPORT_BLOCKED,
          "pin stalls writer");
    CHECK(strstr(R.error, "is blocked by a pin on [") != NULL &&
          strstr(R.error, "a whole ring behind the writer") != NULL,
          "the stall names the pin");
    CHECK(R.lines == 0, "a stall makes no line");
    st = ar_row_begin(a, NULL, &R);
    CHECK(st == REPORT_BLOCKED, "still blocked: the call may be made again");
    OK(ar_commit(a, NULL, &R), a, "commit while blocked");
    CHECK(ar_newest_seq(a) == newest, "a blocked call took no row");
    ar_recordset_release(rs);
    OK(ar_row_begin(a, NULL, &R), a, "unblocked");
    OK(ar_commit(a, NULL, &R), a, "commit after pin");
    double x; bool nul;
    st = ar_get_f64(a, 0, L_tv, &x, &nul, &R);
    CHECK(st == REPORT_STALE && R.error[0] != 0, "old seq stale");
    /* a call must be given a report */
    CHECK(ar_get_f64(a, ar_newest_seq(a), L_tv, &x, &nul, NULL)
          == REPORT_INVALID, "no report: refused");
    newest = ar_newest_seq(a);
    CHECK(ar_row_begin(a, NULL, NULL) == REPORT_INVALID,
          "no report: refused");
    OK(ar_commit(a, NULL, &R), a, "commit after a refused call");
    CHECK(ar_newest_seq(a) == newest, "no report: no row taken");
  }

  /* ---- wire map ---------------------------------------------------------- */
  {
    int32_t leaves[] = { L_tv, L_pos };
    int32_t offs[] = { 0, 8 };
    int32_t sizes[] = { 8, 4 };
    ArWireMap *wm = NULL;
    OK(ar_wiremap_new(a, leaves, offs, sizes, 2, &wm, &R), a, "wiremap");
    unsigned char frame[12];
    double fv = 123.25; int32_t fp = 77;
    memcpy(frame, &fv, 8); memcpy(frame + 8, &fp, 4);
    OK(ar_row_begin(a, NULL, &R), a, "row for wire");
    OK(ar_from_wire(a, wm, frame, 12, &R), a, "from_wire");
    OK(ar_set_bytes(a, L_label, "w", 1, &R), a, "wire label");
    OK(ar_set_list(a, L_samp, listbuf, 0, &R), a, "wire list");
    OK(ar_commit(a, NULL, &R), a, "wire commit");
    unsigned char back[12];
    OK(ar_to_wire(a, wm, ar_newest_seq(a), back, 12, &R), a, "to_wire");
    CHECK(memcmp(frame, back, 12) == 0, "wire roundtrip");
    ar_wiremap_free(wm);
  }

  ar_ring_free(a);
  ar_ring_free(b);

  /* ---- a pin's age is counted on the core's clock ---------------------- */
  {
    struct ArrowSchema f0 = mk("l", "v", 0, 0, NULL);
    struct ArrowSchema *kids2[] = { &f0 };
    struct ArrowSchema top2 = mk("+s", "", 0, 1, kids2);
    ArRing *r2 = NULL;
    OK(ar_ring_new(&top2, 4, NULL, &r2, &R), NULL, "age ring");
    for (int i = 0; i < 4; i++) {
      OK(ar_row_begin(r2, NULL, &R), r2, "age row");
      OK(ar_set_i64(r2, 0, i, &R), r2, "age set");
    }
    OK(ar_commit(r2, NULL, &R), r2, "age commit");
    ar_test_set_clock(1, 2e9, 1000.0);
    ArRecordSet *held = NULL;
    OK(ar_pin(r2, 0, 1, &held, &R), r2, "age pin");
    ar_test_set_clock(1, 2e9, 1012.5);
    CHECK(ar_row_begin(r2, NULL, &R) == REPORT_BLOCKED &&
          strstr(R.error, "a pin on [0, 1) held 12.5s") != NULL,
          "the stall says how long the pin was held");
    ar_test_set_clock(0, 0, 0);
    ar_recordset_release(held);
    OK(ar_row_begin(r2, NULL, &R), r2, "age unblocked");
    ar_ring_free(r2);
  }

  /* ---- a jump in the row numbers: taken, with one warning line ---- */
  {
    struct ArrowSchema f0 = mk("l", "v", 0, 0, NULL);
    struct ArrowSchema *kids2[] = { &f0 };
    struct ArrowSchema top2 = mk("+s", "", 0, 1, kids2);
    ArRing *src = NULL, *dst = NULL;
    OK(ar_ring_new(&top2, 64, NULL, &src, &R), NULL, "gap source");
    f0 = mk("l", "v", 0, 0, NULL);
    top2 = mk("+s", "", 0, 1, kids2);
    OK(ar_ring_new(&top2, 64, NULL, &dst, &R), NULL, "gap target");
    for (int i = 0; i < 8; i++) {
      OK(ar_row_begin(src, NULL, &R), src, "gap row");
      OK(ar_set_i64(src, 0, i, &R), src, "gap set");
    }
    OK(ar_commit(src, NULL, &R), src, "gap commit");
    int64_t at[] = { 0, 100 };      /* rows 0..3 at 0, rows 4..7 at 100 */
    for (int k = 0; k < 2; k++) {
      ArChunkCursor *cur = NULL;
      OK(ar_chunks_open(src, k * 4, k * 4 + 4, 4, &cur, &R), src, "gap open");
      struct ArrowArray batch;
      int64_t first = -1;
      OK(ar_chunks_next(cur, &batch, &first, &R), src, "gap next");
      CHECK(batch.release != NULL && first == k * 4, "gap batch");
      OK(ar_append_batch_at(dst, &batch, at[k], &R), dst, "gap append");
      if (k == 0)
        CHECK(R.lines == 0, "rows in order make no line");
      else
        CHECK(R.lines == 1 && R.line[0].level == REPORT_WARNING &&
              strstr(R.line[0].text, "GAP: jumped seq 4 -> 100 (96 rows "
                     "never received)") != NULL,
              "a jump makes one warning line");
      if (batch.release) batch.release(&batch);
      ar_chunks_free(cur);
    }
    CHECK(ar_newest_seq(dst) == 103 && ar_oldest_seq(dst) == 100, "gap axis");
    ar_ring_free(src);
    ar_ring_free(dst);
  }

  /* ---- allocator fault injection: every alloc failure is a loud
   * REPORT_NO_MEMORY, never a crash or a leak (ASan verifies) ---- */
  {
    int built = 0, refused = 0;
    for (int budget = 0; budget < 64; budget++) {
      struct ArrowSchema g0 = mk("l", "v", 0, 0, NULL);
      struct ArrowSchema g1 = mk("u", "s", 2, 0, NULL);
      struct ArrowSchema *gk[] = { &g0, &g1 };
      struct ArrowSchema gt2 = mk("+s", "", 0, 2, gk);
      ArRing *rr = NULL;
      ar_test_set_alloc_budget(budget);
      int st = ar_ring_new(&gt2, 128, NULL, &rr, &R);
      ar_test_set_alloc_budget(-1);
      if (st != REPORT_OK) {
        CHECK(st == REPORT_NO_MEMORY && R.error[0] != 0 && rr == NULL,
              "nomem is the only failure mode");
        refused++;
        continue;
      }
      for (int i = 0; i < 10; i++) {
        OK(ar_row_begin(rr, NULL, &R), rr, "fi row");
        OK(ar_set_i64(rr, 0, i, &R), rr, "fi set");
        OK(ar_set_bytes(rr, 1, "abc", 3, &R), rr, "fi str");
      }
      OK(ar_commit(rr, NULL, &R), rr, "fi commit");
      ar_ring_free(rr);
      built++;
    }
    CHECK(built > 0, "some budgets succeed");
    CHECK(refused > 0, "some budgets fail");
  }

  if (failures == 0) printf("san_test: all checks passed\n");
  return failures ? 1 : 0;
}
