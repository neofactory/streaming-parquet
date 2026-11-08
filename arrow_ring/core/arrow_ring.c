/* arrow_ring.c — v1 implementation of the arrow-ring C core.
 *
 * Covers: ring lifecycle over an ArrowSchema, leaf-indexed set/get,
 * commit/pins/leases, the segmented var-width pool, and record-set
 * export as ArrowArrayStream (zero-copy chunks).
 *
 * Not yet (loud AR_ERR_INVALID stubs, see core/README.md):
 * ar_append_batch/stream, ArWireMap, ar_recordset_write_ipc, and the
 * parquet shim (separate .so).
 *
 * C11. Single writer thread per ring; readers on any thread; pins are
 * mutex-guarded; cursors are atomics so readers see committed counts
 * without locks.
 */

#include "arrow_ring.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* ---- thread-local error detail ---------------------------------------- */

static _Thread_local char g_err[512];

static ArStatus err(ArStatus s, const char *fmt, ...) {
  __builtin_va_list ap;
  __builtin_va_start(ap, fmt);
  vsnprintf(g_err, sizeof(g_err), fmt, ap);
  __builtin_va_end(ap);
  return s;
}

const char *ar_last_error(const ArRing *ring) {
  (void)ring;
  return g_err;
}

/* ---- allocator --------------------------------------------------------- */

static void *default_alloc(void *ctx, int64_t n) {
  (void)ctx;
  void *p = NULL;
  if (posix_memalign(&p, 64, (size_t)(n > 0 ? n : 1)) != 0) return NULL;
  memset(p, 0, (size_t)n);
  return p;
}

static void default_free(void *ctx, void *p, int64_t n) {
  (void)ctx; (void)n;
  free(p);
}

/* ---- monotonic clock ---------------------------------------------------- */

static double now_mono(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}

/* ---- half floats --------------------------------------------------------- */

static uint16_t f32_to_f16(float f) {
  union { float f; uint32_t u; } v = { f };
  uint32_t x = v.u;
  uint32_t sign = (x >> 16) & 0x8000u;
  int32_t exp = (int32_t)((x >> 23) & 0xffu) - 127 + 15;
  uint32_t man = x & 0x7fffffu;
  if (exp <= 0) return (uint16_t)sign;            /* flush to zero */
  if (exp >= 31) return (uint16_t)(sign | 0x7c00u);
  return (uint16_t)(sign | ((uint32_t)exp << 10) | (man >> 13));
}

static float f16_to_f32(uint16_t h) {
  uint32_t sign = ((uint32_t)h & 0x8000u) << 16;
  int32_t exp = (h >> 10) & 0x1f;
  uint32_t man = h & 0x3ffu;
  uint32_t x;
  if (exp == 0) {
    if (man == 0) {
      x = sign;
    } else {
      int32_t e = 127 - 15 + 1;
      while (!(man & 0x400u)) { man <<= 1; e--; }
      man &= 0x3ffu;
      x = sign | ((uint32_t)e << 23) | (man << 13);
    }
  } else if (exp == 31) {
    x = sign | 0x7f800000u | (man << 13);
  } else {
    x = sign | ((uint32_t)(exp - 15 + 127) << 23) | (man << 13);
  }
  union { uint32_t u; float f; } v = { x };
  return v.f;
}

/* ---- bit helpers --------------------------------------------------------- */

static inline void bit_set(uint8_t *b, int64_t i)   { b[i >> 3] |= (uint8_t)(1u << (i & 7)); }
static inline void bit_clear(uint8_t *b, int64_t i) { b[i >> 3] &= (uint8_t)~(1u << (i & 7)); }
static inline bool bit_test(const uint8_t *b, int64_t i) { return (b[i >> 3] >> (i & 7)) & 1u; }

/* ---- leaf model ----------------------------------------------------------- */

enum { K_PRIM, K_BOOL, K_BYTES, K_FSL, K_VAR };
enum { C_SIGNED, C_UNSIGNED, C_FLOAT };   /* prim class */

typedef struct Seg { int64_t start, n; uint8_t *data; int64_t nbytes; } Seg;

typedef struct Pool {
  int64_t itemsize, seg_elems, cursor;
  Seg *segs; int32_t n_segs, cap_segs;
  uint8_t **freelist; int32_t n_free, cap_free;
  int64_t oversized, skipped;
} Pool;

typedef struct Leaf {
  char *path;
  int kind;
  int cls;            /* prim/fsl element class */
  int64_t itemsize;   /* prim/fsl element bytes; var element bytes */
  int64_t width;      /* bytes: row bytes; fsl: elements per row */
  bool nullable;
  bool large_offsets; /* var */
  bool is_list;       /* var: list vs utf8/binary */
  uint8_t *data;      /* prim/bytes/fsl storage; bool bits */
  uint8_t *valid;     /* bitmap or NULL */
  int64_t *starts, *lens;  /* var */
  Pool pool;
  int64_t data_nbytes, valid_nbytes;
} Leaf;

typedef struct Pin {
  int64_t lo, hi;
  bool ranged;        /* ar_pin: the caller named [lo, hi); no lease */
  double duration_s, t0_mono;
  int64_t *var_lo;    /* per var-leaf elem_lo, indexed by var ordinal */
  struct Pin *next;
} Pin;

struct ArRing {
  atomic_int refs;
  struct ArrowSchema schema;      /* deep copy, owned */
  Leaf *leaves; int32_t n_leaves;
  int32_t *var_leaves; int32_t n_var;
  int32_t *nullable_leaves; int32_t n_nullable;
  int64_t capacity;
  atomic_llong pending, committed;
  atomic_llong read_floor;   /* seqs below this are unreadable: rows
                                skipped by a gap-advance (never
                                received locally) */
  Pin *pins;
  pthread_mutex_t pin_mu;
  double lease_margin_s, rate_ewma, rate_peak, last_commit_mono;
  int64_t segment_bytes;
  ArAllocator alc;
  void (*diag)(void *, const char *); void *diag_ctx;
};

struct ArRecordSet {
  atomic_int refs;
  ArRing *ring;
  int64_t lo, hi;
  double duration_s;
  Pin *pin;           /* NULL when unpinned (empty set) or released */
  bool released;
};

/* ---- ring ref counting ------------------------------------------------ */

static void ring_ref(ArRing *r) { atomic_fetch_add(&r->refs, 1); }
static void ring_free_now(ArRing *r);
static void ring_unref(ArRing *r) {
  if (atomic_fetch_sub(&r->refs, 1) == 1) ring_free_now(r);
}

/* ---- schema deep copy --------------------------------------------------- */

static void schema_release(struct ArrowSchema *s) {
  if (s->release == NULL) return;
  for (int64_t i = 0; i < s->n_children; i++) {
    if (s->children[i] && s->children[i]->release)
      s->children[i]->release(s->children[i]);
    free(s->children[i]);
  }
  free(s->children);
  free((void *)s->format);
  free((void *)s->name);
  free((void *)s->metadata);
  s->release = NULL;
}

static char *dupstr(const char *s) {
  if (s == NULL) return NULL;
  size_t n = strlen(s) + 1;
  char *d = malloc(n);
  if (d) memcpy(d, s, n);
  return d;
}

static int schema_copy(const struct ArrowSchema *src, struct ArrowSchema *dst) {
  memset(dst, 0, sizeof(*dst));
  dst->format = dupstr(src->format ? src->format : "");
  dst->name = dupstr(src->name);
  dst->metadata = NULL;   /* metadata deliberately not carried */
  dst->flags = src->flags;
  dst->n_children = src->n_children;
  dst->dictionary = NULL;
  if (src->n_children > 0) {
    dst->children = calloc((size_t)src->n_children, sizeof(void *));
    if (!dst->children) return -1;
    for (int64_t i = 0; i < src->n_children; i++) {
      dst->children[i] = calloc(1, sizeof(struct ArrowSchema));
      if (!dst->children[i]) return -1;
      if (schema_copy(src->children[i], dst->children[i]) != 0) return -1;
    }
  }
  dst->release = schema_release;
  return 0;
}

/* ---- format-string parsing ------------------------------------------------ */

static int parse_prim(const char *f, int *cls, int64_t *itemsize) {
  if (strcmp(f, "c") == 0) { *cls = C_SIGNED;   *itemsize = 1; return 0; }
  if (strcmp(f, "s") == 0) { *cls = C_SIGNED;   *itemsize = 2; return 0; }
  if (strcmp(f, "i") == 0) { *cls = C_SIGNED;   *itemsize = 4; return 0; }
  if (strcmp(f, "l") == 0) { *cls = C_SIGNED;   *itemsize = 8; return 0; }
  if (strcmp(f, "C") == 0) { *cls = C_UNSIGNED; *itemsize = 1; return 0; }
  if (strcmp(f, "S") == 0) { *cls = C_UNSIGNED; *itemsize = 2; return 0; }
  if (strcmp(f, "I") == 0) { *cls = C_UNSIGNED; *itemsize = 4; return 0; }
  if (strcmp(f, "L") == 0) { *cls = C_UNSIGNED; *itemsize = 8; return 0; }
  if (strcmp(f, "e") == 0) { *cls = C_FLOAT;    *itemsize = 2; return 0; }
  if (strcmp(f, "f") == 0) { *cls = C_FLOAT;    *itemsize = 4; return 0; }
  if (strcmp(f, "g") == 0) { *cls = C_FLOAT;    *itemsize = 8; return 0; }
  /* temporal: raw integer ticks */
  if (strcmp(f, "tdD") == 0) { *cls = C_SIGNED; *itemsize = 4; return 0; }
  if (strcmp(f, "tdm") == 0) { *cls = C_SIGNED; *itemsize = 8; return 0; }
  if (strcmp(f, "tts") == 0 || strcmp(f, "ttm") == 0) { *cls = C_SIGNED; *itemsize = 4; return 0; }
  if (strcmp(f, "ttu") == 0 || strcmp(f, "ttn") == 0) { *cls = C_SIGNED; *itemsize = 8; return 0; }
  if (strncmp(f, "ts", 2) == 0 && (f[2]=='s'||f[2]=='m'||f[2]=='u'||f[2]=='n') && f[3]==':')
    { *cls = C_SIGNED; *itemsize = 8; return 0; }
  if (strncmp(f, "tD", 2) == 0 && (f[2]=='s'||f[2]=='m'||f[2]=='u'||f[2]=='n') && f[3]==0)
    { *cls = C_SIGNED; *itemsize = 8; return 0; }
  return -1;
}

/* ---- leaf construction ---------------------------------------------------- */

typedef struct Flatten {
  Leaf *leaves; int32_t n, cap;
} Flatten;

static Leaf *flat_add(Flatten *fl) {
  if (fl->n == fl->cap) {
    fl->cap = fl->cap ? fl->cap * 2 : 8;
    fl->leaves = realloc(fl->leaves, (size_t)fl->cap * sizeof(Leaf));
  }
  Leaf *lf = &fl->leaves[fl->n++];
  memset(lf, 0, sizeof(*lf));
  return lf;
}

static ArStatus flatten(const struct ArrowSchema *s, const char *prefix,
                        Flatten *fl) {
  for (int64_t i = 0; i < s->n_children; i++) {
    const struct ArrowSchema *c = s->children[i];
    const char *f = c->format;
    char path[512];
    snprintf(path, sizeof(path), "%s%s%s", prefix, prefix[0] ? "." : "",
             c->name ? c->name : "");
    bool nullable = (c->flags & ARROW_FLAG_NULLABLE) != 0;
    if (c->dictionary != NULL)
      return err(AR_ERR_TYPE_UNSUPPORTED,
                 "field '%s': dictionary unsupported (an encoding, not a "
                 "logical type)", path);
    if (strcmp(f, "+s") == 0) {
      if (nullable)
        return err(AR_ERR_TYPE_UNSUPPORTED,
                   "field '%s': nullable struct unsupported (declare the "
                   "struct non-nullable; leaves may be nullable)", path);
      ArStatus st = flatten(c, path, fl);
      if (st != AR_OK) return st;
      continue;
    }
    Leaf lf0;
    memset(&lf0, 0, sizeof(lf0));
    lf0.nullable = nullable;
    int cls; int64_t isz;
    if (strcmp(f, "b") == 0) {
      lf0.kind = K_BOOL;
    } else if (parse_prim(f, &cls, &isz) == 0) {
      lf0.kind = K_PRIM; lf0.cls = cls; lf0.itemsize = isz;
    } else if (f[0] == 'd' && f[1] == ':') {
      lf0.kind = K_BYTES;
      lf0.width = strstr(f, ",256") ? 32 : 16;
    } else if (f[0] == 'w' && f[1] == ':') {
      lf0.kind = K_BYTES; lf0.width = atoll(f + 2);
    } else if (strcmp(f, "u") == 0 || strcmp(f, "U") == 0 ||
               strcmp(f, "z") == 0 || strcmp(f, "Z") == 0) {
      lf0.kind = K_VAR; lf0.itemsize = 1;
      lf0.large_offsets = (f[0] == 'U' || f[0] == 'Z');
    } else if (strcmp(f, "+l") == 0 || strcmp(f, "+L") == 0 ||
               strncmp(f, "+w:", 3) == 0) {
      if (c->n_children != 1)
        return err(AR_ERR_INVALID, "field '%s': malformed list", path);
      const char *cf = c->children[0]->format;
      if (parse_prim(cf, &cls, &isz) != 0)
        return err(AR_ERR_TYPE_UNSUPPORTED,
                   "field '%s': list of '%s' unsupported (fixed-width "
                   "primitive elements only)", path, cf);
      if (f[1] == 'w') {
        lf0.kind = K_FSL; lf0.cls = cls; lf0.itemsize = isz;
        lf0.width = atoll(f + 3);
        if (lf0.width <= 0)
          return err(AR_ERR_INVALID, "field '%s': bad fixed_size_list", path);
      } else {
        lf0.kind = K_VAR; lf0.cls = cls; lf0.itemsize = isz;
        lf0.is_list = true;
        lf0.large_offsets = (f[1] == 'L');
      }
    } else {
      return err(AR_ERR_TYPE_UNSUPPORTED,
                 "field '%s': type '%s' unsupported (see arrow_ring "
                 "README for the supported set)", path, f);
    }
    lf0.path = dupstr(path);
    *flat_add(fl) = lf0;
  }
  return AR_OK;
}

/* ---- pool ------------------------------------------------------------------ */

static void pool_init(Pool *p, int64_t itemsize, int64_t segment_bytes) {
  memset(p, 0, sizeof(*p));
  p->itemsize = itemsize;
  p->seg_elems = segment_bytes / itemsize;
  if (p->seg_elems < 1) p->seg_elems = 1;
}

static void pool_destroy(Pool *p, ArRing *r) {
  for (int32_t i = 0; i < p->n_segs; i++)
    r->alc.free(r->alc.ctx, p->segs[i].data, p->segs[i].nbytes);
  for (int32_t i = 0; i < p->n_free; i++)
    r->alc.free(r->alc.ctx, p->freelist[i], p->seg_elems * p->itemsize);
  free(p->segs);
  free(p->freelist);
}

static Seg *pool_append(Pool *p, int64_t start, int64_t n, uint8_t *data,
                        int64_t nbytes) {
  if (p->n_segs == p->cap_segs) {
    p->cap_segs = p->cap_segs ? p->cap_segs * 2 : 8;
    p->segs = realloc(p->segs, (size_t)p->cap_segs * sizeof(Seg));
  }
  Seg *s = &p->segs[p->n_segs++];
  s->start = start; s->n = n; s->data = data; s->nbytes = nbytes;
  return s;
}

static Seg *pool_current(Pool *p) {
  if (p->n_segs) {
    Seg *s = &p->segs[p->n_segs - 1];
    if (s->start <= p->cursor && p->cursor < s->start + s->n) return s;
  }
  return NULL;
}

static int32_t pool_index_of(const Pool *p, int64_t elem) {
  int32_t lo = 0, hi = p->n_segs;
  while (lo < hi) {
    int32_t mid = (lo + hi) / 2;
    if (elem < p->segs[mid].start) hi = mid;
    else if (elem >= p->segs[mid].start + p->segs[mid].n) lo = mid + 1;
    else return mid;
  }
  return -1;
}

static ArStatus pool_alloc(Pool *p, ArRing *r, const char *path,
                           const void *data, int64_t nbytes,
                           int64_t *out_start, int64_t *out_n) {
  int64_t n = nbytes / p->itemsize;
  if (n == 0) { *out_start = p->cursor; *out_n = 0; return AR_OK; }
  if (n > p->seg_elems) {
    Seg *cur = pool_current(p);
    if (cur) p->cursor = cur->start + cur->n;
    uint8_t *buf = r->alc.alloc(r->alc.ctx, nbytes);
    if (!buf) return err(AR_ERR_NOMEM, "pool alloc %lld bytes", (long long)nbytes);
    memcpy(buf, data, (size_t)nbytes);
    pool_append(p, p->cursor, n, buf, nbytes);
    *out_start = p->cursor;
    p->cursor += n;
    *out_n = n;
    p->oversized++;
    char msg[256];
    snprintf(msg, sizeof(msg),
             "[arrow_ring] oversized value: %lld elements (%lld bytes) in "
             "%s (#%lld)", (long long)n, (long long)nbytes, path,
             (long long)p->oversized);
    if (r->diag) r->diag(r->diag_ctx, msg); else fprintf(stderr, "%s\n", msg);
    return AR_OK;
  }
  Seg *cur = pool_current(p);
  if (cur == NULL || (p->cursor - cur->start) + n > cur->n) {
    if (cur) {
      p->skipped += cur->n - (p->cursor - cur->start);
      p->cursor = cur->start + cur->n;
    }
    uint8_t *buf;
    int64_t nb = p->seg_elems * p->itemsize;
    if (p->n_free) buf = p->freelist[--p->n_free];
    else {
      buf = r->alc.alloc(r->alc.ctx, nb);
      if (!buf) return err(AR_ERR_NOMEM, "pool segment %lld bytes", (long long)nb);
    }
    cur = pool_append(p, p->cursor, p->seg_elems, buf, nb);
  }
  int64_t off = (p->cursor - cur->start) * p->itemsize;
  memcpy(cur->data + off, data, (size_t)nbytes);
  *out_start = p->cursor;
  p->cursor += n;
  *out_n = n;
  return AR_OK;
}

/* Spare segments kept for reuse. Steady state cycles the freelist
 * 0..1 deep (a dying segment is immediately wanted for the next
 * fill), so two spares mean no allocator traffic in normal running.
 * Anything beyond two is spike residue -- a burst that grew the pool
 * while pins held the reclaim floor -- and is returned to the
 * allocator as the wrap passes it. The invariant: pool memory ~=
 * live window + pinned sets + current segment + two spares, with
 * transients decaying at wrap speed (nothing is immortal). */
#define POOL_SPARES 2

static void pool_reclaim(Pool *p, ArRing *r, int64_t floor) {
  int32_t drop = 0;
  while (drop < p->n_segs &&
         p->segs[drop].start + p->segs[drop].n <= floor)
    drop++;
  for (int32_t i = 0; i < drop; i++) {
    Seg *s = &p->segs[i];
    if (s->n == p->seg_elems && p->n_free < POOL_SPARES) {
      if (p->n_free == p->cap_free) {
        p->cap_free = p->cap_free ? p->cap_free * 2 : 8;
        p->freelist = realloc(p->freelist,
                              (size_t)p->cap_free * sizeof(void *));
      }
      p->freelist[p->n_free++] = s->data;
    } else {
      r->alc.free(r->alc.ctx, s->data, s->nbytes);
    }
  }
  if (drop) {
    memmove(p->segs, p->segs + drop,
            (size_t)(p->n_segs - drop) * sizeof(Seg));
    p->n_segs -= drop;
  }
}

/* Reclaim retired segments of a var leaf before growing its pool.
 * Floor = the pool start of `oldest`, the oldest row still live once
 * the caller's write lands, held down by every pin's var_lo. The
 * caller names that row: its slot must not have been overwritten yet
 * (a batch wrapping past the ring's end is ingested in two spans, and
 * the second span once read the floor from a slot the first had just
 * overwritten, freeing live rows' strings -- found 2026-10-27). Runs
 * on EVERY allocation path into the pool -- the setters AND bulk
 * ingest (a ring fed only by ar_append_batch, i.e. every attached
 * subscriber, must reclaim too; missing this grew subscriber pools
 * without bound, found by the torture rig). */
static void pool_retire(ArRing *r, Leaf *lf, int64_t oldest) {
  if (atomic_load(&r->committed) <= 0) return;
  int32_t leaf = (int32_t)(lf - r->leaves);
  if (oldest < 0) oldest = 0;
  int64_t floor = lf->starts[oldest % r->capacity];
  pthread_mutex_lock(&r->pin_mu);
  int32_t vi = 0;
  for (int32_t i = 0; i < r->n_var; i++)
    if (r->var_leaves[i] == leaf) { vi = i; break; }
  for (Pin *p = r->pins; p; p = p->next)
    if (p->var_lo[vi] < floor) floor = p->var_lo[vi];
  pthread_mutex_unlock(&r->pin_mu);
  pool_reclaim(&lf->pool, r, floor);
}

/* ---- ring lifecycle ---------------------------------------------------------- */

ArStatus ar_ring_new(const struct ArrowSchema *schema, int64_t capacity,
                     const ArRingOptions *opts, ArRing **out) {
  *out = NULL;
  if (schema == NULL || strcmp(schema->format, "+s") != 0)
    return err(AR_ERR_INVALID, "schema must be a struct-typed ArrowSchema");
  if (schema->n_children == 0)
    return err(AR_ERR_INVALID, "schema has no fields");
  if (capacity <= 0)
    return err(AR_ERR_INVALID, "capacity must be positive");

  ArRing *r = calloc(1, sizeof(ArRing));
  if (!r) return err(AR_ERR_NOMEM, "ring alloc");
  atomic_init(&r->refs, 1);
  atomic_init(&r->pending, 0);
  atomic_init(&r->committed, 0);
  pthread_mutex_init(&r->pin_mu, NULL);
  r->capacity = capacity;
  r->lease_margin_s = 0.5;
  r->segment_bytes = 1 << 20;
  r->alc.alloc = default_alloc;
  r->alc.free = default_free;
  if (opts) {
    r->rate_ewma = r->rate_peak = opts->rate_hint;
    if (opts->lease_margin_s > 0) r->lease_margin_s = opts->lease_margin_s;
    if (opts->lease_margin_s < 0) r->lease_margin_s = 0.0;
    if (opts->segment_bytes > 0) r->segment_bytes = opts->segment_bytes;
    if (opts->allocator.alloc) r->alc = opts->allocator;
    r->diag = opts->diag; r->diag_ctx = opts->diag_ctx;
  }
  r->last_commit_mono = -1.0;

  Flatten fl = {0};
  ArStatus st = flatten(schema, "", &fl);
  if (st != AR_OK) { free(fl.leaves); free(r); return st; }
  r->leaves = fl.leaves;
  r->n_leaves = fl.n;

  if (schema_copy(schema, &r->schema) != 0) {
    free(r);
    return err(AR_ERR_NOMEM, "schema copy");
  }

  int64_t nbits = (capacity + 7) / 8;
  bool oom = false;    /* every allocation checked; partial builds are
                          torn down by ring_free_now (NULL-tolerant) */
  for (int32_t i = 0; i < r->n_leaves; i++) {
    Leaf *lf = &r->leaves[i];
    if (lf->nullable) {
      lf->valid_nbytes = nbits;
      lf->valid = r->alc.alloc(r->alc.ctx, nbits);
      if (!lf->valid) oom = true;
    }
    switch (lf->kind) {
      case K_PRIM: lf->data_nbytes = capacity * lf->itemsize; break;
      case K_BOOL: lf->data_nbytes = nbits; break;
      case K_BYTES: lf->data_nbytes = capacity * lf->width; break;
      case K_FSL: lf->data_nbytes = capacity * lf->width * lf->itemsize; break;
      case K_VAR:
        lf->starts = r->alc.alloc(r->alc.ctx, capacity * 8);
        lf->lens = r->alc.alloc(r->alc.ctx, capacity * 8);
        if (!lf->starts || !lf->lens) oom = true;
        pool_init(&lf->pool, lf->itemsize, r->segment_bytes);
        r->n_var++;
        break;
    }
    if (lf->data_nbytes) {
      lf->data = r->alc.alloc(r->alc.ctx, lf->data_nbytes);
      if (!lf->data) oom = true;
    }
    if (lf->nullable) r->n_nullable++;
  }
  r->var_leaves = calloc((size_t)(r->n_var ? r->n_var : 1), sizeof(int32_t));
  r->nullable_leaves = calloc((size_t)(r->n_nullable ? r->n_nullable : 1),
                              sizeof(int32_t));
  if (oom || r->var_leaves == NULL || r->nullable_leaves == NULL) {
    ring_free_now(r);
    return err(AR_ERR_NOMEM, "ring storage allocation failed "
               "(capacity %lld rows)", (long long)capacity);
  }
  int32_t v = 0, nn = 0;
  for (int32_t i = 0; i < r->n_leaves; i++) {
    if (r->leaves[i].kind == K_VAR) r->var_leaves[v++] = i;
    if (r->leaves[i].nullable) r->nullable_leaves[nn++] = i;
  }
  /* We consumed the caller's schema (C-interface ownership transfer). */
  if (schema->release)
    ((struct ArrowSchema *)schema)->release((struct ArrowSchema *)schema);
  *out = r;
  return AR_OK;
}

static void ring_free_now(ArRing *r) {
  for (int32_t i = 0; i < r->n_leaves; i++) {
    Leaf *lf = &r->leaves[i];
    if (lf->data) r->alc.free(r->alc.ctx, lf->data, lf->data_nbytes);
    if (lf->valid) r->alc.free(r->alc.ctx, lf->valid, lf->valid_nbytes);
    if (lf->starts) r->alc.free(r->alc.ctx, lf->starts, r->capacity * 8);
    if (lf->lens) r->alc.free(r->alc.ctx, lf->lens, r->capacity * 8);
    if (lf->kind == K_VAR) pool_destroy(&lf->pool, r);
    free(lf->path);
  }
  free(r->leaves);
  free(r->var_leaves);
  free(r->nullable_leaves);
  if (r->schema.release) r->schema.release(&r->schema);
  pthread_mutex_destroy(&r->pin_mu);
  free(r);
}

void ar_ring_free(ArRing *ring) {
  if (ring) ring_unref(ring);
}

/* ---- leaves --------------------------------------------------------------- */

int32_t ar_leaf_index(const ArRing *r, const char *path) {
  for (int32_t i = 0; i < r->n_leaves; i++)
    if (strcmp(r->leaves[i].path, path) == 0) return i;
  return -1;
}

int32_t ar_leaf_count(const ArRing *r) { return r->n_leaves; }

const char *ar_leaf_path(const ArRing *r, int32_t leaf) {
  return (leaf >= 0 && leaf < r->n_leaves) ? r->leaves[leaf].path : NULL;
}

/* ---- pins ------------------------------------------------------------------- */

static Pin *pin_blocking(ArRing *r, int64_t victim, Pin *copy_out) {
  pthread_mutex_lock(&r->pin_mu);
  for (Pin *p = r->pins; p; p = p->next) {
    if (p->lo <= victim && victim < p->hi) {
      *copy_out = *p;
      pthread_mutex_unlock(&r->pin_mu);
      return copy_out;
    }
  }
  pthread_mutex_unlock(&r->pin_mu);
  return NULL;
}

static ArStatus wait_recyclable(ArRing *r, int64_t seq, int64_t block_ms) {
  int64_t victim = seq - r->capacity;
  if (victim < 0) return AR_OK;
  double deadline = now_mono() + (double)block_ms / 1e3;
  for (;;) {
    Pin copy;
    if (pin_blocking(r, victim, &copy) == NULL) return AR_OK;
    double t = now_mono();
    if (t >= deadline) {
      if (copy.ranged)
        return err(AR_ERR_PIN_STALLED,
                   "writer stalled at seq %lld: recycling seq %lld is blocked "
                   "by a range pin [%lld, %lld) held %.1fs, a whole ring "
                   "behind the writer", (long long)seq, (long long)victim,
                   (long long)copy.lo, (long long)copy.hi, t - copy.t0_mono);
      return err(AR_ERR_PIN_STALLED,
                 "writer stalled at seq %lld: recycling seq %lld is blocked "
                 "by a record set [%lld, %lld) held %.1fs against a %.1fs "
                 "lease", (long long)seq, (long long)victim,
                 (long long)copy.lo, (long long)copy.hi,
                 t - copy.t0_mono, copy.duration_s);
    }
    struct timespec ts = {0, 1000000};   /* 1 ms */
    nanosleep(&ts, NULL);
  }
}

/* ---- write side ---------------------------------------------------------------- */

ArStatus ar_row_begin(ArRing *r, int64_t block_ms, int64_t *out_seq) {
  int64_t pending = atomic_load(&r->pending);
  ArStatus st = wait_recyclable(r, pending, block_ms);
  if (st != AR_OK) return st;
  int64_t slot = pending % r->capacity;
  atomic_store(&r->pending, pending + 1);
  for (int32_t i = 0; i < r->n_nullable; i++)
    bit_clear(r->leaves[r->nullable_leaves[i]].valid, slot);
  for (int32_t i = 0; i < r->n_var; i++) {
    Leaf *lf = &r->leaves[r->var_leaves[i]];
    lf->starts[slot] = lf->pool.cursor;
    lf->lens[slot] = 0;
  }
  if (out_seq) *out_seq = pending;
  return AR_OK;
}

static ArStatus leaf_check(const ArRing *r, int32_t leaf, Leaf **out) {
  if (leaf < 0 || leaf >= r->n_leaves)
    return err(AR_ERR_INVALID, "leaf index %d out of range", leaf);
  *out = &((ArRing *)r)->leaves[leaf];
  return AR_OK;
}

static int64_t open_slot(ArRing *r) {
  return (atomic_load(&r->pending) - 1) % r->capacity;
}

static void mark_valid(Leaf *lf, int64_t slot) {
  if (lf->valid) bit_set(lf->valid, slot);
}

static ArStatus store_int(Leaf *lf, int64_t slot, int64_t v, bool from_unsigned,
                          uint64_t uv) {
  uint8_t *p = lf->data + slot * lf->itemsize;
  if (lf->cls == C_FLOAT)
    return err(AR_ERR_INVALID, "field '%s': integer setter on float leaf",
               lf->path);
  if (lf->cls == C_UNSIGNED) {
    uint64_t u = from_unsigned ? uv : (uint64_t)v;
    if (!from_unsigned && v < 0)
      return err(AR_ERR_WIDTH, "field '%s': negative into unsigned", lf->path);
    if (lf->itemsize < 8 && u >> (lf->itemsize * 8))
      return err(AR_ERR_WIDTH, "field '%s': %llu overflows %lld-byte leaf",
                 lf->path, (unsigned long long)u, (long long)lf->itemsize);
    memcpy(p, &u, (size_t)lf->itemsize);   /* little-endian hosts */
    return AR_OK;
  }
  int64_t x = from_unsigned ? (int64_t)uv : v;
  if (from_unsigned && uv > INT64_MAX)
    return err(AR_ERR_WIDTH, "field '%s': u64 overflows signed leaf", lf->path);
  if (lf->itemsize < 8) {
    int64_t lim = (int64_t)1 << (lf->itemsize * 8 - 1);
    if (x < -lim || x >= lim)
      return err(AR_ERR_WIDTH, "field '%s': %lld overflows %lld-byte leaf",
                 lf->path, (long long)x, (long long)lf->itemsize);
  }
  memcpy(p, &x, (size_t)lf->itemsize);
  return AR_OK;
}

ArStatus ar_set_i64(ArRing *r, int32_t leaf, int64_t v) {
  Leaf *lf; ArStatus st = leaf_check(r, leaf, &lf);
  if (st != AR_OK) return st;
  if (lf->kind != K_PRIM)
    return err(AR_ERR_INVALID, "field '%s': not an integer leaf", lf->path);
  st = store_int(lf, open_slot(r), v, false, 0);
  if (st == AR_OK) mark_valid(lf, open_slot(r));
  return st;
}

ArStatus ar_set_u64(ArRing *r, int32_t leaf, uint64_t v) {
  Leaf *lf; ArStatus st = leaf_check(r, leaf, &lf);
  if (st != AR_OK) return st;
  if (lf->kind != K_PRIM)
    return err(AR_ERR_INVALID, "field '%s': not an integer leaf", lf->path);
  st = store_int(lf, open_slot(r), 0, true, v);
  if (st == AR_OK) mark_valid(lf, open_slot(r));
  return st;
}

ArStatus ar_set_f64(ArRing *r, int32_t leaf, double v) {
  Leaf *lf; ArStatus st = leaf_check(r, leaf, &lf);
  if (st != AR_OK) return st;
  if (lf->kind != K_PRIM || lf->cls != C_FLOAT)
    return err(AR_ERR_INVALID, "field '%s': not a float leaf", lf->path);
  int64_t slot = open_slot(r);
  uint8_t *p = lf->data + slot * lf->itemsize;
  if (lf->itemsize == 8) memcpy(p, &v, 8);
  else if (lf->itemsize == 4) { float f = (float)v; memcpy(p, &f, 4); }
  else { uint16_t h = f32_to_f16((float)v); memcpy(p, &h, 2); }
  mark_valid(lf, slot);
  return AR_OK;
}

ArStatus ar_set_bool(ArRing *r, int32_t leaf, bool v) {
  Leaf *lf; ArStatus st = leaf_check(r, leaf, &lf);
  if (st != AR_OK) return st;
  if (lf->kind != K_BOOL)
    return err(AR_ERR_INVALID, "field '%s': not a bool leaf", lf->path);
  int64_t slot = open_slot(r);
  if (v) bit_set(lf->data, slot); else bit_clear(lf->data, slot);
  mark_valid(lf, slot);
  return AR_OK;
}

ArStatus ar_set_bytes(ArRing *r, int32_t leaf, const void *data,
                      int64_t nbytes) {
  Leaf *lf; ArStatus st = leaf_check(r, leaf, &lf);
  if (st != AR_OK) return st;
  int64_t slot = open_slot(r);
  if (lf->kind == K_BYTES) {
    if (nbytes != lf->width)
      return err(AR_ERR_WIDTH, "field '%s': needs exactly %lld bytes, got "
                 "%lld", lf->path, (long long)lf->width, (long long)nbytes);
    memcpy(lf->data + slot * lf->width, data, (size_t)nbytes);
    mark_valid(lf, slot);
    return AR_OK;
  }
  if (lf->kind == K_VAR && !lf->is_list) {
    pool_retire(r, lf, atomic_load(&r->pending) - r->capacity);
    int64_t start, n;
    st = pool_alloc(&lf->pool, r, lf->path, data, nbytes, &start, &n);
    if (st != AR_OK) return st;
    lf->starts[slot] = start;
    lf->lens[slot] = n;
    mark_valid(lf, slot);
    return AR_OK;
  }
  return err(AR_ERR_INVALID, "field '%s': not a bytes leaf", lf->path);
}

ArStatus ar_set_list(ArRing *r, int32_t leaf, const void *elems,
                     int64_t n_elems) {
  Leaf *lf; ArStatus st = leaf_check(r, leaf, &lf);
  if (st != AR_OK) return st;
  int64_t slot = open_slot(r);
  if (lf->kind == K_FSL) {
    if (n_elems != lf->width)
      return err(AR_ERR_WIDTH, "field '%s': needs exactly %lld elements, "
                 "got %lld", lf->path, (long long)lf->width,
                 (long long)n_elems);
    memcpy(lf->data + slot * lf->width * lf->itemsize, elems,
           (size_t)(n_elems * lf->itemsize));
    mark_valid(lf, slot);
    return AR_OK;
  }
  if (lf->kind == K_VAR && lf->is_list) {
    pool_retire(r, lf, atomic_load(&r->pending) - r->capacity);
    int64_t start, n;
    st = pool_alloc(&lf->pool, r, lf->path, elems, n_elems * lf->itemsize,
                    &start, &n);
    if (st != AR_OK) return st;
    lf->starts[slot] = start;
    lf->lens[slot] = n;
    mark_valid(lf, slot);
    return AR_OK;
  }
  return err(AR_ERR_INVALID, "field '%s': not a list leaf", lf->path);
}

ArStatus ar_set_null(ArRing *r, int32_t leaf) {
  Leaf *lf; ArStatus st = leaf_check(r, leaf, &lf);
  if (st != AR_OK) return st;
  if (!lf->nullable)
    return err(AR_ERR_NOT_NULLABLE, "field '%s' is not nullable", lf->path);
  int64_t slot = open_slot(r);
  bit_clear(lf->valid, slot);
  if (lf->kind == K_VAR) lf->lens[slot] = 0;
  return AR_OK;
}

ArStatus ar_commit(ArRing *r, int64_t *out_count) {
  int64_t pending = atomic_load(&r->pending);
  int64_t committed = atomic_load(&r->committed);
  int64_t n = pending - committed;
  if (n > 0) {
    atomic_store(&r->committed, pending);
    double t = now_mono();
    if (r->last_commit_mono >= 0) {
      double dt = t - r->last_commit_mono;
      if (dt > 0) {
        double inst = (double)n / dt;
        double alpha = dt / 10.0;   /* RATE_TAU_S */
        if (alpha > 1.0) alpha = 1.0;
        r->rate_ewma += alpha * (inst - r->rate_ewma);
        if (r->rate_ewma > r->rate_peak) r->rate_peak = r->rate_ewma;
      }
    }
    r->last_commit_mono = t;
  }
  if (out_count) *out_count = n;
  return AR_OK;
}

/* ---- read side: single values --------------------------------------------------- */

int64_t ar_oldest_seq(const ArRing *r) {
  int64_t o = atomic_load(&((ArRing *)r)->pending) - r->capacity;
  int64_t f = atomic_load(&((ArRing *)r)->read_floor);
  if (o < f) o = f;
  return o > 0 ? o : 0;
}

int64_t ar_newest_seq(const ArRing *r) {
  return atomic_load(&((ArRing *)r)->committed) - 1;
}

static ArStatus read_check(const ArRing *r, int64_t seq, int32_t leaf,
                           Leaf **out, int64_t *slot, bool *is_null,
                           bool *null_out) {
  ArStatus st = leaf_check(r, leaf, out);
  if (st != AR_OK) return st;
  int64_t committed = atomic_load(&((ArRing *)r)->committed);
  if (seq >= committed)
    return err(AR_ERR_STALE, "seq %lld not committed (newest is %lld)",
               (long long)seq, (long long)(committed - 1));
  int64_t oldest = ar_oldest_seq(r);
  if (seq < oldest)
    return err(AR_ERR_STALE, "seq %lld recycled (oldest readable is %lld)",
               (long long)seq, (long long)oldest);
  *slot = seq % r->capacity;
  bool null = (*out)->valid ? !bit_test((*out)->valid, *slot) : false;
  if (is_null) *is_null = null;
  *null_out = null;
  return AR_OK;
}

ArStatus ar_get_i64(const ArRing *r, int64_t seq, int32_t leaf, int64_t *out,
                    bool *is_null) {
  Leaf *lf; int64_t slot; bool null;
  ArStatus st = read_check(r, seq, leaf, &lf, &slot, is_null, &null);
  if (st != AR_OK) return st;
  if (null) { *out = 0; return AR_OK; }
  if (lf->kind != K_PRIM || lf->cls == C_FLOAT)
    return err(AR_ERR_INVALID, "field '%s': not an integer leaf", lf->path);
  const uint8_t *p = lf->data + slot * lf->itemsize;
  int64_t v = 0;
  if (lf->cls == C_SIGNED) {
    switch (lf->itemsize) {
      case 1: v = *(const int8_t *)p; break;
      case 2: { int16_t x; memcpy(&x, p, 2); v = x; break; }
      case 4: { int32_t x; memcpy(&x, p, 4); v = x; break; }
      default: memcpy(&v, p, 8); break;
    }
  } else {
    uint64_t u = 0; memcpy(&u, p, (size_t)lf->itemsize);
    v = (int64_t)u;
  }
  *out = v;
  return AR_OK;
}

ArStatus ar_get_u64(const ArRing *r, int64_t seq, int32_t leaf, uint64_t *out,
                    bool *is_null) {
  Leaf *lf; int64_t slot; bool null;
  ArStatus st = read_check(r, seq, leaf, &lf, &slot, is_null, &null);
  if (st != AR_OK) return st;
  if (null) { *out = 0; return AR_OK; }
  if (lf->kind != K_PRIM || lf->cls != C_UNSIGNED)
    return err(AR_ERR_INVALID, "field '%s': not an unsigned leaf", lf->path);
  uint64_t u = 0;
  memcpy(&u, lf->data + slot * lf->itemsize, (size_t)lf->itemsize);
  *out = u;
  return AR_OK;
}

ArStatus ar_get_f64(const ArRing *r, int64_t seq, int32_t leaf, double *out,
                    bool *is_null) {
  Leaf *lf; int64_t slot; bool null;
  ArStatus st = read_check(r, seq, leaf, &lf, &slot, is_null, &null);
  if (st != AR_OK) return st;
  if (null) { *out = 0.0; return AR_OK; }
  if (lf->kind != K_PRIM || lf->cls != C_FLOAT)
    return err(AR_ERR_INVALID, "field '%s': not a float leaf", lf->path);
  const uint8_t *p = lf->data + slot * lf->itemsize;
  if (lf->itemsize == 8) memcpy(out, p, 8);
  else if (lf->itemsize == 4) { float f; memcpy(&f, p, 4); *out = f; }
  else { uint16_t h; memcpy(&h, p, 2); *out = f16_to_f32(h); }
  return AR_OK;
}

ArStatus ar_get_bool(const ArRing *r, int64_t seq, int32_t leaf, bool *out,
                     bool *is_null) {
  Leaf *lf; int64_t slot; bool null;
  ArStatus st = read_check(r, seq, leaf, &lf, &slot, is_null, &null);
  if (st != AR_OK) return st;
  if (null) { *out = false; return AR_OK; }
  if (lf->kind != K_BOOL)
    return err(AR_ERR_INVALID, "field '%s': not a bool leaf", lf->path);
  *out = bit_test(lf->data, slot);
  return AR_OK;
}

ArStatus ar_get_var(const ArRing *r, int64_t seq, int32_t leaf,
                    const void **data, int64_t *n_elems, bool *is_null) {
  Leaf *lf; int64_t slot; bool null;
  ArStatus st = read_check(r, seq, leaf, &lf, &slot, is_null, &null);
  if (st != AR_OK) return st;
  if (null) { *data = NULL; *n_elems = 0; return AR_OK; }
  if (lf->kind == K_BYTES) {
    *data = lf->data + slot * lf->width;
    *n_elems = lf->width;
    return AR_OK;
  }
  if (lf->kind == K_FSL) {
    *data = lf->data + slot * lf->width * lf->itemsize;
    *n_elems = lf->width;
    return AR_OK;
  }
  if (lf->kind == K_VAR) {
    int64_t n = lf->lens[slot];
    if (n == 0) { *data = NULL; *n_elems = 0; return AR_OK; }
    int32_t si = pool_index_of(&lf->pool, lf->starts[slot]);
    if (si < 0)
      return err(AR_ERR_STALE, "field '%s': value storage recycled", lf->path);
    Seg *s = &lf->pool.segs[si];
    *data = s->data + (lf->starts[slot] - s->start) * lf->itemsize;
    *n_elems = n;
    return AR_OK;
  }
  return err(AR_ERR_INVALID, "field '%s': no var view for this leaf", lf->path);
}

double ar_lease_rate(const ArRing *r) { return r->rate_peak; }

uint64_t ar_leaf_data_address(const ArRing *r, int32_t leaf) {
  if (leaf < 0 || leaf >= r->n_leaves) return 0;
  return (uint64_t)(uintptr_t)r->leaves[leaf].data;
}

uint64_t ar_leaf_valid_address(const ArRing *r, int32_t leaf) {
  if (leaf < 0 || leaf >= r->n_leaves) return 0;
  return (uint64_t)(uintptr_t)r->leaves[leaf].valid;
}

uint64_t ar_var_data_address(const ArRing *r, int32_t leaf,
                             int64_t elem_seq) {
  if (leaf < 0 || leaf >= r->n_leaves) return 0;
  Leaf *lf = &((ArRing *)r)->leaves[leaf];
  if (lf->kind != K_VAR) return 0;
  int32_t si = pool_index_of(&lf->pool, elem_seq);
  if (si < 0) return 0;
  Seg *s = &lf->pool.segs[si];
  return (uint64_t)(uintptr_t)(s->data + (elem_seq - s->start) * lf->itemsize);
}

int32_t ar_pool_segment_count(const ArRing *r, int32_t leaf) {
  if (leaf < 0 || leaf >= r->n_leaves) return -1;
  Leaf *lf = &((ArRing *)r)->leaves[leaf];
  return lf->kind == K_VAR ? lf->pool.n_segs : -1;
}

int64_t ar_pool_oversized_count(const ArRing *r, int32_t leaf) {
  if (leaf < 0 || leaf >= r->n_leaves) return -1;
  Leaf *lf = &((ArRing *)r)->leaves[leaf];
  return lf->kind == K_VAR ? lf->pool.oversized : -1;
}

void ar_set_lease_margin(ArRing *r, double margin_s) {
  r->lease_margin_s = margin_s >= 0 ? margin_s : 0.0;
}

/* ---- record sets ------------------------------------------------------------------ */

static void rs_ref(ArRecordSet *rs) { atomic_fetch_add(&rs->refs, 1); }

static void rs_drop_pin(ArRecordSet *rs) {
  if (rs->pin == NULL) return;
  ArRing *r = rs->ring;
  pthread_mutex_lock(&r->pin_mu);
  Pin **pp = &r->pins;
  while (*pp && *pp != rs->pin) pp = &(*pp)->next;
  if (*pp) *pp = rs->pin->next;
  pthread_mutex_unlock(&r->pin_mu);
  free(rs->pin->var_lo);
  free(rs->pin);
  rs->pin = NULL;
}

static void rs_unref(ArRecordSet *rs) {
  if (atomic_fetch_sub(&rs->refs, 1) == 1) {
    rs_drop_pin(rs);
    ring_unref(rs->ring);
    free(rs);
  }
}

void ar_recordset_release(ArRecordSet *rs) {
  if (rs == NULL || rs->released) return;
  rs->released = true;
  rs_drop_pin(rs);
  rs_unref(rs);
}

void ar_recordset_detach(ArRecordSet *rs) {
  if (rs == NULL || rs->released) return;
  rs->released = true;
  rs_unref(rs);   /* pin drops with the LAST reference (stream/batches) */
}

int64_t ar_recordset_lo(const ArRecordSet *rs) { return rs->lo; }
int64_t ar_recordset_hi(const ArRecordSet *rs) { return rs->hi; }

/* The record set over [lo, hi), both already clamped to the readable
 * window: pinned when non-empty. Shared by ar_relation (the ring picks
 * the range from a lease) and ar_pin (the caller names it). */
static ArStatus make_relation(ArRing *r, int64_t lo, int64_t hi,
                              double duration_s, bool ranged,
                              ArRecordSet **out) {
  ArRecordSet *rs = calloc(1, sizeof(ArRecordSet));
  if (!rs) return err(AR_ERR_NOMEM, "record set");
  atomic_init(&rs->refs, 1);
  rs->ring = r;
  ring_ref(r);
  rs->duration_s = duration_s;
  if (lo >= hi) {
    rs->lo = rs->hi = lo;
    *out = rs;
    return AR_OK;
  }
  rs->lo = lo; rs->hi = hi;
  Pin *pin = calloc(1, sizeof(Pin));
  pin->lo = lo; pin->hi = hi;
  pin->ranged = ranged;
  pin->duration_s = duration_s;
  pin->t0_mono = now_mono();
  pin->var_lo = calloc((size_t)(r->n_var ? r->n_var : 1), sizeof(int64_t));
  for (int32_t i = 0; i < r->n_var; i++) {
    Leaf *lf = &r->leaves[r->var_leaves[i]];
    pin->var_lo[i] = lf->starts[lo % r->capacity];
  }
  pthread_mutex_lock(&r->pin_mu);
  pin->next = r->pins;
  r->pins = pin;
  pthread_mutex_unlock(&r->pin_mu);
  rs->pin = pin;
  *out = rs;
  return AR_OK;
}

ArStatus ar_relation(ArRing *r, double duration_s, ArRecordSet **out) {
  *out = NULL;
  double excl_d = r->rate_peak * (duration_s + r->lease_margin_s);
  int64_t exclusion = (int64_t)excl_d;
  if ((double)exclusion < excl_d) exclusion++;
  /* Exclude the rows the writer will RECYCLE during an honest hold:
   * the recycle frontier after the hold is pending + exclusion -
   * capacity. A ring that is not near full excludes nothing (the
   * naive oldest+exclusion formula broke the archive+hot union view
   * on cold start -- found by the exerciser 2026-08-19). */
  int64_t lo = atomic_load(&r->pending) + exclusion - r->capacity;
  int64_t oldest = ar_oldest_seq(r);
  if (lo < oldest) lo = oldest;
  int64_t hi = atomic_load(&r->committed);
  return make_relation(r, lo, hi, duration_s, false, out);
}

ArStatus ar_pin(ArRing *r, int64_t lo, int64_t hi, ArRecordSet **out) {
  *out = NULL;
  if (lo < 0 || hi < lo)
    return err(AR_ERR_INVALID, "ar_pin: bad range [%lld, %lld)",
               (long long)lo, (long long)hi);
  /* Clamp to the readable window: rows below the oldest are gone (the
   * caller sees rs->lo > lo), rows at or above committed do not exist
   * yet (rs->hi < hi). No lease, no exclusion: the pin holds exactly
   * the rows asked for. */
  int64_t oldest = ar_oldest_seq(r);
  int64_t committed = atomic_load(&r->committed);
  if (lo < oldest) lo = oldest;
  if (hi > committed) hi = committed;
  return make_relation(r, lo, hi, 0.0, true, out);
}

/* ---- export: chunk computation ------------------------------------------------- */

typedef struct Chunk { int64_t a, b; } Chunk;   /* physical slot span */

static Chunk *compute_chunks(ArRing *r, int64_t lo, int64_t hi,
                             int32_t *n_out) {
  int64_t p0 = lo % r->capacity, total = hi - lo;
  int64_t spans[2][2]; int n_spans;
  if (p0 + total <= r->capacity) {
    spans[0][0] = p0; spans[0][1] = p0 + total; n_spans = 1;
  } else {
    spans[0][0] = p0; spans[0][1] = r->capacity;
    spans[1][0] = 0; spans[1][1] = (p0 + total) % r->capacity;
    n_spans = 2;
  }
  int32_t cap = 8, n = 0;
  Chunk *chunks = malloc((size_t)cap * sizeof(Chunk));
  for (int s = 0; s < n_spans; s++) {
    int64_t a = spans[s][0], b = spans[s][1], start = a;
    for (int64_t i = a + 1; i < b; i++) {
      bool cut = false;
      for (int32_t vi = 0; vi < r->n_var && !cut; vi++) {
        Leaf *lf = &r->leaves[r->var_leaves[vi]];
        if (lf->starts[i] != lf->starts[i - 1] + lf->lens[i - 1]) cut = true;
        else if (lf->lens[i] && lf->lens[i - 1] &&
                 pool_index_of(&lf->pool, lf->starts[i]) !=
                 pool_index_of(&lf->pool, lf->starts[i - 1])) cut = true;
      }
      if (cut) {
        if (n == cap) { cap *= 2; chunks = realloc(chunks, (size_t)cap * sizeof(Chunk)); }
        chunks[n].a = start; chunks[n].b = i; n++;
        start = i;
      }
    }
    if (b > start) {
      if (n == cap) { cap *= 2; chunks = realloc(chunks, (size_t)cap * sizeof(Chunk)); }
      chunks[n].a = start; chunks[n].b = b; n++;
    }
  }
  *n_out = n;
  return chunks;
}

/* ---- export: batch building ------------------------------------------------------ */

typedef struct BatchPriv {
  ArRecordSet *rs;
  void **allocs; int32_t n_allocs, cap_allocs;
} BatchPriv;

static void *bp_alloc(BatchPriv *bp, size_t n) {
  void *p = calloc(1, n ? n : 1);
  if (bp->n_allocs == bp->cap_allocs) {
    bp->cap_allocs = bp->cap_allocs ? bp->cap_allocs * 2 : 16;
    bp->allocs = realloc(bp->allocs, (size_t)bp->cap_allocs * sizeof(void *));
  }
  bp->allocs[bp->n_allocs++] = p;
  return p;
}

static void child_release(struct ArrowArray *a) { a->release = NULL; }

static void batch_release(struct ArrowArray *a) {
  if (a->release == NULL) return;
  BatchPriv *bp = a->private_data;
  for (int32_t i = 0; i < bp->n_allocs; i++) free(bp->allocs[i]);
  free(bp->allocs);
  rs_unref(bp->rs);
  free(bp);
  a->release = NULL;
}

/* Build the ArrowArray node for one leaf over physical slots [a, a+n). */
static ArStatus leaf_array(ArRing *r, BatchPriv *bp, int32_t li,
                           int64_t a, int64_t n, struct ArrowArray *node) {
  Leaf *lf = &r->leaves[li];
  node->length = n;
  node->offset = 0;
  node->null_count = lf->valid ? -1 : 0;
  node->release = child_release;
  if (lf->kind == K_PRIM || lf->kind == K_BOOL || lf->kind == K_BYTES) {
    node->n_buffers = 2;
    node->buffers = bp_alloc(bp, 2 * sizeof(void *));
    node->buffers[0] = lf->valid;   /* bit offset via node->offset */
    node->buffers[1] = lf->data;
    node->offset = a;
    return AR_OK;
  }
  if (lf->kind == K_FSL) {
    node->n_buffers = 1;
    node->buffers = bp_alloc(bp, sizeof(void *));
    node->buffers[0] = lf->valid;
    node->offset = a;
    node->n_children = 1;
    node->children = bp_alloc(bp, sizeof(void *));
    struct ArrowArray *child = bp_alloc(bp, sizeof(struct ArrowArray));
    node->children[0] = child;
    child->length = (a + n) * lf->width;
    child->offset = 0;
    child->null_count = 0;
    child->n_buffers = 2;
    child->buffers = bp_alloc(bp, 2 * sizeof(void *));
    child->buffers[0] = NULL;
    child->buffers[1] = lf->data;
    child->release = child_release;
    return AR_OK;
  }
  /* K_VAR: fresh offsets (+ fresh validity), zero-copy data segment. */
  int64_t first_nz = -1;
  for (int64_t i = a; i < a + n; i++)
    if (lf->lens[i]) { first_nz = i; break; }
  uint8_t *seg_data = NULL;
  int64_t seg_elems = 0, base = 0;
  if (first_nz >= 0) {
    int32_t si = pool_index_of(&lf->pool, lf->starts[first_nz]);
    if (si < 0)
      return err(AR_ERR_STALE, "field '%s': segment recycled under pin "
                 "(internal error)", lf->path);
    Seg *s = &lf->pool.segs[si];
    seg_data = s->data;
    seg_elems = s->n;
    base = lf->starts[first_nz] - s->start;
  }
  int64_t osz = lf->large_offsets ? 8 : 4;
  uint8_t *offs = bp_alloc(bp, (size_t)((n + 1) * osz));
  int64_t acc = base;
  for (int64_t i = 0; i <= n; i++) {
    if (!lf->large_offsets && acc > INT32_MAX)
      return err(AR_ERR_OFFSET_OVERFLOW,
                 "field '%s': chunk exceeds int32 offsets; declare the "
                 "field large_*", lf->path);
    if (lf->large_offsets) ((int64_t *)offs)[i] = acc;
    else ((int32_t *)offs)[i] = (int32_t)acc;
    if (i < n) acc += lf->lens[a + i];
  }
  uint8_t *vfresh = NULL;
  if (lf->valid) {
    vfresh = bp_alloc(bp, (size_t)((n + 7) / 8));
    for (int64_t i = 0; i < n; i++)
      if (bit_test(lf->valid, a + i)) vfresh[i >> 3] |= (uint8_t)(1u << (i & 7));
  }
  if (lf->is_list) {
    node->n_buffers = 2;
    node->buffers = bp_alloc(bp, 2 * sizeof(void *));
    node->buffers[0] = vfresh;
    node->buffers[1] = offs;
    node->n_children = 1;
    node->children = bp_alloc(bp, sizeof(void *));
    struct ArrowArray *child = bp_alloc(bp, sizeof(struct ArrowArray));
    node->children[0] = child;
    child->length = seg_elems;
    child->null_count = 0;
    child->n_buffers = 2;
    child->buffers = bp_alloc(bp, 2 * sizeof(void *));
    child->buffers[0] = NULL;
    child->buffers[1] = seg_data;
    child->release = child_release;
  } else {
    node->n_buffers = 3;
    node->buffers = bp_alloc(bp, 3 * sizeof(void *));
    node->buffers[0] = vfresh;
    node->buffers[1] = offs;
    node->buffers[2] = seg_data;
  }
  return AR_OK;
}

/* Recursively build nodes following the schema tree; `li` walks the
 * flattened leaves in the same depth-first order flatten() used. */
static ArStatus build_children(ArRing *r, BatchPriv *bp,
                               const struct ArrowSchema *s, int32_t *li,
                               int64_t a, int64_t n,
                               struct ArrowArray *parent) {
  parent->n_children = s->n_children;
  parent->children = bp_alloc(bp, (size_t)s->n_children * sizeof(void *));
  for (int64_t i = 0; i < s->n_children; i++) {
    struct ArrowArray *node = bp_alloc(bp, sizeof(struct ArrowArray));
    parent->children[i] = node;
    const struct ArrowSchema *cs = s->children[i];
    if (strcmp(cs->format, "+s") == 0) {
      node->length = n;
      node->offset = 0;
      node->null_count = 0;
      node->n_buffers = 1;
      node->buffers = bp_alloc(bp, sizeof(void *));
      node->buffers[0] = NULL;
      node->release = child_release;
      ArStatus st = build_children(r, bp, cs, li, a, n, node);
      if (st != AR_OK) return st;
    } else {
      ArStatus st = leaf_array(r, bp, (*li)++, a, n, node);
      if (st != AR_OK) return st;
    }
  }
  return AR_OK;
}

static ArStatus build_batch(ArRecordSet *rs, int64_t a, int64_t b,
                            struct ArrowArray *out) {
  ArRing *r = rs->ring;
  memset(out, 0, sizeof(*out));
  BatchPriv *bp = calloc(1, sizeof(BatchPriv));
  bp->rs = rs;
  rs_ref(rs);
  out->private_data = bp;
  out->release = batch_release;
  out->length = b - a;
  out->null_count = 0;
  out->n_buffers = 1;
  out->buffers = bp_alloc(bp, sizeof(void *));
  out->buffers[0] = NULL;
  int32_t li = 0;
  ArStatus st = build_children(r, bp, &r->schema, &li, a, b - a, out);
  if (st != AR_OK) { batch_release(out); return st; }
  return AR_OK;
}

/* ---- export: ArrowArrayStream ------------------------------------------------------ */

typedef struct StreamPriv {
  ArRecordSet *rs;
  Chunk *chunks; int32_t n_chunks, idx;
  char errbuf[512];
} StreamPriv;

static int stream_get_schema(struct ArrowArrayStream *s,
                             struct ArrowSchema *out) {
  StreamPriv *sp = s->private_data;
  /* errno-style return codes, per the C Stream Interface contract */
  return schema_copy(&sp->rs->ring->schema, out) == 0 ? 0 : 12 /* ENOMEM */;
}

static int stream_get_next(struct ArrowArrayStream *s,
                           struct ArrowArray *out) {
  StreamPriv *sp = s->private_data;
  if (sp->idx >= sp->n_chunks) {
    out->release = NULL;   /* end of stream */
    return 0;
  }
  Chunk c = sp->chunks[sp->idx++];
  ArStatus st = build_batch(sp->rs, c.a, c.b, out);
  if (st != AR_OK) {
    snprintf(sp->errbuf, sizeof(sp->errbuf), "%s", g_err);
    return 5;   /* EIO-style */
  }
  return 0;
}

static const char *stream_get_last_error(struct ArrowArrayStream *s) {
  StreamPriv *sp = s->private_data;
  return sp->errbuf[0] ? sp->errbuf : NULL;
}

static void stream_release(struct ArrowArrayStream *s) {
  if (s->release == NULL) return;
  StreamPriv *sp = s->private_data;
  free(sp->chunks);
  rs_unref(sp->rs);
  free(sp);
  s->release = NULL;
}

ArStatus ar_recordset_stream(ArRecordSet *rs, struct ArrowArrayStream *out) {
  StreamPriv *sp = calloc(1, sizeof(StreamPriv));
  if (!sp) return err(AR_ERR_NOMEM, "stream");
  sp->rs = rs;
  rs_ref(rs);
  if (rs->hi > rs->lo)
    sp->chunks = compute_chunks(rs->ring, rs->lo, rs->hi, &sp->n_chunks);
  out->get_schema = stream_get_schema;
  out->get_next = stream_get_next;
  out->get_last_error = stream_get_last_error;
  out->release = stream_release;
  out->private_data = sp;
  return AR_OK;
}

/* ---- ingest: Arrow batches in (the memcpy path) --------------------------------- */

static void copy_bits(uint8_t *dst, int64_t di, const uint8_t *src,
                      int64_t si, int64_t n) {
  for (int64_t k = 0; k < n; k++) {
    if (src == NULL || bit_test(src, si + k)) bit_set(dst, di + k);
    else bit_clear(dst, di + k);
  }
}

static bool any_null_bits(const uint8_t *valid, int64_t off, int64_t n) {
  if (valid == NULL) return false;
  for (int64_t k = 0; k < n; k++)
    if (!bit_test(valid, off + k)) return true;
  return false;
}

/* Ingest rows [row0, row0+n) of leaf array `a` (whose logical element
 * offset, including all parent struct offsets, is `eo`) into ring slots
 * starting at dest_slot (span guaranteed unwrapped by the caller). */
static ArStatus ingest_leaf(ArRing *r, Leaf *lf, const struct ArrowArray *a,
                            int64_t eo, int64_t row0, int64_t n,
                            int64_t dest_slot, int64_t oldest) {
  const uint8_t *src_valid = a->n_buffers > 0 ? a->buffers[0] : NULL;
  int64_t so = eo + row0;
  if (lf->valid) {
    copy_bits(lf->valid, dest_slot, src_valid, so, n);
  } else if (src_valid && any_null_bits(src_valid, so, n)) {
    return err(AR_ERR_INVALID, "field '%s': null values into a "
               "non-nullable leaf", lf->path);
  }
  switch (lf->kind) {
    case K_PRIM:
      memcpy(lf->data + dest_slot * lf->itemsize,
             (const uint8_t *)a->buffers[1] + so * lf->itemsize,
             (size_t)(n * lf->itemsize));
      return AR_OK;
    case K_BYTES:
      memcpy(lf->data + dest_slot * lf->width,
             (const uint8_t *)a->buffers[1] + so * lf->width,
             (size_t)(n * lf->width));
      return AR_OK;
    case K_BOOL:
      copy_bits(lf->data, dest_slot, a->buffers[1], so, n);
      return AR_OK;
    case K_FSL: {
      const struct ArrowArray *c = a->children[0];
      memcpy(lf->data + dest_slot * lf->width * lf->itemsize,
             (const uint8_t *)c->buffers[1] +
                 (c->offset + so * lf->width) * lf->itemsize,
             (size_t)(n * lf->width * lf->itemsize));
      return AR_OK;
    }
    case K_VAR: {
      int64_t first, last1;   /* element range [first, last1) in values */
      const uint8_t *vals;
      if (lf->large_offsets) {
        const int64_t *off = a->buffers[1];
        first = off[so]; last1 = off[so + n];
      } else {
        const int32_t *off = a->buffers[1];
        first = off[so]; last1 = off[so + n];
      }
      if (lf->is_list) {
        const struct ArrowArray *c = a->children[0];
        vals = (const uint8_t *)c->buffers[1] +
               (c->offset + first) * lf->itemsize;
      } else {
        vals = (const uint8_t *)a->buffers[2] + first * lf->itemsize;
      }
      int64_t start, total;
      pool_retire(r, lf, oldest);
      ArStatus st = pool_alloc(&lf->pool, r, lf->path, vals,
                               (last1 - first) * lf->itemsize, &start, &total);
      if (st != AR_OK) return st;
      for (int64_t i = 0; i < n; i++) {
        int64_t o0, o1;
        if (lf->large_offsets) {
          const int64_t *off = a->buffers[1];
          o0 = off[so + i]; o1 = off[so + i + 1];
        } else {
          const int32_t *off = a->buffers[1];
          o0 = off[so + i]; o1 = off[so + i + 1];
        }
        lf->starts[dest_slot + i] = start + (o0 - first);
        lf->lens[dest_slot + i] = o1 - o0;
      }
      return AR_OK;
    }
  }
  return err(AR_ERR_INVALID, "field '%s': unknown kind", lf->path);
}

/* Walk schema/array trees in the flatten DFS order. */
static ArStatus ingest_children(ArRing *r, const struct ArrowSchema *s,
                                const struct ArrowArray *a, int64_t eo,
                                int32_t *li, int64_t row0, int64_t n,
                                int64_t dest_slot, int64_t oldest) {
  if (a->n_children != s->n_children)
    return err(AR_ERR_INVALID, "batch structure does not match ring schema "
               "(%lld children vs %lld)", (long long)a->n_children,
               (long long)s->n_children);
  for (int64_t i = 0; i < s->n_children; i++) {
    const struct ArrowSchema *cs = s->children[i];
    const struct ArrowArray *ca = a->children[i];
    int64_t ceo = eo + ca->offset;
    if (strcmp(cs->format, "+s") == 0) {
      ArStatus st = ingest_children(r, cs, ca, ceo, li, row0, n, dest_slot,
                                    oldest);
      if (st != AR_OK) return st;
    } else {
      ArStatus st = ingest_leaf(r, &r->leaves[(*li)++], ca, ceo, row0, n,
                                dest_slot, oldest);
      if (st != AR_OK) return st;
    }
  }
  return AR_OK;
}

ArStatus ar_append_batch_at(ArRing *r, struct ArrowArray *batch,
                            int64_t first_seq) {
  if (batch == NULL || batch->release == NULL)
    return err(AR_ERR_INVALID, "append: released/NULL batch");
  if (first_seq < -1)
    return err(AR_ERR_INVALID, "append_at: first_seq %lld is invalid "
               "(-1 = plain append, >= 0 = explicit axis)",
               (long long)first_seq);
  int64_t n = batch->length;
  ArStatus st = AR_OK;
  if (first_seq >= 0) {
    int64_t next = atomic_load(&r->pending);
    if (first_seq < next) {
      st = err(AR_ERR_INVALID, "append_at: seq %lld regresses (next "
               "expected is %lld)", (long long)first_seq,
               (long long)next);
      goto done;
    }
    if (first_seq > next) {
      if (atomic_load(&r->committed) != next) {
        st = err(AR_ERR_INVALID, "append_at: cannot gap-advance with "
                 "open (uncommitted) rows");
        goto done;
      }
      if (next > 0) {          /* a real gap, not a fresh-ring start */
        char msg[192];
        snprintf(msg, sizeof(msg),
                 "[arrow_ring] GAP: jumped seq %lld -> %lld (%lld rows "
                 "never received)", (long long)next,
                 (long long)first_seq, (long long)(first_seq - next));
        if (r->diag) r->diag(r->diag_ctx, msg);
        else fprintf(stderr, "%s\n", msg);
      }
      atomic_store(&r->pending, first_seq);
      atomic_store(&r->committed, first_seq);
      atomic_store(&r->read_floor, first_seq);
    }
  }
  if (n > r->capacity) {
    st = err(AR_ERR_INVALID, "append: batch of %lld rows exceeds ring "
             "capacity %lld", (long long)n, (long long)r->capacity);
    goto done;
  }
  if (n > 0) {
    int64_t pending = atomic_load(&r->pending);
    /* Recycling check for the whole span (no blocking on ingest). */
    for (int64_t seq = pending; seq < pending + n; seq++) {
      st = wait_recyclable(r, seq, 0);
      if (st != AR_OK) goto done;
    }
    int64_t done_rows = 0;
    while (done_rows < n) {
      int64_t dest_slot = (pending + done_rows) % r->capacity;
      int64_t span = n - done_rows;
      if (dest_slot + span > r->capacity) span = r->capacity - dest_slot;
      int32_t li = 0;
      /* the oldest row live after this span: the first one past it,
       * whose slot this span does not touch */
      st = ingest_children(r, &r->schema, batch, batch->offset, &li,
                           done_rows, span, dest_slot,
                           pending + done_rows + span - r->capacity);
      if (st != AR_OK) goto done;
      done_rows += span;
    }
    atomic_store(&r->pending, pending + n);
    atomic_store(&r->committed, pending + n);
    /* rate observation, same as commit */
    double t = now_mono();
    if (r->last_commit_mono >= 0) {
      double dt = t - r->last_commit_mono;
      if (dt > 0) {
        double inst = (double)n / dt;
        double alpha = dt / 10.0;
        if (alpha > 1.0) alpha = 1.0;
        r->rate_ewma += alpha * (inst - r->rate_ewma);
        if (r->rate_ewma > r->rate_peak) r->rate_peak = r->rate_ewma;
      }
    }
    r->last_commit_mono = t;
  }
done:
  batch->release(batch);   /* we consumed it (C-interface ownership) */
  return st;
}

ArStatus ar_append_batch(ArRing *r, struct ArrowArray *batch) {
  return ar_append_batch_at(r, batch, -1);
}

ArStatus ar_append_stream(ArRing *r, struct ArrowArrayStream *stream) {
  if (stream == NULL || stream->release == NULL)
    return err(AR_ERR_INVALID, "append: released/NULL stream");
  ArStatus st = AR_OK;
  for (;;) {
    struct ArrowArray batch;
    if (stream->get_next(stream, &batch) != 0) {
      const char *msg = stream->get_last_error
          ? stream->get_last_error(stream) : NULL;
      st = err(AR_ERR_IO, "append: stream error: %s", msg ? msg : "unknown");
      break;
    }
    if (batch.release == NULL) break;   /* end of stream */
    st = ar_append_batch(r, &batch);
    if (st != AR_OK) break;
  }
  stream->release(stream);
  return st;
}

/* ---- wire maps: packed row-major frames <-> fixed-width leaves ------------------ */

struct ArWireMap {
  int32_t n;
  int32_t *leaves, *offsets, *sizes;
  int64_t min_frame;      /* largest offset+size: the frame length floor */
};

static int64_t leaf_row_bytes(const Leaf *lf) {
  switch (lf->kind) {
    case K_PRIM: return lf->itemsize;
    case K_BYTES: return lf->width;
    case K_FSL: return lf->width * lf->itemsize;
    default: return -1;   /* bool (bit-packed) and var are not wire-mappable */
  }
}

ArStatus ar_wiremap_new(const ArRing *r, const int32_t *leaves,
                        const int32_t *offsets, const int32_t *sizes,
                        int32_t n, ArWireMap **out) {
  *out = NULL;
  if (n <= 0) return err(AR_ERR_INVALID, "wiremap: empty map");
  ArWireMap *m = calloc(1, sizeof(ArWireMap));
  m->n = n;
  m->leaves = malloc((size_t)n * 4);
  m->offsets = malloc((size_t)n * 4);
  m->sizes = malloc((size_t)n * 4);
  for (int32_t i = 0; i < n; i++) {
    if (leaves[i] < 0 || leaves[i] >= r->n_leaves) {
      ar_wiremap_free(m);
      return err(AR_ERR_INVALID, "wiremap: leaf index %d out of range",
                 leaves[i]);
    }
    const Leaf *lf = &r->leaves[leaves[i]];
    int64_t want = leaf_row_bytes(lf);
    if (want < 0) {
      ar_wiremap_free(m);
      return err(AR_ERR_TYPE_UNSUPPORTED,
                 "wiremap: field '%s' is not wire-mappable (fixed-width "
                 "prim/decimal/fixed_size_binary/fixed_size_list only)",
                 lf->path);
    }
    if (sizes[i] != want) {
      ar_wiremap_free(m);
      return err(AR_ERR_WIDTH, "wiremap: field '%s' is %lld bytes, map "
                 "says %d", lf->path, (long long)want, sizes[i]);
    }
    if (offsets[i] < 0) {
      ar_wiremap_free(m);
      return err(AR_ERR_INVALID, "wiremap: negative offset");
    }
    m->leaves[i] = leaves[i];
    m->offsets[i] = offsets[i];
    m->sizes[i] = sizes[i];
    if (offsets[i] + sizes[i] > m->min_frame)
      m->min_frame = offsets[i] + sizes[i];
  }
  *out = m;
  return AR_OK;
}

void ar_wiremap_free(ArWireMap *m) {
  if (m == NULL) return;
  free(m->leaves);
  free(m->offsets);
  free(m->sizes);
  free(m);
}

ArStatus ar_from_wire(ArRing *r, const ArWireMap *m, const void *frame,
                      int64_t frame_len) {
  if (frame_len < m->min_frame)
    return err(AR_ERR_WIDTH, "from_wire: frame %lld bytes, map needs %lld",
               (long long)frame_len, (long long)m->min_frame);
  int64_t slot = open_slot(r);
  const uint8_t *f = frame;
  for (int32_t i = 0; i < m->n; i++) {
    Leaf *lf = &r->leaves[m->leaves[i]];
    memcpy(lf->data + slot * m->sizes[i], f + m->offsets[i],
           (size_t)m->sizes[i]);
    if (lf->valid) bit_set(lf->valid, slot);
  }
  return AR_OK;
}

ArStatus ar_to_wire(const ArRing *r, const ArWireMap *m, int64_t seq,
                    void *frame, int64_t frame_len) {
  if (frame_len < m->min_frame)
    return err(AR_ERR_WIDTH, "to_wire: frame %lld bytes, map needs %lld",
               (long long)frame_len, (long long)m->min_frame);
  int64_t committed = atomic_load(&((ArRing *)r)->committed);
  if (seq >= committed)
    return err(AR_ERR_STALE, "seq %lld not committed", (long long)seq);
  int64_t oldest = ar_oldest_seq(r);
  if (seq < oldest)
    return err(AR_ERR_STALE, "seq %lld recycled (oldest readable %lld)",
               (long long)seq, (long long)oldest);
  int64_t slot = seq % r->capacity;
  uint8_t *f = frame;
  for (int32_t i = 0; i < m->n; i++) {
    const Leaf *lf = &r->leaves[m->leaves[i]];
    memcpy(f + m->offsets[i], lf->data + slot * m->sizes[i],
           (size_t)m->sizes[i]);
  }
  return AR_OK;
}

/* ---- not yet implemented (loud stubs) ---------------------------------------------- */

ArStatus ar_recordset_write_ipc(ArRecordSet *rs, ArWriteFn write, void *ctx) {
  (void)rs; (void)write; (void)ctx;
  return err(AR_ERR_INVALID, "ar_recordset_write_ipc: not implemented in "
             "core v1 (feed ar_recordset_stream to a host IPC writer)");
}
