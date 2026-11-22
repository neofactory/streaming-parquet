/* pqc.c — parquet_checkpoint core. See pqc.h (the spec).
 *
 * C11, libc only. The Python package is the reference implementation;
 * run_c_compat.py proves parity (byte-level where the formats are
 * deterministic, interchangeability everywhere).
 */

#include "pqc.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ---- errors ----------------------------------------------------------- */

static void set_err(char *err, size_t errlen, const char *msg) {
    if (err && errlen) {
        strncpy(err, msg, errlen - 1);
        err[errlen - 1] = '\0';
    }
}

/* ---- CRC-32 (IEEE, zlib-compatible) ----------------------------------- */

static uint32_t crc_table[256];
static int crc_ready = 0;

static void crc_init(void) {
    for (uint32_t n = 0; n < 256; n++) {
        uint32_t c = n;
        for (int k = 0; k < 8; k++)
            c = (c & 1) ? 0xEDB88320u ^ (c >> 1) : c >> 1;
        crc_table[n] = c;
    }
    crc_ready = 1;
}

uint32_t pqc_crc32(const uint8_t *data, size_t len) {
    if (!crc_ready) crc_init();
    uint32_t c = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; i++)
        c = crc_table[(c ^ data[i]) & 0xFF] ^ (c >> 8);
    return c ^ 0xFFFFFFFFu;
}

double pqc_dead_after(double beat_seconds) {
    double beats = PQC_DEAD_WRITER_BEATS * beat_seconds;
    return beats > PQC_DEAD_WRITER_MIN_S ? beats : PQC_DEAD_WRITER_MIN_S;
}

/* ---- growable buffer -------------------------------------------------- */

typedef struct { uint8_t *p; size_t len, cap; } Buf;

static int buf_reserve(Buf *b, size_t extra) {
    if (b->len + extra <= b->cap) return 0;
    size_t cap = b->cap ? b->cap : 256;
    while (cap < b->len + extra) cap *= 2;
    uint8_t *p = realloc(b->p, cap);
    if (!p) return -1;
    b->p = p;
    b->cap = cap;
    return 0;
}

static int buf_put(Buf *b, const void *src, size_t n) {
    if (buf_reserve(b, n)) return -1;
    memcpy(b->p + b->len, src, n);
    b->len += n;
    return 0;
}

static int buf_byte(Buf *b, uint8_t v) { return buf_put(b, &v, 1); }

static void buf_free(Buf *b) { free(b->p); b->p = NULL; b->len = b->cap = 0; }

static int buf_u16le(Buf *b, uint16_t v) {
    uint8_t x[2] = { (uint8_t)v, (uint8_t)(v >> 8) };
    return buf_put(b, x, 2);
}

static int buf_u32le(Buf *b, uint32_t v) {
    uint8_t x[4] = { (uint8_t)v, (uint8_t)(v >> 8),
                     (uint8_t)(v >> 16), (uint8_t)(v >> 24) };
    return buf_put(b, x, 4);
}

static int buf_u64le(Buf *b, uint64_t v) {
    uint8_t x[8];
    for (int i = 0; i < 8; i++) x[i] = (uint8_t)(v >> (8 * i));
    return buf_put(b, x, 8);
}

static uint16_t rd_u16le(const uint8_t *p) {
    return (uint16_t)(p[0] | (p[1] << 8));
}

static uint32_t rd_u32le(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8)
         | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

static uint64_t rd_u64le(const uint8_t *p) {
    uint64_t v = 0;
    for (int i = 7; i >= 0; i--) v = (v << 8) | p[i];
    return v;
}

/* ---- thrift compact protocol ------------------------------------------ */

enum {
    T_BOOL_TRUE = 1, T_BOOL_FALSE = 2, T_I8 = 3, T_I16 = 4, T_I32 = 5,
    T_I64 = 6, T_DOUBLE = 7, T_BINARY = 8, T_LIST = 9, T_SET = 10,
    T_MAP = 11, T_STRUCT = 12
};

/* Contexts along the FileMetaData -> RowGroup -> ColumnChunk ->
 * ColumnMetaData path; everything else copies verbatim. */
enum { CTX_PLAIN = 0, CTX_FMD, CTX_RG, CTX_CC, CTX_CMD };

typedef struct {
    const uint8_t *in;
    size_t i, n;
    Buf *out;                   /* NULL while skipping */
    int64_t delta;
    /* capture during an FMD walk (add-time scan) */
    Buf *rg_slices;             /* concatenated RowGroup element bytes */
    uint32_t rg_count;
    int64_t num_rows;
    int have_rows;
    /* error */
    char *err;
    size_t errlen;
    int failed;
} T;

static void t_fail(T *t, const char *msg) {
    if (!t->failed) {
        t->failed = 1;
        set_err(t->err, t->errlen, msg);
    }
}

static uint8_t t_byte(T *t) {
    if (t->i >= t->n) { t_fail(t, "thrift: truncated"); return 0; }
    return t->in[t->i++];
}

static uint64_t t_varint(T *t) {
    uint64_t v = 0;
    int shift = 0;
    for (;;) {
        if (shift > 63) { t_fail(t, "thrift: varint overflow"); return 0; }
        uint8_t b = t_byte(t);
        if (t->failed) return 0;
        v |= (uint64_t)(b & 0x7F) << shift;
        if (!(b & 0x80)) return v;
        shift += 7;
    }
}

static int64_t t_zigzag(T *t) {
    uint64_t v = t_varint(t);
    return (int64_t)(v >> 1) ^ -(int64_t)(v & 1);
}

static void w_varint(Buf *b, uint64_t v) {
    while (v > 0x7F) {
        buf_byte(b, (uint8_t)((v & 0x7F) | 0x80));
        v >>= 7;
    }
    buf_byte(b, (uint8_t)v);
}

static void w_zigzag(Buf *b, int64_t v) {
    w_varint(b, ((uint64_t)v << 1) ^ (uint64_t)(v >> 63));
}

/* Skip one value of type `ty` (no output). */
static void t_skip(T *t, int ty) {
    switch (ty) {
    case T_BOOL_TRUE: case T_BOOL_FALSE: return;   /* struct-field bool */
    case T_I8: t_byte(t); return;
    case T_I16: case T_I32: case T_I64: t_varint(t); return;
    case T_DOUBLE:
        if (t->i + 8 > t->n) { t_fail(t, "thrift: truncated double"); return; }
        t->i += 8;
        return;
    case T_BINARY: {
        uint64_t len = t_varint(t);
        if (t->failed) return;
        if (t->i + len > t->n) { t_fail(t, "thrift: truncated binary"); return; }
        t->i += len;
        return;
    }
    case T_LIST: case T_SET: {
        uint8_t head = t_byte(t);
        uint64_t size = head >> 4;
        int et = head & 0x0F;
        if (size == 15) size = t_varint(t);
        for (uint64_t k = 0; k < size && !t->failed; k++) {
            if (et == T_BOOL_TRUE || et == T_BOOL_FALSE) t_byte(t);
            else t_skip(t, et);
        }
        return;
    }
    case T_MAP: {
        uint64_t size = t_varint(t);
        if (t->failed || size == 0) return;
        uint8_t kv = t_byte(t);
        int kt = kv >> 4, vt = kv & 0x0F;
        for (uint64_t k = 0; k < size && !t->failed; k++) {
            t_skip(t, kt);
            t_skip(t, vt);
        }
        return;
    }
    case T_STRUCT: {
        for (;;) {
            uint8_t head = t_byte(t);
            if (t->failed) return;
            if (head == 0) return;
            int ft = head & 0x0F;
            if (!(head >> 4)) t_zigzag(t);          /* long-form field id */
            if (ft == T_BOOL_TRUE || ft == T_BOOL_FALSE) continue;
            t_skip(t, ft);
        }
    }
    default:
        t_fail(t, "thrift: unsupported compact type");
    }
}

/* Copy one value verbatim: skip it in the input, memcpy the span. */
static void t_copy_verbatim(T *t, int ty) {
    size_t start = t->i;
    t_skip(t, ty);
    if (!t->failed && t->out)
        buf_put(t->out, t->in + start, t->i - start);
}

/* Does this (ctx, field, type) contain anything we must rewrite? */
static int needs_recurse(int ctx, int64_t fid, int ty) {
    switch (ctx) {
    case CTX_FMD: return fid == 4 && ty == T_LIST;
    case CTX_RG:  return fid == 1 && ty == T_LIST;
    case CTX_CC:  return fid == 3 && ty == T_STRUCT;
    default:      return 0;
    }
}

static int is_shift_field(int ctx, int64_t fid, int ty) {
    if (ty != T_I64) return 0;
    switch (ctx) {
    case CTX_RG:  return fid == 5;
    case CTX_CC:  return fid == 2;
    case CTX_CMD: return fid == 9 || fid == 10 || fid == 11 || fid == 14;
    default:      return 0;
    }
}

/* ColumnChunk page-index references (offset_index_offset/_length,
 * column_index_offset/_length) are DROPPED during rebase: segment
 * concatenation interleaves index sections, and mainstream readers
 * (parquet-go) shortcut-read all indexes as one contiguous region
 * before the footer -- non-contiguous references crash them. The
 * index bytes become dead bytes; row-group statistics survive. */
static int is_drop_field(int ctx, int64_t fid) {
    return ctx == CTX_CC && fid >= 4 && fid <= 7;
}

static int child_ctx(int ctx) {
    switch (ctx) {
    case CTX_FMD: return CTX_RG;
    case CTX_RG:  return CTX_CC;
    case CTX_CC:  return CTX_CMD;
    default:      return CTX_PLAIN;
    }
}

static void copy_struct(T *t, int ctx);

/* Copy a list whose elements need the rewriting walk. In CTX_FMD this
 * is the row_groups list: each element's OUTPUT bytes are also
 * appended to rg_slices (the combine machinery's raw material). */
static void copy_list_recurse(T *t, int ctx) {
    size_t hstart = t->i;
    uint8_t head = t_byte(t);
    uint64_t size = head >> 4;
    int et = head & 0x0F;
    if (size == 15) size = t_varint(t);
    if (t->failed) return;
    if (et != T_STRUCT) {                 /* row_groups/columns are structs */
        t_fail(t, "thrift: expected struct list on the offset path");
        return;
    }
    if (t->out) buf_put(t->out, t->in + hstart, t->i - hstart);
    int cctx = child_ctx(ctx);
    for (uint64_t k = 0; k < size && !t->failed; k++) {
        size_t ostart = t->out ? t->out->len : 0;
        copy_struct(t, cctx);
        if (ctx == CTX_FMD && t->rg_slices && t->out && !t->failed) {
            buf_put(t->rg_slices, t->out->p + ostart, t->out->len - ostart);
            t->rg_count++;
        }
    }
}

/* Canonical field header: short form when the delta fits. Needed
 * because dropped fields change the deltas of the fields after them. */
static void emit_fhead(Buf *out, int64_t fid, int wire_ty,
                       int64_t *out_last) {
    int64_t d = fid - *out_last;
    if (d > 0 && d <= 15)
        buf_byte(out, (uint8_t)((d << 4) | wire_ty));
    else {
        buf_byte(out, (uint8_t)wire_ty);
        w_zigzag(out, fid);
    }
    *out_last = fid;
}

static void copy_struct(T *t, int ctx) {
    int64_t last_id = 0, out_last = 0;
    for (;;) {
        uint8_t head = t_byte(t);
        if (t->failed) return;
        if (head == 0) {
            if (t->out) buf_byte(t->out, 0);
            return;
        }
        int delta = head >> 4;
        int ft = head & 0x0F;
        int64_t fid = delta ? last_id + delta : t_zigzag(t);
        if (t->failed) return;
        last_id = fid;
        if (is_drop_field(ctx, fid)) {
            if (ft != T_BOOL_TRUE && ft != T_BOOL_FALSE)
                t_skip(t, ft);
            continue;
        }
        if (t->out) emit_fhead(t->out, fid, ft, &out_last);
        if (ft == T_BOOL_TRUE || ft == T_BOOL_FALSE)
            continue;                     /* type IS the value */
        if (ctx == CTX_FMD && fid == 3
            && (ft == T_I16 || ft == T_I32 || ft == T_I64)) {
            size_t vstart = t->i;
            t->num_rows = t_zigzag(t);
            t->have_rows = 1;
            if (t->out && !t->failed)
                buf_put(t->out, t->in + vstart, t->i - vstart);
            continue;
        }
        if (is_shift_field(ctx, fid, ft)) {
            int64_t v = t_zigzag(t);
            if (t->failed) return;
            if (t->out) w_zigzag(t->out, v + t->delta);
            continue;
        }
        if (needs_recurse(ctx, fid, ft)) {
            if (ft == T_LIST) copy_list_recurse(t, ctx);
            else copy_struct(t, child_ctx(ctx));
            continue;
        }
        t_copy_verbatim(t, ft);
        if (t->failed) return;
    }
}

/* Shift a bare FileMetaData footer; optionally capture row-group
 * element slices and num_rows. Returns 0 or -1 (err set). */
static int shift_fmd(const uint8_t *thrift, size_t len, int64_t delta,
                     Buf *out, Buf *rg_slices, uint32_t *rg_count,
                     int64_t *num_rows, char *err, size_t errlen) {
    T t = { .in = thrift, .n = len, .out = out, .delta = delta,
            .rg_slices = rg_slices, .err = err, .errlen = errlen };
    copy_struct(&t, CTX_FMD);
    if (t.failed) return -1;
    if (t.i != t.n) {
        set_err(err, errlen, "thrift: trailing bytes after FileMetaData "
                "-- refusing to rebase a footer I don't fully understand");
        return -1;
    }
    if (!t.have_rows) {
        set_err(err, errlen, "thrift: FileMetaData has no num_rows");
        return -1;
    }
    if (rg_count) *rg_count = t.rg_count;
    if (num_rows) *num_rows = t.num_rows;
    return 0;
}

/* Rebuild a combined footer from a TEMPLATE (any group's full shifted
 * footer: supplies schema, version, created_by, ...) with num_rows
 * replaced by `total_rows` and the row_groups list replaced by
 * `splice` (`splice_count` pre-encoded RowGroup structs). */
static int build_combined(const uint8_t *tpl, size_t tpl_len,
                          int64_t total_rows, const uint8_t *splice,
                          size_t splice_len, uint32_t splice_count,
                          Buf *out, char *err, size_t errlen) {
    T t = { .in = tpl, .n = tpl_len, .out = out,
            .err = err, .errlen = errlen };
    int64_t last_id = 0;
    int saw_rows = 0, saw_rgs = 0;
    for (;;) {
        size_t hstart = t.i;
        uint8_t head = t_byte(&t);
        if (t.failed) return -1;
        if (head == 0) {
            buf_byte(out, 0);
            break;
        }
        int delta = head >> 4;
        int ft = head & 0x0F;
        int64_t fid = delta ? last_id + delta : t_zigzag(&t);
        if (t.failed) return -1;
        last_id = fid;
        buf_put(out, t.in + hstart, t.i - hstart);
        if (ft == T_BOOL_TRUE || ft == T_BOOL_FALSE) continue;
        if (fid == 3 && (ft == T_I16 || ft == T_I32 || ft == T_I64)) {
            t_skip(&t, ft);                     /* template's own count */
            w_zigzag(out, total_rows);
            saw_rows = 1;
            continue;
        }
        if (fid == 4 && ft == T_LIST) {
            t_skip(&t, ft);                     /* template's own list */
            if (splice_count < 15)
                buf_byte(out, (uint8_t)((splice_count << 4) | T_STRUCT));
            else {
                buf_byte(out, 0xF0 | T_STRUCT);
                w_varint(out, splice_count);
            }
            buf_put(out, splice, splice_len);
            saw_rgs = 1;
            continue;
        }
        size_t vstart = t.i;
        t_skip(&t, ft);
        if (t.failed) return -1;
        buf_put(out, t.in + vstart, t.i - vstart);
    }
    if (t.i != t.n) {
        set_err(err, errlen, "thrift: trailing bytes after template");
        return -1;
    }
    if (!saw_rows || !saw_rgs) {
        set_err(err, errlen, "thrift: template lacks num_rows/row_groups");
        return -1;
    }
    return 0;
}

/* ---- mini parquet file intake ----------------------------------------- */

static const uint8_t MAGIC[4] = { 'P', 'A', 'R', '1' };

/* Split a complete parquet file into pages + footer thrift. */
static int split_mini(const uint8_t *mini, size_t len,
                      const uint8_t **pages, size_t *pages_len,
                      const uint8_t **thrift, size_t *thrift_len,
                      char *err, size_t errlen) {
    if (len < 12 || memcmp(mini, MAGIC, 4) != 0
        || memcmp(mini + len - 4, MAGIC, 4) != 0) {
        set_err(err, errlen, "mini file: bad or missing PAR1 magic");
        return -1;
    }
    uint32_t flen = rd_u32le(mini + len - 8);
    if ((size_t)flen + 12 > len) {
        set_err(err, errlen, "mini file: footer length exceeds file");
        return -1;
    }
    *pages = mini + 4;
    *pages_len = len - 8 - flen - 4;
    *thrift = mini + len - 8 - flen;
    *thrift_len = flen;
    return 0;
}

/* ---- writer ----------------------------------------------------------- */

/* The file, as the store holds it while it is written, is a list of
 * objects in file order and a footer:
 *
 *   chunks            >= chunk_bytes of final row groups each
 *   loose groups      final row groups not yet in a chunk, one object each
 *   unfinished        the rows of the row group still being filled:
 *                     one object for each checkpoint's new rows, merged
 *                     in steps
 *
 * An object is written once and never changed. The cost of a checkpoint
 * follows the new rows: it stores one unfinished object and the
 * checkpoint object (this list and the footer), never a row a stored
 * object already holds. A merge replaces the unfinished objects from
 * some row on by one object of the same rows, so it only ever replaces
 * the END of the list, and no object that stays ever moves. */

#define PQC_LEVELS 24           /* size steps of the unfinished objects */

typedef struct {
    char name[PQC_NAME_MAX];
    uint64_t length;
    uint32_t crc;
} ManEntry;

typedef struct {                /* a final row group */
    uint8_t *pages;             /* NULL once a chunk holds it          */
    size_t pages_len;
    Buf rg;                     /* shifted RowGroup element bytes      */
    uint32_t rg_count;
    int64_t rows;
    int stored;                 /* in the store as an object of its own */
    ManEntry obj;               /* that object, while no chunk holds it */
} Group;

typedef struct {                /* an unfinished object */
    ManEntry obj;
    Buf rg;                     /* shifted RowGroup element bytes      */
    uint32_t rg_count;
    int64_t rows;
    uint64_t first_row;         /* file row of its first row           */
    uint64_t pages_len;
    uint32_t level;             /* its size step: 0 = one checkpoint   */
} Unf;

enum { PEND_NONE = 0, PEND_CHUNK, PEND_GROUP, PEND_UNF };

struct PqcWriter {
    char name[PQC_NAME_MAX];
    uint64_t chunk_bytes;
    uint64_t logical_end;       /* end of the final groups; starts
                                   after the leading magic */
    uint64_t rows_final;
    uint32_t objects_made;      /* numbers the object names */

    Group *groups;
    size_t n_groups, cap_groups;
    size_t first_loose;         /* first group not in a stored chunk */
    uint64_t loose_bytes;

    Unf *unf;                   /* in file order; levels never rise */
    size_t n_unf, cap_unf;
    uint64_t unf_rows;
    /* footer bytes the objects of each size step have cost: every
     * checkpoint writes the footer, which has an entry for each */
    uint64_t paid[PQC_LEVELS];

    int span_set;               /* pqc_next_span's answer, for the */
    size_t span_from;           /* pqc_add_unfinished that follows  */
    uint64_t span_first_row, span_rows;
    uint32_t span_level;

    int has_new;                /* an unfinished object not yet stored */
    Unf new_unf;
    uint8_t *new_pages;
    size_t new_from;            /* it replaces unf[new_from ..] */

    uint8_t *tpl;               /* the first object's full shifted footer */
    size_t tpl_len;

    ManEntry *man;              /* the chunks */
    size_t n_man, cap_man;

    Buf pending;                /* the object awaiting the host's store */
    uint32_t pending_crc;
    int pending_kind;
    size_t pending_take;        /* PEND_CHUNK: the groups it holds */
    size_t pending_group;       /* PEND_GROUP: which group */
    char pending_name[PQC_NAME_MAX];
};

PqcWriter *pqc_writer_new(const char *final_name, uint64_t chunk_bytes,
                          char *err, size_t errlen) {
    size_t nlen = strlen(final_name);
    if (nlen < 9 || strcmp(final_name + nlen - 8, ".parquet") != 0) {
        set_err(err, errlen, "final_name must end in .parquet");
        return NULL;
    }
    if (nlen + 16 > PQC_NAME_MAX) {
        set_err(err, errlen, "final_name too long");
        return NULL;
    }
    PqcWriter *w = calloc(1, sizeof(*w));
    if (!w) { set_err(err, errlen, "out of memory"); return NULL; }
    strcpy(w->name, final_name);
    w->chunk_bytes = chunk_bytes;
    w->logical_end = 4;
    return w;
}

static void drop_unfinished(PqcWriter *w) {
    for (size_t i = 0; i < w->n_unf; i++) buf_free(&w->unf[i].rg);
    w->n_unf = 0;
    w->unf_rows = 0;
    memset(w->paid, 0, sizeof(w->paid));
    w->span_set = 0;
    if (w->has_new) {
        buf_free(&w->new_unf.rg);
        free(w->new_pages);
        w->new_pages = NULL;
        w->has_new = 0;
    }
    if (w->pending_kind == PEND_UNF) {
        w->pending_kind = PEND_NONE;
        w->pending.len = 0;
    }
}

void pqc_writer_free(PqcWriter *w) {
    if (!w) return;
    drop_unfinished(w);
    for (size_t i = 0; i < w->n_groups; i++) {
        free(w->groups[i].pages);
        buf_free(&w->groups[i].rg);
    }
    free(w->groups);
    free(w->unf);
    free(w->tpl);
    free(w->man);
    buf_free(&w->pending);
    free(w);
}

uint64_t pqc_rows_final(const PqcWriter *w) { return w->rows_final; }
uint64_t pqc_rows_unfinished(const PqcWriter *w) { return w->unf_rows; }
uint32_t pqc_chunks_cut(const PqcWriter *w) { return (uint32_t)w->n_man; }
uint64_t pqc_logical_end(const PqcWriter *w) { return w->logical_end; }

void pqc_drop_unfinished(PqcWriter *w) { drop_unfinished(w); }

int64_t pqc_add_group(PqcWriter *w, const uint8_t *mini, size_t len,
                      char *err, size_t errlen) {
    const uint8_t *pages, *thrift;
    size_t pages_len, thrift_len;
    if (split_mini(mini, len, &pages, &pages_len, &thrift, &thrift_len,
                   err, errlen))
        return -1;
    Buf footer = {0}, slices = {0};
    uint32_t rg_count = 0;
    int64_t rows = 0;
    if (shift_fmd(thrift, thrift_len, (int64_t)w->logical_end - 4,
                  &footer, &slices, &rg_count, &rows, err, errlen)) {
        buf_free(&footer);
        buf_free(&slices);
        return -1;
    }
    if (w->n_groups == w->cap_groups) {
        size_t cap = w->cap_groups ? w->cap_groups * 2 : 16;
        Group *g = realloc(w->groups, cap * sizeof(Group));
        if (!g) { set_err(err, errlen, "out of memory");
                  buf_free(&footer); buf_free(&slices); return -1; }
        w->groups = g;
        w->cap_groups = cap;
    }
    Group *g = &w->groups[w->n_groups++];
    memset(g, 0, sizeof(*g));
    g->pages = malloc(pages_len ? pages_len : 1);
    if (!g->pages) { set_err(err, errlen, "out of memory");
                     w->n_groups--; buf_free(&footer); buf_free(&slices);
                     return -1; }
    memcpy(g->pages, pages, pages_len);
    g->pages_len = pages_len;
    g->rg = slices;             /* ownership moves */
    g->rg_count = rg_count;
    g->rows = rows;
    if (!w->tpl) {              /* the first object: keep its full footer */
        w->tpl = footer.p;
        w->tpl_len = footer.len;
    } else {
        buf_free(&footer);
    }
    w->logical_end += pages_len;
    w->rows_final += (uint64_t)rows;
    w->loose_bytes += pages_len;
    /* the final groups hold the rows of the unfinished objects now */
    drop_unfinished(w);
    return rows;
}

void pqc_next_span(PqcWriter *w, uint64_t new_rows,
                   uint64_t *first_row, uint64_t *rows) {
    uint64_t n[PQC_LEVELS] = {0}, bytes[PQC_LEVELS] = {0};
    for (size_t i = 0; i < w->n_unf; i++) {
        n[w->unf[i].level]++;
        bytes[w->unf[i].level] += w->unf[i].obj.length;
    }
    n[0]++;                     /* the object the new rows would be */
    int merge = -1;             /* the highest step whose run merges */
    for (int k = PQC_LEVELS - 1; k >= 0; k--) {
        if (n[k] >= 2 && (w->paid[k] >= bytes[k] || n[k] >= PQC_RUN_LIMIT)) {
            merge = k;
            break;
        }
    }
    uint64_t end = w->rows_final + w->unf_rows + new_rows;
    w->span_from = w->n_unf;
    w->span_first_row = w->rows_final + w->unf_rows;
    w->span_level = 0;
    if (merge >= 0) {
        /* levels never rise along the list: the run of step `merge`
         * begins at the first object of that step or below */
        for (size_t i = 0; i < w->n_unf; i++) {
            if ((int)w->unf[i].level <= merge) {
                w->span_from = i;
                w->span_first_row = w->unf[i].first_row;
                break;
            }
        }
        w->span_level = merge + 1 < PQC_LEVELS ? (uint32_t)merge + 1
                                               : PQC_LEVELS - 1;
    }
    w->span_rows = end - w->span_first_row;
    w->span_set = 1;
    *first_row = w->span_first_row;
    *rows = w->span_rows;
}

int64_t pqc_add_unfinished(PqcWriter *w, const uint8_t *mini, size_t len,
                           char *err, size_t errlen) {
    if (!w->span_set) {
        set_err(err, errlen, "pqc_add_unfinished without pqc_next_span: "
                "the core says which rows the object holds");
        return -1;
    }
    if (w->has_new) {
        set_err(err, errlen, "an unfinished object is not stored yet");
        return -1;
    }
    const uint8_t *pages, *thrift;
    size_t pages_len, thrift_len;
    if (split_mini(mini, len, &pages, &pages_len, &thrift, &thrift_len,
                   err, errlen))
        return -1;
    uint64_t pos = w->logical_end;
    for (size_t i = 0; i < w->span_from; i++) pos += w->unf[i].pages_len;
    Buf footer = {0}, slices = {0};
    uint32_t rg_count = 0;
    int64_t rows = 0;
    if (shift_fmd(thrift, thrift_len, (int64_t)pos - 4, &footer, &slices,
                  &rg_count, &rows, err, errlen)) {
        buf_free(&footer);
        buf_free(&slices);
        return -1;
    }
    if ((uint64_t)rows != w->span_rows) {
        set_err(err, errlen, "the unfinished object does not hold the "
                "rows pqc_next_span asked for");
        buf_free(&footer);
        buf_free(&slices);
        return -1;
    }
    w->new_pages = malloc(pages_len ? pages_len : 1);
    if (!w->new_pages) {
        set_err(err, errlen, "out of memory");
        buf_free(&footer);
        buf_free(&slices);
        return -1;
    }
    memcpy(w->new_pages, pages, pages_len);
    memset(&w->new_unf, 0, sizeof(w->new_unf));
    w->new_unf.rg = slices;     /* ownership moves */
    w->new_unf.rg_count = rg_count;
    w->new_unf.rows = rows;
    w->new_unf.first_row = w->span_first_row;
    w->new_unf.pages_len = pages_len;
    w->new_unf.level = w->span_level;
    w->new_from = w->span_from;
    w->has_new = 1;
    w->span_set = 0;
    if (!w->tpl) {
        w->tpl = footer.p;
        w->tpl_len = footer.len;
    } else {
        buf_free(&footer);
    }
    return rows;
}

/* Whether a stored object is still owed: the host must drain
 * pqc_next_object before a checkpoint may claim anything. */
static int owes_object(const PqcWriter *w) {
    if (w->pending_kind != PEND_NONE || w->has_new) return 1;
    if (w->loose_bytes >= w->chunk_bytes && w->first_loose < w->n_groups)
        return 1;
    for (size_t i = w->first_loose; i < w->n_groups; i++)
        if (!w->groups[i].stored) return 1;
    return 0;
}

/* Choose the next object to store and put its bytes in w->pending. */
static int cut_pending(PqcWriter *w) {
    w->pending.len = 0;
    if (w->loose_bytes >= w->chunk_bytes && w->first_loose < w->n_groups) {
        /* a chunk: final groups up to chunk_bytes. A group not yet
         * stored as an object of its own is written only here. */
        size_t take = 0;
        uint64_t size = 0;
        while (size < w->chunk_bytes
               && w->first_loose + take < w->n_groups) {
            size += w->groups[w->first_loose + take].pages_len;
            take++;
        }
        if (w->n_man == 0)
            if (buf_put(&w->pending, MAGIC, 4)) return -1;
        for (size_t k = 0; k < take; k++) {
            Group *g = &w->groups[w->first_loose + k];
            if (buf_put(&w->pending, g->pages, g->pages_len)) return -1;
        }
        w->pending_take = take;
        w->pending_kind = PEND_CHUNK;
    } else {
        size_t i = w->first_loose;
        while (i < w->n_groups && w->groups[i].stored) i++;
        if (i < w->n_groups) {  /* a final group, as its own object */
            if (w->n_man == 0 && i == 0)
                if (buf_put(&w->pending, MAGIC, 4)) return -1;
            if (buf_put(&w->pending, w->groups[i].pages,
                        w->groups[i].pages_len)) return -1;
            w->pending_group = i;
            w->pending_kind = PEND_GROUP;
        } else if (w->has_new) {
            if (w->n_man == 0 && w->n_groups == 0 && w->new_from == 0)
                if (buf_put(&w->pending, MAGIC, 4)) return -1;
            if (buf_put(&w->pending, w->new_pages, w->new_unf.pages_len))
                return -1;
            w->pending_kind = PEND_UNF;
        } else {
            return 0;
        }
    }
    w->pending_crc = pqc_crc32(w->pending.p, w->pending.len);
    snprintf(w->pending_name, PQC_NAME_MAX, "%s.chunks/%06u", w->name,
             (unsigned)(w->objects_made + 1));
    return 0;
}

int pqc_next_object(PqcWriter *w, char name[PQC_NAME_MAX],
                    const uint8_t **data, uint64_t *len, uint32_t *crc) {
    if (w->pending_kind == PEND_NONE && cut_pending(w)) {
        w->pending_kind = PEND_NONE;
        return -1;
    }
    if (w->pending_kind == PEND_NONE) return 0;
    memcpy(name, w->pending_name, PQC_NAME_MAX);
    *data = w->pending.p;
    *len = w->pending.len;
    *crc = w->pending_crc;
    return 1;
}

void pqc_object_stored(PqcWriter *w) {
    if (w->pending_kind == PEND_NONE) return;
    ManEntry e;
    memcpy(e.name, w->pending_name, PQC_NAME_MAX);
    e.length = w->pending.len;
    e.crc = w->pending_crc;
    if (w->pending_kind == PEND_CHUNK) {
        if (w->n_man == w->cap_man) {
            size_t cap = w->cap_man ? w->cap_man * 2 : 8;
            ManEntry *m = realloc(w->man, cap * sizeof(ManEntry));
            if (!m) return;     /* the same object is given again */
            w->man = m;
            w->cap_man = cap;
        }
        w->man[w->n_man++] = e;
        for (size_t k = 0; k < w->pending_take; k++) {
            Group *g = &w->groups[w->first_loose + k];
            w->loose_bytes -= g->pages_len;
            free(g->pages);
            g->pages = NULL;
            g->pages_len = 0;   /* rg slices + rows stay: footers need them */
        }
        w->first_loose += w->pending_take;
    } else if (w->pending_kind == PEND_GROUP) {
        w->groups[w->pending_group].stored = 1;
        w->groups[w->pending_group].obj = e;
    } else {                    /* PEND_UNF: it replaces unf[new_from ..] */
        if (w->new_from == w->cap_unf) {
            size_t cap = w->cap_unf ? w->cap_unf * 2 : 16;
            Unf *u = realloc(w->unf, cap * sizeof(Unf));
            if (!u) return;     /* the same object is given again */
            w->unf = u;
            w->cap_unf = cap;
        }
        for (size_t i = w->new_from; i < w->n_unf; i++)
            buf_free(&w->unf[i].rg);
        w->new_unf.obj = e;
        w->unf[w->new_from] = w->new_unf;       /* rg ownership moves */
        w->n_unf = w->new_from + 1;
        w->unf_rows = 0;
        for (size_t i = 0; i < w->n_unf; i++)
            w->unf_rows += (uint64_t)w->unf[i].rows;
        /* the runs below its step are gone; they owe nothing more */
        for (uint32_t k = 0; k < w->new_unf.level; k++) w->paid[k] = 0;
        free(w->new_pages);
        w->new_pages = NULL;
        w->has_new = 0;
    }
    w->objects_made++;
    w->pending_kind = PEND_NONE;
    w->pending.len = 0;
}

static uint8_t *encode_ckpt(const char *name, uint64_t watermark,
                            const ManEntry *man, size_t n_man,
                            const uint8_t *data, size_t data_len,
                            uint64_t *out_len, char *err, size_t errlen) {
    Buf body = {0};
    size_t nlen = strlen(name);
    buf_u16le(&body, (uint16_t)nlen);
    buf_put(&body, name, nlen);
    buf_u64le(&body, watermark);
    size_t man_bytes = 4;
    for (size_t i = 0; i < n_man; i++)
        man_bytes += 2 + strlen(man[i].name) + 8 + 4;
    /* data_offset counts from the start of the whole object */
    uint64_t data_offset = 4 + 1 + 4 + (2 + nlen + 8) + 8 + man_bytes;
    buf_u64le(&body, data_offset);
    buf_u32le(&body, (uint32_t)n_man);
    for (size_t i = 0; i < n_man; i++) {
        size_t cl = strlen(man[i].name);
        buf_u16le(&body, (uint16_t)cl);
        buf_put(&body, man[i].name, cl);
        buf_u64le(&body, man[i].length);
        buf_u32le(&body, man[i].crc);
    }
    if (buf_put(&body, data, data_len)) {
        set_err(err, errlen, "out of memory");
        buf_free(&body);
        return NULL;
    }
    Buf out = {0};
    buf_put(&out, "PQC2", 4);
    buf_byte(&out, PQC_VERSION);
    buf_u32le(&out, pqc_crc32(body.p, body.len));
    if (buf_put(&out, body.p, body.len)) {
        set_err(err, errlen, "out of memory");
        buf_free(&body);
        buf_free(&out);
        return NULL;
    }
    buf_free(&body);
    *out_len = out.len;
    return out.p;
}

uint8_t *pqc_build_checkpoint(PqcWriter *w, const uint8_t *empty_mini,
                              size_t empty_len, uint64_t *out_len,
                              uint64_t *out_watermark,
                              char *err, size_t errlen) {
    if (owes_object(w)) {
        set_err(err, errlen, "an object is not stored -- a checkpoint "
                "must never claim bytes that are not durable");
        return NULL;
    }
    /* No object at all: the mini file IS the data region. */
    if (w->n_groups == 0 && w->n_man == 0 && w->n_unf == 0) {
        if (!empty_mini) {
            set_err(err, errlen, "empty writer: pass an (empty) parquet "
                    "file for a zero-row checkpoint");
            return NULL;
        }
        const uint8_t *tp, *tt;
        size_t tpl_, ttl;
        if (split_mini(empty_mini, empty_len, &tp, &tpl_, &tt, &ttl,
                       err, errlen))
            return NULL;
        int64_t trows = 0;
        if (shift_fmd(tt, ttl, 0, NULL, NULL, NULL, &trows, err, errlen))
            return NULL;
        uint8_t *ck = encode_ckpt(w->name, (uint64_t)trows, w->man, 0,
                                  empty_mini, empty_len, out_len,
                                  err, errlen);
        if (ck && out_watermark) *out_watermark = (uint64_t)trows;
        return ck;
    }
    /* The list: chunks, loose groups, unfinished objects, in file
     * order. The footer: every row group of them. */
    size_t n_list = w->n_man + (w->n_groups - w->first_loose) + w->n_unf;
    ManEntry *list = malloc((n_list ? n_list : 1) * sizeof(ManEntry));
    Buf splice = {0}, footer = {0}, data = {0};
    uint8_t *ck = NULL;
    if (!list) { set_err(err, errlen, "out of memory"); return NULL; }
    size_t at = 0;
    for (size_t i = 0; i < w->n_man; i++) list[at++] = w->man[i];
    for (size_t i = w->first_loose; i < w->n_groups; i++)
        list[at++] = w->groups[i].obj;
    for (size_t i = 0; i < w->n_unf; i++) list[at++] = w->unf[i].obj;
    uint32_t total_rg = 0;
    int64_t total_rows = 0;
    for (size_t i = 0; i < w->n_groups; i++) {
        if (buf_put(&splice, w->groups[i].rg.p, w->groups[i].rg.len))
            goto oom;
        total_rg += w->groups[i].rg_count;
        total_rows += w->groups[i].rows;
    }
    for (size_t i = 0; i < w->n_unf; i++) {
        if (buf_put(&splice, w->unf[i].rg.p, w->unf[i].rg.len)) goto oom;
        total_rg += w->unf[i].rg_count;
        total_rows += w->unf[i].rows;
    }
    if (build_combined(w->tpl, w->tpl_len, total_rows, splice.p,
                       splice.len, total_rg, &footer, err, errlen))
        goto done;
    if (buf_put(&data, footer.p, footer.len)
        || buf_u32le(&data, (uint32_t)footer.len)
        || buf_put(&data, MAGIC, 4))
        goto oom;
    ck = encode_ckpt(w->name, (uint64_t)total_rows, list, n_list, data.p,
                     data.len, out_len, err, errlen);
    if (ck) {
        if (out_watermark) *out_watermark = (uint64_t)total_rows;
        /* each unfinished object has cost its footer entry once more */
        for (size_t i = 0; i < w->n_unf; i++)
            w->paid[w->unf[i].level] += w->unf[i].rg.len;
    }
    goto done;
oom:
    set_err(err, errlen, "out of memory");
done:
    free(list);
    buf_free(&splice);
    buf_free(&footer);
    buf_free(&data);
    return ck;
}

/* ---- reader ----------------------------------------------------------- */

struct PqcReader {
    char name[PQC_NAME_MAX];
    uint64_t watermark;
    ManEntry *man;
    uint32_t n_man;
    uint64_t data_offset, data_len;
};

PqcReader *pqc_reader_open(const uint8_t *ckpt, size_t len,
                           char *err, size_t errlen) {
    if (len < 9 || memcmp(ckpt, "PQC2", 4) != 0) {
        set_err(err, errlen, "bad checkpoint magic");
        return NULL;
    }
    if (ckpt[4] != PQC_VERSION) {
        set_err(err, errlen, "unsupported checkpoint version");
        return NULL;
    }
    if (pqc_crc32(ckpt + 9, len - 9) != rd_u32le(ckpt + 5)) {
        set_err(err, errlen, "checkpoint CRC mismatch -- corrupt, refusing");
        return NULL;
    }
    const uint8_t *p = ckpt + 9, *end = ckpt + len;
    if (p + 2 > end) goto trunc;
    uint16_t nlen = rd_u16le(p);
    p += 2;
    if (nlen >= PQC_NAME_MAX || p + nlen + 8 + 8 + 4 > end) goto trunc;
    PqcReader *r = calloc(1, sizeof(*r));
    if (!r) { set_err(err, errlen, "out of memory"); return NULL; }
    memcpy(r->name, p, nlen);
    p += nlen;
    r->watermark = rd_u64le(p); p += 8;
    r->data_offset = rd_u64le(p); p += 8;
    r->n_man = rd_u32le(p); p += 4;
    if (r->n_man) {
        r->man = calloc(r->n_man, sizeof(ManEntry));
        if (!r->man) { free(r); set_err(err, errlen, "out of memory");
                       return NULL; }
    }
    for (uint32_t i = 0; i < r->n_man; i++) {
        if (p + 2 > end) goto trunc_r;
        uint16_t cl = rd_u16le(p);
        p += 2;
        if (cl >= PQC_NAME_MAX || p + cl + 12 > end) goto trunc_r;
        memcpy(r->man[i].name, p, cl);
        p += cl;
        r->man[i].length = rd_u64le(p); p += 8;
        r->man[i].crc = rd_u32le(p); p += 4;
    }
    if (r->data_offset != (uint64_t)(p - ckpt)) {
        set_err(err, errlen, "checkpoint data_offset inconsistent");
        pqc_reader_free(r);
        return NULL;
    }
    r->data_len = (uint64_t)(end - p);
    return r;
trunc_r:
    pqc_reader_free(r);
trunc:
    set_err(err, errlen, "checkpoint truncated");
    return NULL;
}

void pqc_reader_free(PqcReader *r) {
    if (!r) return;
    free(r->man);
    free(r);
}

const char *pqc_reader_name(const PqcReader *r) { return r->name; }
uint64_t pqc_reader_watermark(const PqcReader *r) { return r->watermark; }
uint32_t pqc_reader_n_chunks(const PqcReader *r) { return r->n_man; }

int pqc_reader_chunk(const PqcReader *r, uint32_t idx, const char **name,
                     uint64_t *length, uint32_t *crc) {
    if (idx >= r->n_man) return -1;
    *name = r->man[idx].name;
    *length = r->man[idx].length;
    *crc = r->man[idx].crc;
    return 0;
}

void pqc_reader_data(const PqcReader *r, uint64_t *offset,
                     uint64_t *length) {
    *offset = r->data_offset;
    *length = r->data_len;
}

/* ---- test hook -------------------------------------------------------- */

uint8_t *pqc_shift_footer(const uint8_t *thrift, size_t len,
                          int64_t delta, size_t *out_len,
                          char *err, size_t errlen) {
    Buf out = {0};
    if (shift_fmd(thrift, len, delta, &out, NULL, NULL, NULL,
                  err, errlen)) {
        buf_free(&out);
        return NULL;
    }
    *out_len = out.len;
    return out.p;
}

void pqc_free(void *p) { free(p); }
