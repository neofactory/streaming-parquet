/* sp_catalog.c -- the stream core's catalog: the list of a stream's
 * finished files, as a DuckLake 1.0 catalog in a SQLite database. See
 * sp.h, THE CATALOG. No disk, no network, no clock: the host brings the
 * catalog's bytes and each file's footer, and stores the bytes it gets
 * back. */
#include "sp.h"

#include <math.h>
#include <sqlite3.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void fail(char *err, size_t errlen, const char *fmt, ...) {
    if (!err || errlen == 0) return;
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(err, errlen, fmt, ap);
    va_end(ap);
}

static char *copy_bytes(const uint8_t *s, size_t n) {
    char *p = malloc(n + 1);
    if (!p) return NULL;
    if (n) memcpy(p, s, n);
    p[n] = 0;
    return p;
}

/* ---- thrift, compact protocol: what a parquet footer is written in ------ */

enum { T_STOP = 0, T_TRUE = 1, T_FALSE = 2, T_BYTE = 3, T_I16 = 4, T_I32 = 5,
       T_I64 = 6, T_DOUBLE = 7, T_BINARY = 8, T_LIST = 9, T_SET = 10,
       T_MAP = 11, T_STRUCT = 12 };

#define TR_DEPTH 48

typedef struct {
    const uint8_t *p, *end;
    int bad;
    int depth;
} Tr;

static uint64_t tr_varint(Tr *t) {
    uint64_t v = 0;
    for (int shift = 0; shift < 64; shift += 7) {
        if (t->p >= t->end) { t->bad = 1; return 0; }
        uint8_t b = *t->p++;
        v |= (uint64_t)(b & 0x7f) << shift;
        if (!(b & 0x80)) return v;
    }
    t->bad = 1;
    return 0;
}

static int64_t tr_int(Tr *t) {
    uint64_t v = tr_varint(t);
    return (int64_t)(v >> 1) ^ -(int64_t)(v & 1);
}

static const uint8_t *tr_binary(Tr *t, size_t *len) {
    uint64_t n = tr_varint(t);
    if (t->bad || n > (uint64_t)(t->end - t->p)) {
        t->bad = 1;
        *len = 0;
        return t->p;
    }
    const uint8_t *s = t->p;
    t->p += n;
    *len = (size_t)n;
    return s;
}

/* The next field of a struct: 1 with its id and type, 0 at the stop. */
static int tr_field(Tr *t, int *id, int *type) {
    if (t->bad || t->p >= t->end) { t->bad = 1; return 0; }
    uint8_t b = *t->p++;
    if (b == T_STOP) return 0;
    *type = b & 0x0f;
    int delta = b >> 4;
    if (delta) *id += delta;
    else *id = (int)tr_int(t);
    return !t->bad;
}

/* A list's header: its count, and the type of its elements. */
static int64_t tr_list(Tr *t, int *etype) {
    if (t->p >= t->end) { t->bad = 1; return 0; }
    uint8_t b = *t->p++;
    *etype = b & 0x0f;
    uint64_t n = b >> 4;
    if (n == 15) n = tr_varint(t);
    /* each element is a byte or more */
    if (t->bad || n > (uint64_t)(t->end - t->p)) { t->bad = 1; return 0; }
    return (int64_t)n;
}

static void tr_skip(Tr *t, int type) {
    if (t->bad) return;
    if (++t->depth > TR_DEPTH) { t->bad = 1; return; }
    switch (type) {
    case T_TRUE: case T_FALSE: break;
    case T_BYTE:
        if (t->p >= t->end) t->bad = 1; else t->p++;
        break;
    case T_I16: case T_I32: case T_I64: tr_varint(t); break;
    case T_DOUBLE:
        if (t->end - t->p < 8) t->bad = 1; else t->p += 8;
        break;
    case T_BINARY: { size_t n; tr_binary(t, &n); break; }
    case T_LIST: case T_SET: {
        int et;
        int64_t n = tr_list(t, &et);
        for (int64_t i = 0; i < n && !t->bad; i++) {
            if (et == T_TRUE || et == T_FALSE) {       /* a byte each */
                if (t->p >= t->end) t->bad = 1; else t->p++;
            } else {
                tr_skip(t, et);
            }
        }
        break;
    }
    case T_MAP: {
        uint64_t n = tr_varint(t);
        if (n == 0) break;
        if (t->p >= t->end || n > (uint64_t)(t->end - t->p)) { t->bad = 1; break; }
        uint8_t kv = *t->p++;
        for (uint64_t i = 0; i < n && !t->bad; i++) {
            tr_skip(t, kv >> 4);
            tr_skip(t, kv & 0x0f);
        }
        break;
    }
    case T_STRUCT: {
        int id = 0, ft;
        while (tr_field(t, &id, &ft)) tr_skip(t, ft);
        break;
    }
    default: t->bad = 1;
    }
    t->depth--;
}

/* ---- a parquet footer, as far as the catalog needs it ------------------- */

enum { PQ_BOOLEAN = 0, PQ_INT32, PQ_INT64, PQ_INT96, PQ_FLOAT, PQ_DOUBLE,
       PQ_BYTE_ARRAY, PQ_FIXED };

/* What a value is, for its text and its order. */
typedef enum {
    K_NONE = 0,     /* no minimum or maximum is kept */
    K_BOOL, K_INT, K_UINT, K_FLOAT, K_DOUBLE, K_TEXT, K_BLOB, K_DATE,
    K_TIME, K_TIMESTAMP, K_DECIMAL
} Kind;

enum { UNIT_MILLIS = 1, UNIT_MICROS = 2, UNIT_NANOS = 3 };

typedef struct {
    char *name;
    int physical;             /* PQ_*, -1 for a group */
    int type_length;
    int repeated;
    int num_children;
    int converted;            /* ConvertedType, -1 for none */
    int scale, precision;
    int logical;              /* the LogicalType's field id, 0 for none */
    int utc, unit, bits, is_signed;
    int parent;               /* index of the group above, -1 at the top */
    /* what the catalog calls it */
    char type[40];            /* the DuckLake type; "" when not taken */
    int kind;
    int list_wrapper;         /* the repeated group inside a list */
    int64_t column_id;        /* in the catalog, once matched */
    /* a leaf's numbers, over all row groups */
    int has_chunk;
    int64_t values, nulls, size;
    int has_nulls;
    uint8_t *min, *max;
    size_t min_len, max_len;
    int has_min, has_max, no_range;
} PqNode;

typedef struct {
    PqNode *nodes;
    int n;
    int *leaves;              /* indices of the leaf nodes, in file order */
    int n_leaves;
    int64_t num_rows;
    int64_t row_groups;
    char *run;                /* the key sp_run */
    int64_t first_row;        /* the key sp_base_seq */
    int has_first_row;
} PqFooter;

static void footer_free(PqFooter *f) {
    for (int i = 0; i < f->n; i++) {
        free(f->nodes[i].name);
        free(f->nodes[i].min);
        free(f->nodes[i].max);
    }
    free(f->nodes);
    free(f->leaves);
    free(f->run);
    memset(f, 0, sizeof *f);
}

static void read_logical(Tr *t, PqNode *n) {
    int id = 0, ft;
    while (tr_field(t, &id, &ft)) {
        if (ft != T_STRUCT) { tr_skip(t, ft); continue; }
        n->logical = id;
        int id2 = 0, ft2;
        while (tr_field(t, &id2, &ft2)) {
            if (id == 5 && id2 == 1 && ft2 == T_I32) n->scale = (int)tr_int(t);
            else if (id == 5 && id2 == 2 && ft2 == T_I32) n->precision = (int)tr_int(t);
            else if ((id == 7 || id == 8) && id2 == 1
                     && (ft2 == T_TRUE || ft2 == T_FALSE)) n->utc = ft2 == T_TRUE;
            else if ((id == 7 || id == 8) && id2 == 2 && ft2 == T_STRUCT) {
                int id3 = 0, ft3;
                while (tr_field(t, &id3, &ft3)) {
                    n->unit = id3;
                    tr_skip(t, ft3);
                }
            } else if (id == 10 && id2 == 1 && ft2 == T_BYTE) {
                if (t->p >= t->end) t->bad = 1; else n->bits = (int8_t)*t->p++;
            } else if (id == 10 && id2 == 2 && (ft2 == T_TRUE || ft2 == T_FALSE))
                n->is_signed = ft2 == T_TRUE;
            else tr_skip(t, ft2);
        }
    }
}

static int read_schema(Tr *t, PqFooter *f) {
    int et;
    int64_t n = tr_list(t, &et);
    if (t->bad || et != T_STRUCT || n < 1 || n > 100000) { t->bad = 1; return -1; }
    f->nodes = calloc((size_t)n, sizeof *f->nodes);
    if (!f->nodes) return -1;
    for (int64_t i = 0; i < n && !t->bad; i++) {
        PqNode *x = &f->nodes[i];
        x->physical = -1;
        x->converted = -1;
        x->parent = -1;
        x->column_id = -1;
        f->n = (int)i + 1;
        int id = 0, ft;
        while (tr_field(t, &id, &ft)) {
            if (id == 1 && ft == T_I32) x->physical = (int)tr_int(t);
            else if (id == 2 && ft == T_I32) x->type_length = (int)tr_int(t);
            else if (id == 3 && ft == T_I32) x->repeated = tr_int(t) == 2;
            else if (id == 4 && ft == T_BINARY) {
                size_t len;
                const uint8_t *s = tr_binary(t, &len);
                free(x->name);
                x->name = copy_bytes(s, len);
                if (!x->name || strlen(x->name) != len) t->bad = 1;
            } else if (id == 5 && ft == T_I32) x->num_children = (int)tr_int(t);
            else if (id == 6 && ft == T_I32) x->converted = (int)tr_int(t);
            else if (id == 7 && ft == T_I32) x->scale = (int)tr_int(t);
            else if (id == 8 && ft == T_I32) x->precision = (int)tr_int(t);
            else if (id == 10 && ft == T_STRUCT) read_logical(t, x);
            else tr_skip(t, ft);
        }
        if (!x->name) t->bad = 1;
    }
    return t->bad ? -1 : 0;
}

/* Give each node its parent, and list the leaves in order. Returns the
 * index after the subtree at `at`, or -1 for a tree that does not fit. */
static int link_tree(PqFooter *f, int at, int parent, int depth) {
    if (at >= f->n || depth > TR_DEPTH) return -1;
    PqNode *x = &f->nodes[at];
    x->parent = parent;
    if (x->physical >= 0) {
        if (x->num_children != 0) return -1;
        f->leaves[f->n_leaves++] = at;
        return at + 1;
    }
    if (x->num_children < 0) return -1;
    int next = at + 1;
    for (int k = 0; k < x->num_children; k++) {
        next = link_tree(f, next, at, depth + 1);
        if (next < 0) return -1;
    }
    return next;
}

static int value_cmp(const PqNode *n, const uint8_t *a, size_t alen,
                     const uint8_t *b, size_t blen, int *ok);

static void keep_edge(PqNode *leaf, const uint8_t *v, size_t len, int is_max) {
    uint8_t **have = is_max ? &leaf->max : &leaf->min;
    size_t *have_len = is_max ? &leaf->max_len : &leaf->min_len;
    int *has = is_max ? &leaf->has_max : &leaf->has_min;
    if (*has) {
        int ok = 1;
        int c = value_cmp(leaf, v, len, *have, *have_len, &ok);
        if (!ok) { leaf->no_range = 1; return; }
        if (is_max ? c <= 0 : c >= 0) return;
    }
    uint8_t *mine = malloc(len ? len : 1);
    if (!mine) { leaf->no_range = 1; return; }
    if (len) memcpy(mine, v, len);
    free(*have);
    *have = mine;
    *have_len = len;
    *has = 1;
}

/* One chunk's statistics. *ranged: it has a minimum and a maximum.
 * *nulls: its null count, -1 when it has none. */
static void read_statistics(Tr *t, PqNode *leaf, int *ranged, int64_t *nulls) {
    int id = 0, ft;
    int had_min = 0, had_max = 0;
    const uint8_t *old_min = NULL, *old_max = NULL;
    size_t old_min_len = 0, old_max_len = 0;
    while (tr_field(t, &id, &ft)) {
        if (ft == T_BINARY && (id == 1 || id == 2 || id == 5 || id == 6)) {
            size_t len;
            const uint8_t *s = tr_binary(t, &len);
            if (t->bad) break;
            if (id == 5) { keep_edge(leaf, s, len, 1); had_max = 1; }
            else if (id == 6) { keep_edge(leaf, s, len, 0); had_min = 1; }
            else if (id == 1) { old_max = s; old_max_len = len; }
            else { old_min = s; old_min_len = len; }
        } else if (id == 3 && ft == T_I64) {
            *nulls = tr_int(t);
        } else {
            tr_skip(t, ft);
        }
    }
    /* the fields of writers before 2018 hold a signed order only: taken
     * for the kinds whose order that is */
    int signed_order = leaf->kind == K_INT || leaf->kind == K_BOOL
        || leaf->kind == K_FLOAT || leaf->kind == K_DOUBLE
        || leaf->kind == K_DATE || leaf->kind == K_TIME
        || leaf->kind == K_TIMESTAMP;
    if (!had_max && old_max && signed_order) { keep_edge(leaf, old_max, old_max_len, 1); had_max = 1; }
    if (!had_min && old_min && signed_order) { keep_edge(leaf, old_min, old_min_len, 0); had_min = 1; }
    *ranged = had_min && had_max;
}

static void read_column_meta(Tr *t, PqNode *leaf) {
    int id = 0, ft;
    int ranged = 0;
    int64_t values = 0, nulls = -1;
    while (tr_field(t, &id, &ft)) {
        if (id == 5 && ft == T_I64) values = tr_int(t);
        else if (id == 7 && ft == T_I64) leaf->size += tr_int(t);
        else if (id == 12 && ft == T_STRUCT) read_statistics(t, leaf, &ranged, &nulls);
        else tr_skip(t, ft);
    }
    /* counts no file has: a footer that is not one */
    const int64_t most = (int64_t)1 << 48;
    if (values < 0 || values > most || nulls < -1 || nulls > values
        || leaf->size < 0 || leaf->size > ((int64_t)1 << 56)) {
        t->bad = 1;
        return;
    }
    leaf->values += values;
    if (nulls < 0) leaf->has_nulls = 0;
    else leaf->nulls += nulls;
    /* a chunk of nulls alone has no range, and ends none */
    if (!ranged && !(nulls >= 0 && nulls == values)) leaf->no_range = 1;
}

static void read_row_groups(Tr *t, PqFooter *f) {
    int et;
    int64_t n = tr_list(t, &et);
    if (t->bad || et != T_STRUCT) { t->bad = 1; return; }
    f->row_groups = n;
    for (int64_t g = 0; g < n && !t->bad; g++) {
        int id = 0, ft;
        while (tr_field(t, &id, &ft)) {
            if (id != 1 || ft != T_LIST) { tr_skip(t, ft); continue; }
            int et2;
            int64_t cols = tr_list(t, &et2);
            if (t->bad || et2 != T_STRUCT || cols != f->n_leaves) { t->bad = 1; return; }
            for (int64_t c = 0; c < cols && !t->bad; c++) {
                PqNode *leaf = &f->nodes[f->leaves[c]];
                int id2 = 0, ft2, had_meta = 0;
                while (tr_field(t, &id2, &ft2)) {
                    if (id2 == 3 && ft2 == T_STRUCT) { read_column_meta(t, leaf); had_meta = 1; }
                    else tr_skip(t, ft2);
                }
                leaf->has_chunk = 1;
                if (!had_meta) { leaf->no_range = 1; leaf->has_nulls = 0; }
            }
        }
    }
}

static void read_key_values(Tr *t, PqFooter *f) {
    int et;
    int64_t n = tr_list(t, &et);
    if (t->bad || et != T_STRUCT) { t->bad = 1; return; }
    for (int64_t i = 0; i < n && !t->bad; i++) {
        int id = 0, ft;
        const uint8_t *k = NULL, *v = NULL;
        size_t klen = 0, vlen = 0;
        while (tr_field(t, &id, &ft)) {
            if (id == 1 && ft == T_BINARY) k = tr_binary(t, &klen);
            else if (id == 2 && ft == T_BINARY) v = tr_binary(t, &vlen);
            else tr_skip(t, ft);
        }
        if (t->bad || !k || !v) continue;
        if (klen == 6 && memcmp(k, "sp_run", 6) == 0 && vlen < 64) {
            free(f->run);
            f->run = copy_bytes(v, vlen);
        } else if (klen == 11 && memcmp(k, "sp_base_seq", 11) == 0
                   && vlen > 0 && vlen < 24) {
            char num[24], *end;
            memcpy(num, v, vlen);
            num[vlen] = 0;
            long long got = strtoll(num, &end, 10);
            if (*end == 0) { f->first_row = got; f->has_first_row = 1; }
        }
    }
}

static void name_types(PqFooter *f);

/* Read a footer (the thrift bytes alone). 0, or -1 with the reason. */
static int footer_read(const uint8_t *bytes, size_t len, PqFooter *f,
                       char *err, size_t errlen) {
    memset(f, 0, sizeof *f);
    Tr t = { bytes, bytes + len, 0, 0 };
    int id = 0, ft, owed = 0;
    while (tr_field(&t, &id, &ft)) {
        if (id == 2 && ft == T_LIST && !f->nodes) {
            if (read_schema(&t, f) != 0) { t.bad = 1; break; }
            f->leaves = calloc((size_t)f->n, sizeof *f->leaves);
            if (!f->leaves || link_tree(f, 0, -1, 0) != f->n) { t.bad = 1; break; }
            name_types(f);
            for (int i = 0; i < f->n_leaves; i++)
                f->nodes[f->leaves[i]].has_nulls = 1;
        } else if (id == 3 && ft == T_I64) {
            f->num_rows = tr_int(&t);
            if (f->num_rows < 0 || f->num_rows > ((int64_t)1 << 48)) t.bad = 1;
        } else if (id == 4 && ft == T_LIST && f->nodes) {
            read_row_groups(&t, f);
        } else if (id == 5 && ft == T_LIST) {
            read_key_values(&t, f);
        } else {
            if (id == 4) owed = 1;        /* row groups before the schema */
            tr_skip(&t, ft);
        }
    }
    if (!t.bad && f->nodes && owed) {
        t.p = bytes;
        id = 0;
        while (tr_field(&t, &id, &ft)) {
            if (id == 4 && ft == T_LIST) { read_row_groups(&t, f); break; }
            tr_skip(&t, ft);
        }
    }
    if (t.bad || !f->nodes || f->num_rows < 0) {
        footer_free(f);
        fail(err, errlen, "not a parquet footer");
        return -1;
    }
    return 0;
}

/* ---- the DuckLake type of each column ----------------------------------- */

enum { CV_UTF8 = 0, CV_MAP = 1, CV_MAP_KEY_VALUE = 2, CV_LIST = 3, CV_ENUM = 4,
       CV_DECIMAL = 5, CV_DATE = 6, CV_TIME_MILLIS = 7, CV_TIME_MICROS = 8,
       CV_TS_MILLIS = 9, CV_TS_MICROS = 10, CV_UINT_8 = 11, CV_UINT_16 = 12,
       CV_UINT_32 = 13, CV_UINT_64 = 14, CV_INT_8 = 15, CV_INT_16 = 16,
       CV_INT_32 = 17, CV_INT_64 = 18, CV_JSON = 19 };
enum { LT_STRING = 1, LT_MAP = 2, LT_LIST = 3, LT_ENUM = 4, LT_DECIMAL = 5,
       LT_DATE = 6, LT_TIME = 7, LT_TIMESTAMP = 8, LT_INTEGER = 10,
       LT_JSON = 12 };

static void set_type(PqNode *x, const char *type, int kind) {
    snprintf(x->type, sizeof x->type, "%s", type);
    x->kind = kind;
}

static void name_leaf(PqNode *x) {
    int lt = x->logical, cv = x->converted, ph = x->physical;
    if (lt == LT_DECIMAL || cv == CV_DECIMAL) {
        if (x->precision < 1 || x->precision > 38 || x->scale < 0
            || x->scale > x->precision) return;
        char type[40];
        snprintf(type, sizeof type, "decimal(%d,%d)", x->precision, x->scale);
        if (ph == PQ_INT32 || ph == PQ_INT64 || ph == PQ_FIXED
            || ph == PQ_BYTE_ARRAY) set_type(x, type, K_DECIMAL);
        return;
    }
    if (lt == LT_INTEGER || (cv >= CV_UINT_8 && cv <= CV_INT_64)) {
        int bits = x->bits, sign = x->is_signed;
        if (lt != LT_INTEGER) {
            static const int BITS[] = {8, 16, 32, 64, 8, 16, 32, 64};
            bits = BITS[cv - CV_UINT_8];
            sign = cv >= CV_INT_8;
        }
        if ((bits != 8 && bits != 16 && bits != 32 && bits != 64)
            || (ph != PQ_INT32 && ph != PQ_INT64)
            || (bits == 64) != (ph == PQ_INT64)) return;
        char type[16];
        snprintf(type, sizeof type, "%sint%d", sign ? "" : "u", bits);
        set_type(x, type, sign ? K_INT : K_UINT);
        return;
    }
    if (lt == LT_DATE || cv == CV_DATE) {
        if (ph == PQ_INT32) set_type(x, "date", K_DATE);
        return;
    }
    if (lt == LT_TIMESTAMP || cv == CV_TS_MILLIS || cv == CV_TS_MICROS) {
        if (ph != PQ_INT64) return;
        if (lt != LT_TIMESTAMP) {
            x->unit = cv == CV_TS_MILLIS ? UNIT_MILLIS : UNIT_MICROS;
            x->utc = 1;       /* the old annotation means UTC */
        }
        if (x->unit < UNIT_MILLIS || x->unit > UNIT_NANOS) return;
        if (x->utc && x->unit == UNIT_NANOS) set_type(x, "timestamptz_ns", K_TIMESTAMP);
        else if (x->utc) set_type(x, "timestamptz", K_TIMESTAMP);
        else if (x->unit == UNIT_NANOS) set_type(x, "timestamp_ns", K_TIMESTAMP);
        else set_type(x, "timestamp", K_TIMESTAMP);
        return;
    }
    if (lt == LT_TIME || cv == CV_TIME_MILLIS || cv == CV_TIME_MICROS) {
        if (lt != LT_TIME) x->unit = cv == CV_TIME_MILLIS ? UNIT_MILLIS : UNIT_MICROS;
        if ((x->unit == UNIT_MILLIS && ph == PQ_INT32)
            || (x->unit == UNIT_MICROS && ph == PQ_INT64))
            set_type(x, "time", K_TIME);
        return;
    }
    if (lt == LT_STRING || lt == LT_ENUM || cv == CV_UTF8 || cv == CV_ENUM) {
        if (ph == PQ_BYTE_ARRAY) set_type(x, "varchar", K_TEXT);
        return;
    }
    if (lt != 0 || cv != -1) return;      /* an annotation not taken */
    switch (ph) {
    case PQ_BOOLEAN: set_type(x, "boolean", K_BOOL); break;
    case PQ_INT32: set_type(x, "int32", K_INT); break;
    case PQ_INT64: set_type(x, "int64", K_INT); break;
    case PQ_FLOAT: set_type(x, "float32", K_FLOAT); break;
    case PQ_DOUBLE: set_type(x, "float64", K_DOUBLE); break;
    case PQ_BYTE_ARRAY: set_type(x, "blob", K_BLOB); break;
    default: break;
    }
}

/* The catalog's type of every node: a leaf's from its annotation, a
 * group's "struct" or "list". A list is the three-level form: a group
 * marked LIST with one repeated group that has one child. A node that
 * is not taken keeps "" and the file is refused with its name. */
static void name_types(PqFooter *f) {
    for (int i = 1; i < f->n; i++) {
        PqNode *x = &f->nodes[i];
        if (x->physical >= 0) {
            if (!x->repeated) name_leaf(x);
            continue;
        }
        if (x->logical == LT_LIST || x->converted == CV_LIST) {
            if (x->num_children == 1 && !x->repeated && i + 1 < f->n) {
                PqNode *w = &f->nodes[i + 1];
                if (w->physical < 0 && w->repeated && w->num_children == 1) {
                    w->list_wrapper = 1;
                    set_type(x, "list", K_NONE);
                }
            }
        } else if (x->logical == 0 && x->converted == -1 && !x->repeated
                   && x->num_children > 0) {
            set_type(x, "struct", K_NONE);
        }
    }
}

/* ---- a value: its order and its text ------------------------------------ */

typedef __int128 i128;

static int64_t le_int(const uint8_t *p, size_t n) {
    uint64_t v = 0;
    for (size_t i = 0; i < n; i++) v |= (uint64_t)p[i] << (8 * i);
    if (n == 4) return (int32_t)(uint32_t)v;
    return (int64_t)v;
}

/* The decimal's unscaled value. */
static int decimal_value(const PqNode *n, const uint8_t *p, size_t len, i128 *out) {
    if (n->physical == PQ_INT32 || n->physical == PQ_INT64) {
        if (len != (n->physical == PQ_INT32 ? 4u : 8u)) return 0;
        *out = le_int(p, len);
        return 1;
    }
    if (len < 1 || len > 16) return 0;
    i128 v = (p[0] & 0x80) ? -1 : 0;      /* big-endian two's complement */
    for (size_t i = 0; i < len; i++) v = (i128)(((unsigned __int128)v << 8) | p[i]);
    *out = v;
    return 1;
}

static size_t value_width(const PqNode *n) {
    switch (n->physical) {
    case PQ_BOOLEAN: return 1;
    case PQ_INT32: case PQ_FLOAT: return 4;
    case PQ_INT64: case PQ_DOUBLE: return 8;
    default: return 0;
    }
}

static double le_double(const PqNode *n, const uint8_t *p) {
    if (n->physical == PQ_FLOAT) { float f; memcpy(&f, p, 4); return f; }
    double d;
    memcpy(&d, p, 8);
    return d;
}

/* The order of two values of a column: <0, 0, >0. *ok = 0 for bytes
 * that are not a value of the column's kind (and for a NaN). */
static int value_cmp(const PqNode *n, const uint8_t *a, size_t alen,
                     const uint8_t *b, size_t blen, int *ok) {
    size_t w = value_width(n);
    if (w && (alen != w || blen != w)) { *ok = 0; return 0; }
    switch (n->kind) {
    case K_BOOL: return (a[0] != 0) - (b[0] != 0);
    case K_INT: case K_DATE: case K_TIME: case K_TIMESTAMP: {
        int64_t x = le_int(a, alen), y = le_int(b, blen);
        return (x > y) - (x < y);
    }
    case K_UINT: {
        uint64_t x = (uint64_t)le_int(a, alen), y = (uint64_t)le_int(b, blen);
        if (alen == 4) { x &= 0xffffffffu; y &= 0xffffffffu; }
        return (x > y) - (x < y);
    }
    case K_FLOAT: case K_DOUBLE: {
        double x = le_double(n, a), y = le_double(n, b);
        if (isnan(x) || isnan(y)) { *ok = 0; return 0; }
        return (x > y) - (x < y);
    }
    case K_TEXT: case K_BLOB: {
        size_t m = alen < blen ? alen : blen;
        int c = m ? memcmp(a, b, m) : 0;
        if (c) return c;
        return (alen > blen) - (alen < blen);
    }
    case K_DECIMAL: {
        i128 x, y;
        if (!decimal_value(n, a, alen, &x) || !decimal_value(n, b, blen, &y)) {
            *ok = 0;
            return 0;
        }
        return (x > y) - (x < y);
    }
    default: *ok = 0; return 0;
    }
}

static int utf8_ok(const uint8_t *s, size_t n) {
    size_t i = 0;
    while (i < n) {
        uint8_t c = s[i];
        size_t need;
        uint32_t cp;
        if (c == 0) return 0;
        if (c < 0x80) { i++; continue; }
        else if ((c & 0xe0) == 0xc0) { need = 1; cp = c & 0x1f; }
        else if ((c & 0xf0) == 0xe0) { need = 2; cp = c & 0x0f; }
        else if ((c & 0xf8) == 0xf0) { need = 3; cp = c & 0x07; }
        else return 0;
        if (need > n - 1 - i) return 0;
        for (size_t k = 1; k <= need; k++) {
            if ((s[i + k] & 0xc0) != 0x80) return 0;
            cp = (cp << 6) | (s[i + k] & 0x3f);
        }
        if ((need == 1 && cp < 0x80) || (need == 2 && cp < 0x800)
            || (need == 3 && (cp < 0x10000 || cp > 0x10ffff))
            || (cp >= 0xd800 && cp <= 0xdfff)) return 0;
        i += need + 1;
    }
    return 1;
}

/* A double (or a float) as the shortest text that reads back as the
 * same value: "1.5", "32500000000.0", "1e-300", "1.7976931348623157e+308". */
static void number_text(double v, int is_float, char *out, size_t outlen) {
    if (isinf(v)) { snprintf(out, outlen, v < 0 ? "-inf" : "inf"); return; }
    char e[40];
    int prec;
    for (prec = 1; prec <= 17; prec++) {
        snprintf(e, sizeof e, "%.*e", prec - 1, v);
        if (is_float ? (strtof(e, NULL) == (float)v) : (strtod(e, NULL) == v)) break;
    }
    /* e is [-]d[.ddd]e[+-]XX */
    char digits[24];
    int nd = 0, neg = e[0] == '-';
    const char *p = e + neg;
    for (; *p && *p != 'e'; p++)
        if (*p != '.') digits[nd++] = *p;
    while (nd > 1 && digits[nd - 1] == '0') nd--;
    digits[nd] = 0;
    int exp10 = atoi(p + 1);
    char body[64];
    int at = 0;
    if (exp10 >= -4 && exp10 < 16) {
        if (exp10 < 0) {
            at += snprintf(body + at, sizeof body - (size_t)at, "0.");
            for (int k = -1; k > exp10; k--) body[at++] = '0';
            memcpy(body + at, digits, (size_t)nd);
            at += nd;
        } else {
            for (int k = 0; k <= exp10; k++) body[at++] = k < nd ? digits[k] : '0';
            body[at++] = '.';
            if (nd > exp10 + 1) {
                memcpy(body + at, digits + exp10 + 1, (size_t)(nd - exp10 - 1));
                at += nd - exp10 - 1;
            } else {
                body[at++] = '0';
            }
        }
        body[at] = 0;
    } else {
        body[at++] = digits[0];
        if (nd > 1) {
            body[at++] = '.';
            memcpy(body + at, digits + 1, (size_t)(nd - 1));
            at += nd - 1;
        }
        snprintf(body + at, sizeof body - (size_t)at, "e%c%02d",
                 exp10 < 0 ? '-' : '+', exp10 < 0 ? -exp10 : exp10);
    }
    snprintf(out, outlen, "%s%s", neg ? "-" : "", body);
}

/* Days since 1970-01-01 as a date of the proleptic Gregorian calendar. */
static void civil(int64_t days, int64_t *y, int *m, int *d) {
    days += 719468;
    int64_t era = (days >= 0 ? days : days - 146096) / 146097;
    int64_t doe = days - era * 146097;
    int64_t yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    int64_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    int64_t mp = (5 * doy + 2) / 153;
    *d = (int)(doy - (153 * mp + 2) / 5 + 1);
    *m = (int)(mp < 10 ? mp + 3 : mp - 9);
    *y = yoe + era * 400 + (*m <= 2);
}

/* HH:MM:SS and the fraction with its final zeros dropped. `sub`: the
 * part of a second, in units of 10^-digits. */
static int clock_text(char *out, size_t outlen, int64_t secs, int64_t sub,
                      int digits) {
    int at = snprintf(out, outlen, "%02d:%02d:%02d", (int)(secs / 3600),
                      (int)(secs / 60 % 60), (int)(secs % 60));
    if (sub) {
        char frac[16];
        snprintf(frac, sizeof frac, "%0*lld", digits, (long long)sub);
        int n = digits;
        while (n > 0 && frac[n - 1] == '0') n--;
        frac[n] = 0;
        at += snprintf(out + at, outlen - (size_t)at, ".%s", frac);
    }
    return at;
}

/* The text the catalog keeps for a minimum or a maximum: what DuckDB
 * casts back to the column's type. Returns sqlite3_malloc'd text, or
 * NULL for a value the catalog does not keep. */
static char *value_text(const PqNode *n, const uint8_t *p, size_t len) {
    size_t w = value_width(n);
    if (w && len != w) return NULL;
    char buf[96];
    switch (n->kind) {
    case K_BOOL: return sqlite3_mprintf("%s", p[0] ? "true" : "false");
    case K_INT: return sqlite3_mprintf("%lld", (long long)le_int(p, len));
    case K_UINT: {
        uint64_t v = (uint64_t)le_int(p, len);
        if (len == 4) v &= 0xffffffffu;
        return sqlite3_mprintf("%llu", (unsigned long long)v);
    }
    case K_FLOAT: case K_DOUBLE: {
        double v = le_double(n, p);
        if (isnan(v)) return NULL;
        number_text(v, n->kind == K_FLOAT, buf, sizeof buf);
        return sqlite3_mprintf("%s", buf);
    }
    case K_TEXT:
        if (!utf8_ok(p, len)) return NULL;
        return sqlite3_mprintf("%.*s", (int)len, (const char *)p);
    case K_BLOB: {
        /* DuckDB's text of a blob: printable ASCII as it is, every
         * other byte as \xHH */
        sqlite3_str *s = sqlite3_str_new(NULL);
        for (size_t i = 0; i < len; i++) {
            uint8_t c = p[i];
            if (c > 31 && c < 127 && c != '"' && c != '\'' && c != '\\')
                sqlite3_str_appendchar(s, 1, (char)c);
            else
                sqlite3_str_appendf(s, "\\x%02X", c);
        }
        return sqlite3_str_finish(s);
    }
    case K_DATE: case K_TIMESTAMP: {
        int64_t v = le_int(p, len), days, sub = 0, secs = 0;
        int digits = 0;
        if (n->kind == K_DATE) {
            days = v;
        } else {
            int64_t per = n->unit == UNIT_MILLIS ? 1000
                : n->unit == UNIT_MICROS ? 1000000 : 1000000000;
            digits = n->unit == UNIT_MILLIS ? 3 : n->unit == UNIT_MICROS ? 6 : 9;
            int64_t whole = v / per;
            sub = v % per;
            if (sub < 0) { sub += per; whole--; }
            days = whole / 86400;
            secs = whole % 86400;
            if (secs < 0) { secs += 86400; days--; }
        }
        int64_t y;
        int m, d;
        civil(days, &y, &m, &d);
        if (y < 1 || y > 9999) return NULL;
        int at = snprintf(buf, sizeof buf, "%04lld-%02d-%02d", (long long)y, m, d);
        if (n->kind == K_TIMESTAMP) {
            buf[at++] = ' ';
            at += clock_text(buf + at, sizeof buf - (size_t)at, secs, sub, digits);
            if (n->utc) snprintf(buf + at, sizeof buf - (size_t)at, "+00");
        }
        return sqlite3_mprintf("%s", buf);
    }
    case K_TIME: {
        int64_t v = le_int(p, len);
        int64_t per = n->unit == UNIT_MILLIS ? 1000 : 1000000;
        if (v < 0 || v / per >= 86400) return NULL;
        clock_text(buf, sizeof buf, v / per, v % per,
                   n->unit == UNIT_MILLIS ? 3 : 6);
        return sqlite3_mprintf("%s", buf);
    }
    case K_DECIMAL: {
        i128 v;
        if (!decimal_value(n, p, len, &v)) return NULL;
        int neg = v < 0;
        unsigned __int128 u = neg ? -(unsigned __int128)v : (unsigned __int128)v;
        char digits[48];
        int nd = 0;
        do { digits[nd++] = (char)('0' + (int)(u % 10)); u /= 10; } while (u);
        while (nd <= n->scale) digits[nd++] = '0';
        int at = 0;
        if (neg) buf[at++] = '-';
        for (int k = nd - 1; k >= 0; k--) {
            buf[at++] = digits[k];
            if (k == n->scale && k != 0) buf[at++] = '.';
        }
        buf[at] = 0;
        return sqlite3_mprintf("%s", buf);
    }
    default: return NULL;
    }
}

/* ---- the footer's place in a file --------------------------------------- */

int64_t sp_parquet_footer_length(const uint8_t *tail, size_t len,
                                 char *err, size_t errlen) {
    if (!tail || len < 8 || memcmp(tail + len - 4, "PAR1", 4) != 0) {
        fail(err, errlen, "not a parquet file: it does not end in PAR1");
        return -1;
    }
    uint64_t n = 0;
    for (int i = 0; i < 4; i++) n |= (uint64_t)tail[len - 8 + (size_t)i] << (8 * i);
    if (n == 0) {
        fail(err, errlen, "not a parquet file: its footer is empty");
        return -1;
    }
    return (int64_t)n;
}

/* ---- the catalog --------------------------------------------------------- */

/* The tables of a DuckLake 1.0 catalog, as DuckDB 2.0.0 makes them in
 * SQLite (2026-11-21). Generated from that catalog; not edited by hand. */
static const char *const DUCKLAKE_1_0[] = {
    "CREATE TABLE ducklake_column(column_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, table_id BIGINT, column_order BIGINT, column_name VARCHAR, column_type VARCHAR, initial_default VARCHAR, default_value VARCHAR, nulls_allowed BIGINT, parent_column BIGINT, default_value_type VARCHAR, default_value_dialect VARCHAR)",
    "CREATE TABLE ducklake_column_mapping(mapping_id BIGINT, table_id BIGINT, \"type\" VARCHAR)",
    "CREATE TABLE ducklake_column_tag(table_id BIGINT, column_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, \"key\" VARCHAR, \"value\" VARCHAR)",
    "CREATE TABLE ducklake_data_file(data_file_id BIGINT PRIMARY KEY, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, file_order BIGINT, path VARCHAR, path_is_relative BIGINT, file_format VARCHAR, record_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, row_id_start BIGINT, partition_id BIGINT, encryption_key VARCHAR, mapping_id BIGINT, partial_max BIGINT)",
    "CREATE TABLE ducklake_delete_file(delete_file_id BIGINT PRIMARY KEY, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, data_file_id BIGINT, path VARCHAR, path_is_relative BIGINT, format VARCHAR, delete_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, encryption_key VARCHAR, partial_max BIGINT)",
    "CREATE TABLE ducklake_file_column_stats(data_file_id BIGINT, table_id BIGINT, column_id BIGINT, column_size_bytes BIGINT, value_count BIGINT, null_count BIGINT, min_value VARCHAR, max_value VARCHAR, contains_nan BIGINT, extra_stats VARCHAR)",
    "CREATE TABLE ducklake_file_partition_value(data_file_id BIGINT, table_id BIGINT, partition_key_index BIGINT, partition_value VARCHAR)",
    "CREATE TABLE ducklake_file_variant_stats(data_file_id BIGINT, table_id BIGINT, column_id BIGINT, variant_path VARCHAR, shredded_type VARCHAR, column_size_bytes BIGINT, value_count BIGINT, null_count BIGINT, min_value VARCHAR, max_value VARCHAR, contains_nan BIGINT, extra_stats VARCHAR)",
    "CREATE TABLE ducklake_files_scheduled_for_deletion(data_file_id BIGINT, path VARCHAR, path_is_relative BIGINT, schedule_start VARCHAR)",
    "CREATE TABLE ducklake_inlined_data_tables(table_id BIGINT, table_name VARCHAR, schema_version BIGINT)",
    "CREATE TABLE ducklake_macro(schema_id BIGINT, macro_id BIGINT, macro_name VARCHAR, begin_snapshot BIGINT, end_snapshot BIGINT)",
    "CREATE TABLE ducklake_macro_impl(macro_id BIGINT, impl_id BIGINT, dialect VARCHAR, \"sql\" VARCHAR, \"type\" VARCHAR)",
    "CREATE TABLE ducklake_macro_parameters(macro_id BIGINT, impl_id BIGINT, column_id BIGINT, parameter_name VARCHAR, parameter_type VARCHAR, default_value VARCHAR, default_value_type VARCHAR)",
    "CREATE TABLE ducklake_metadata(\"key\" VARCHAR NOT NULL, \"value\" VARCHAR NOT NULL, \"scope\" VARCHAR, scope_id BIGINT)",
    "CREATE TABLE ducklake_name_mapping(mapping_id BIGINT, column_id BIGINT, source_name VARCHAR, target_field_id BIGINT, parent_column BIGINT, is_partition BIGINT)",
    "CREATE TABLE ducklake_partition_column(partition_id BIGINT, table_id BIGINT, partition_key_index BIGINT, column_id BIGINT, \"transform\" VARCHAR)",
    "CREATE TABLE ducklake_partition_info(partition_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT)",
    "CREATE TABLE ducklake_schema(schema_id BIGINT PRIMARY KEY, schema_uuid VARCHAR, begin_snapshot BIGINT, end_snapshot BIGINT, schema_name VARCHAR, path VARCHAR, path_is_relative BIGINT)",
    "CREATE TABLE ducklake_schema_versions(begin_snapshot BIGINT, schema_version BIGINT, table_id BIGINT)",
    "CREATE TABLE ducklake_snapshot(snapshot_id BIGINT PRIMARY KEY, snapshot_time VARCHAR, schema_version BIGINT, next_catalog_id BIGINT, next_file_id BIGINT)",
    "CREATE TABLE ducklake_snapshot_changes(snapshot_id BIGINT PRIMARY KEY, changes_made VARCHAR, author VARCHAR, commit_message VARCHAR, commit_extra_info VARCHAR)",
    "CREATE TABLE ducklake_sort_expression(sort_id BIGINT, table_id BIGINT, sort_key_index BIGINT, expression VARCHAR, dialect VARCHAR, sort_direction VARCHAR, null_order VARCHAR)",
    "CREATE TABLE ducklake_sort_info(sort_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT)",
    "CREATE TABLE ducklake_table(table_id BIGINT, table_uuid VARCHAR, begin_snapshot BIGINT, end_snapshot BIGINT, schema_id BIGINT, table_name VARCHAR, path VARCHAR, path_is_relative BIGINT)",
    "CREATE TABLE ducklake_table_column_stats(table_id BIGINT, column_id BIGINT, contains_null BIGINT, contains_nan BIGINT, min_value VARCHAR, max_value VARCHAR, extra_stats VARCHAR)",
    "CREATE TABLE ducklake_table_stats(table_id BIGINT, record_count BIGINT, next_row_id BIGINT, file_size_bytes BIGINT)",
    "CREATE TABLE ducklake_tag(object_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, \"key\" VARCHAR, \"value\" VARCHAR)",
    "CREATE TABLE ducklake_view(view_id BIGINT, view_uuid VARCHAR, begin_snapshot BIGINT, end_snapshot BIGINT, schema_id BIGINT, view_name VARCHAR, dialect VARCHAR, \"sql\" VARCHAR, column_aliases VARCHAR)",
    NULL
};
/* Ours, in the same database; DuckLake does not look at them.
 *   sp_file      each finished file's run and the stream row number of
 *                its first row (the footer's sp_run and sp_base_seq)
 *   sp_range     each column's minimum and maximum over all files, as
 *                the files hold them: what the next file's are compared
 *                with
 *   sp_mapping   the name mapping of each shape of file              */
static const char *const SP_TABLES[] = {
    "CREATE TABLE sp_file(data_file_id BIGINT PRIMARY KEY, run VARCHAR, first_row BIGINT)",
    "CREATE TABLE sp_range(column_id BIGINT PRIMARY KEY, min_value BLOB, max_value BLOB)",
    "CREATE TABLE sp_mapping(mapping_id BIGINT PRIMARY KEY, shape VARCHAR)",
    NULL
};

#define TABLE_ID 1
#define TABLE_NAME "data"

struct SpCatalog {
    sqlite3 *db;
    char *data_path;
    int64_t snapshot;         /* the snapshot of the staged changes; 0: none */
    int64_t schema_version, next_catalog_id, next_file_id;
    int created, altered, inserted;
    int has_table;
    int64_t staged;           /* files added and not yet in the bytes */
};

static int cat_exec(SpCatalog *c, char *err, size_t errlen, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    char *sql = sqlite3_vmprintf(fmt, ap);
    va_end(ap);
    if (!sql) { fail(err, errlen, "catalog: memory is out"); return -1; }
    char *msg = NULL;
    int rc = sqlite3_exec(c->db, sql, NULL, NULL, &msg);
    if (rc != SQLITE_OK)
        fail(err, errlen, "catalog: %s", msg ? msg : sqlite3_errmsg(c->db));
    sqlite3_free(msg);
    sqlite3_free(sql);
    return rc == SQLITE_OK ? 0 : -1;
}

/* One integer from a query; *found = 0 when it has no row or a NULL. */
static int64_t cat_int(SpCatalog *c, int *found, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    char *sql = sqlite3_vmprintf(fmt, ap);
    va_end(ap);
    int64_t out = 0;
    if (found) *found = 0;
    sqlite3_stmt *st = NULL;
    if (sql && sqlite3_prepare_v2(c->db, sql, -1, &st, NULL) == SQLITE_OK
        && sqlite3_step(st) == SQLITE_ROW
        && sqlite3_column_type(st, 0) != SQLITE_NULL) {
        out = sqlite3_column_int64(st, 0);
        if (found) *found = 1;
    }
    sqlite3_finalize(st);
    sqlite3_free(sql);
    return out;
}

void sp_catalog_free(SpCatalog *c) {
    if (!c) return;
    if (c->db) sqlite3_close(c->db);
    free(c->data_path);
    free(c);
}

/* A UUID-shaped text from a name: the same name gives the same text. */
static void name_uuid(const char *a, const char *b, char out[37]) {
    uint64_t h[2] = { 0xcbf29ce484222325ULL, 0x84222325cbf29ce4ULL };
    for (int k = 0; k < 2; k++)
        for (const char *s = k ? b : a; s && *s; s++) {
            h[0] = (h[0] ^ (uint8_t)*s) * 0x100000001b3ULL;
            h[1] = (h[1] ^ (uint8_t)*s ^ (uint64_t)k) * 0x100000001b3ULL + (h[0] >> 29);
        }
    snprintf(out, 37, "%08x-%04x-8%03x-a%03x-%012llx",
             (unsigned)(h[0] >> 32), (unsigned)(h[0] >> 16 & 0xffff),
             (unsigned)(h[0] & 0xfff), (unsigned)(h[1] >> 48 & 0xfff),
             (unsigned long long)(h[1] & 0xffffffffffffULL));
}

/* The catalog's text of a time: UTC, to the microsecond. */
static void time_text(double wall, char *out, size_t outlen) {
    if (!(wall >= 0) || wall > 253402300799.0) wall = 0;
    int64_t whole = (int64_t)floor(wall);
    int64_t micros = (int64_t)((wall - (double)whole) * 1e6);
    if (micros > 999999) micros = 999999;
    int64_t y;
    int m, d;
    civil(whole / 86400, &y, &m, &d);
    int64_t secs = whole % 86400;
    snprintf(out, outlen, "%04lld-%02d-%02d %02d:%02d:%02d.%06lld+00",
             (long long)y, m, d, (int)(secs / 3600), (int)(secs / 60 % 60),
             (int)(secs % 60), (long long)micros);
}

static int cat_begin(SpCatalog *c, char *err, size_t errlen) {
    int found;
    c->snapshot = 0;
    c->created = c->altered = c->inserted = 0;
    c->staged = 0;
    int64_t last = cat_int(c, &found, "SELECT max(snapshot_id) FROM ducklake_snapshot");
    if (!found) { fail(err, errlen, "catalog: it has no snapshot"); return -1; }
    c->schema_version = cat_int(c, NULL,
        "SELECT schema_version FROM ducklake_snapshot WHERE snapshot_id = %lld", (long long)last);
    c->next_catalog_id = cat_int(c, NULL,
        "SELECT next_catalog_id FROM ducklake_snapshot WHERE snapshot_id = %lld", (long long)last);
    c->next_file_id = cat_int(c, NULL,
        "SELECT next_file_id FROM ducklake_snapshot WHERE snapshot_id = %lld", (long long)last);
    c->snapshot = last + 1;
    cat_int(c, &c->has_table,
            "SELECT table_id FROM ducklake_table WHERE table_id = %d "
            "AND end_snapshot IS NULL", TABLE_ID);
    return cat_exec(c, err, errlen, "BEGIN");
}

SpCatalog *sp_catalog_open(const uint8_t *bytes, size_t len,
                           const char *data_path, double now,
                           char *err, size_t errlen) {
    if (!data_path) { fail(err, errlen, "catalog: no data path"); return NULL; }
    SpCatalog *c = calloc(1, sizeof *c);
    if (!c) { fail(err, errlen, "catalog: memory is out"); return NULL; }
    c->data_path = copy_bytes((const uint8_t *)data_path, strlen(data_path));
    if (!c->data_path || sqlite3_open(":memory:", &c->db) != SQLITE_OK) {
        fail(err, errlen, "catalog: memory is out");
        sp_catalog_free(c);
        return NULL;
    }
    if (bytes && len) {
        unsigned char *mine = sqlite3_malloc64(len);
        if (!mine) {
            fail(err, errlen, "catalog: memory is out");
            sp_catalog_free(c);
            return NULL;
        }
        memcpy(mine, bytes, len);
        int rc = sqlite3_deserialize(c->db, "main", mine, (sqlite3_int64)len,
            (sqlite3_int64)len,
            SQLITE_DESERIALIZE_FREEONCLOSE | SQLITE_DESERIALIZE_RESIZEABLE);
        int found = 0;
        if (rc == SQLITE_OK)
            cat_int(c, &found, "SELECT count(*) FROM ducklake_metadata "
                               "WHERE \"key\" = 'version' AND \"value\" = '1.0'");
        int ours = 0;
        if (found) cat_int(c, &ours, "SELECT count(*) FROM sp_file");
        if (rc != SQLITE_OK || !found || !ours
            || cat_int(c, NULL, "SELECT count(*) FROM ducklake_metadata "
                                "WHERE \"key\" = 'version' AND \"value\" = '1.0'") != 1) {
            fail(err, errlen, "catalog: these %zu bytes are not a stream's "
                 "catalog (a SQLite database with a DuckLake 1.0 catalog and "
                 "the stream core's tables)", len);
            sp_catalog_free(c);
            return NULL;
        }
    } else {
        char schema[37], when[40];
        name_uuid(data_path, "schema", schema);
        time_text(now, when, sizeof when);
        int rc = cat_exec(c, err, errlen, "PRAGMA page_size = 4096; BEGIN");
        for (int i = 0; rc == 0 && DUCKLAKE_1_0[i]; i++)
            rc = cat_exec(c, err, errlen, "%s", DUCKLAKE_1_0[i]);
        for (int i = 0; rc == 0 && SP_TABLES[i]; i++)
            rc = cat_exec(c, err, errlen, "%s", SP_TABLES[i]);
        if (rc == 0) rc = cat_exec(c, err, errlen,
            "INSERT INTO ducklake_metadata VALUES ('version', '1.0', NULL, NULL),"
            " ('created_by', 'streaming-parquet', NULL, NULL),"
            " ('data_path', %Q, NULL, NULL), ('encrypted', 'false', NULL, NULL);"
            "INSERT INTO ducklake_schema VALUES (0, %Q, 0, NULL, 'main', 'main/', 1);"
            "INSERT INTO ducklake_snapshot VALUES (0, %Q, 0, 1, 0);"
            "INSERT INTO ducklake_snapshot_changes VALUES "
            "(0, 'created_schema:\"main\"', NULL, NULL, NULL);"
            "COMMIT", data_path, schema, when);
        if (rc != 0) { sp_catalog_free(c); return NULL; }
    }
    if (cat_begin(c, err, errlen) != 0) { sp_catalog_free(c); return NULL; }
    return c;
}

int sp_catalog_lists(const SpCatalog *c, const char *path) {
    if (!c || !path) return 0;
    return cat_int((SpCatalog *)c, NULL,
        "SELECT count(*) FROM ducklake_data_file WHERE path = %Q "
        "AND end_snapshot IS NULL", path) > 0;
}

int64_t sp_catalog_files(const SpCatalog *c) {
    if (!c) return 0;
    return cat_int((SpCatalog *)c, NULL,
        "SELECT count(*) FROM ducklake_data_file WHERE end_snapshot IS NULL");
}

char *sp_catalog_file(const SpCatalog *c, int64_t index) {
    if (!c || index < 0) return NULL;
    sqlite3_stmt *st = NULL;
    char *out = NULL;
    if (sqlite3_prepare_v2(c->db,
            "SELECT path FROM ducklake_data_file WHERE end_snapshot IS NULL "
            "ORDER BY data_file_id LIMIT 1 OFFSET ?", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_int64(st, 1, index);
        if (sqlite3_step(st) == SQLITE_ROW) {
            const unsigned char *p = sqlite3_column_text(st, 0);
            if (p) out = copy_bytes(p, strlen((const char *)p));
        }
    }
    sqlite3_finalize(st);
    return out;
}

/* The catalog's column for a name under a parent (0: the top); -1 for
 * none. *type gets its type (sqlite3_free it). */
static int64_t cat_column(SpCatalog *c, const char *name, int64_t parent,
                          char **type) {
    sqlite3_stmt *st = NULL;
    int64_t id = -1;
    *type = NULL;
    if (sqlite3_prepare_v2(c->db,
            "SELECT column_id, column_type FROM ducklake_column WHERE table_id = ?1 "
            "AND end_snapshot IS NULL AND column_name = ?2 AND "
            "coalesce(parent_column, 0) = ?3", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_int(st, 1, TABLE_ID);
        sqlite3_bind_text(st, 2, name, -1, SQLITE_STATIC);
        sqlite3_bind_int64(st, 3, parent);
        if (sqlite3_step(st) == SQLITE_ROW) {
            id = sqlite3_column_int64(st, 0);
            *type = sqlite3_mprintf("%s", sqlite3_column_text(st, 1));
        }
    }
    sqlite3_finalize(st);
    return id;
}

/* The node the catalog has a column for, at or under `at`: a list's
 * repeated group is passed over (its one child is the column, named
 * "element"). */
static int column_node(const PqFooter *f, int at) {
    return f->nodes[at].list_wrapper ? at + 1 : at;
}

static const char *column_name(const PqFooter *f, int at) {
    if (at > 0 && f->nodes[f->nodes[at].parent].list_wrapper) return "element";
    return f->nodes[at].name;
}

static int next_sibling(const PqFooter *f, int at) {
    if (f->nodes[at].physical >= 0) return at + 1;
    int next = at + 1;
    for (int k = 0; k < f->nodes[at].num_children; k++) next = next_sibling(f, next);
    return next;
}

static void path_text(const PqFooter *f, int at, char *out, size_t outlen) {
    if (f->nodes[at].parent > 0) {
        path_text(f, f->nodes[at].parent, out, outlen);
        size_t n = strlen(out);
        snprintf(out + n, outlen - n, ".%s", f->nodes[at].name);
    } else {
        snprintf(out, outlen, "%s", f->nodes[at].name);
    }
}

/* Match the subtree at `at` to the catalog's columns under `parent`,
 * adding what the catalog lacks when `add`. Sets each node's column_id.
 * -1 with the reason for a type the catalog does not take or a column
 * whose type changed. */
static int match_column(SpCatalog *c, PqFooter *f, int at, int64_t parent,
                        int add, const char *path, char *err, size_t errlen) {
    at = column_node(f, at);
    PqNode *x = &f->nodes[at];
    char where[256];
    path_text(f, at, where, sizeof where);
    if (!x->type[0]) {
        fail(err, errlen, "catalog: %s: the column %s has a parquet type the "
             "catalog does not take (physical type %d, converted type %d, "
             "logical type %d%s)", path, where, x->physical, x->converted,
             x->logical, x->repeated ? ", repeated" : "");
        return -1;
    }
    char *have = NULL;
    const char *name = column_name(f, at);
    int64_t id = cat_column(c, name, parent, &have);
    if (id >= 0 && strcmp(have, x->type) != 0) {
        fail(err, errlen, "catalog: %s: the column %s is %s in this file and "
             "%s in the catalog; a column's type does not change. The file is "
             "not listed.", path, where, x->type, have);
        sqlite3_free(have);
        return -1;
    }
    sqlite3_free(have);
    if (id < 0) {
        if (!add) {
            fail(err, errlen, "catalog: %s: the column %s is new inside a "
                 "column the catalog has; a list or a struct keeps its "
                 "members. The file is not listed.", path, where);
            return -1;
        }
        id = cat_int(c, NULL, "SELECT coalesce(max(column_id), 0) + 1 FROM "
                              "ducklake_column WHERE table_id = %d", TABLE_ID);
        int nested = x->physical < 0;
        char above[24] = "NULL";
        if (parent) snprintf(above, sizeof above, "%lld", (long long)parent);
        if (cat_exec(c, err, errlen,
                "INSERT INTO ducklake_column VALUES (%lld, %lld, NULL, %d, %lld, "
                "%Q, %Q, NULL, 'NULL', 1, %s, %Q, 'duckdb')",
                (long long)id, (long long)c->snapshot, TABLE_ID, (long long)id,
                name, x->type, above, nested ? "" : "literal") != 0)
            return -1;
        if (parent == 0 && c->has_table) c->altered = 1;
    } else if (x->physical < 0) {
        add = 0;                          /* an old list or struct keeps its members */
    }
    x->column_id = id;
    if (x->physical < 0) {
        int child = at + 1;
        for (int k = 0; k < x->num_children; k++) {
            if (match_column(c, f, child, id, add, path, err, errlen) != 0) return -1;
            child = next_sibling(f, child);
        }
    }
    return 0;
}

/* The shape of a file: every node's name, in order, with its depth. */
static char *file_shape(const PqFooter *f) {
    sqlite3_str *s = sqlite3_str_new(NULL);
    for (int i = 1; i < f->n; i++) {
        int depth = 0;
        for (int p = f->nodes[i].parent; p > 0; p = f->nodes[p].parent) depth++;
        sqlite3_str_appendf(s, "%d:%d:%s\n", depth, (int)strlen(f->nodes[i].name),
                            f->nodes[i].name);
    }
    return sqlite3_str_finish(s);
}

/* The name mapping for this shape of file: the one the catalog has, or
 * a new one. -1 on failure. */
static int64_t file_mapping(SpCatalog *c, const PqFooter *f, char *err,
                            size_t errlen) {
    char *shape = file_shape(f);
    if (!shape) { fail(err, errlen, "catalog: memory is out"); return -1; }
    int found;
    int64_t id = cat_int(c, &found, "SELECT mapping_id FROM sp_mapping "
                                    "WHERE shape = %Q", shape);
    if (found) { sqlite3_free(shape); return id; }
    id = c->next_file_id++;
    int rc = cat_exec(c, err, errlen,
        "INSERT INTO ducklake_column_mapping VALUES (%lld, %d, 'map_by_name');"
        "INSERT INTO sp_mapping VALUES (%lld, %Q)",
        (long long)id, TABLE_ID, (long long)id, shape);
    sqlite3_free(shape);
    /* one row for each node the reader meets: a list's repeated group
     * stands for its element */
    int64_t *local = calloc((size_t)f->n, sizeof *local);
    if (!local) { fail(err, errlen, "catalog: memory is out"); return -1; }
    int64_t row = 0;
    for (int i = 1; rc == 0 && i < f->n; i++) {
        const PqNode *x = &f->nodes[i];
        if (x->parent > 0 && f->nodes[x->parent].list_wrapper) {
            local[i] = local[x->parent];
            continue;                     /* the element: its group's row */
        }
        int target = x->list_wrapper ? i + 1 : i;
        int up = x->parent;
        local[i] = row;
        if (up > 0)
            rc = cat_exec(c, err, errlen,
                "INSERT INTO ducklake_name_mapping VALUES (%lld, %lld, %Q, %lld, %lld, 0)",
                (long long)id, (long long)row, x->name,
                (long long)f->nodes[target].column_id, (long long)local[up]);
        else
            rc = cat_exec(c, err, errlen,
                "INSERT INTO ducklake_name_mapping VALUES (%lld, %lld, %Q, %lld, NULL, 0)",
                (long long)id, (long long)row, x->name,
                (long long)f->nodes[target].column_id);
        row++;
    }
    free(local);
    return rc == 0 ? id : -1;
}

static int bind_text_or_null(sqlite3_stmt *st, int at, char *text) {
    if (text) return sqlite3_bind_text(st, at, text, -1, sqlite3_free);
    return sqlite3_bind_null(st, at);
}

/* One leaf's rows: the file's numbers, and the table's with them. */
static int leaf_stats(SpCatalog *c, const PqNode *x, int64_t file_id,
                      int64_t rows, int64_t rows_before, char *err,
                      size_t errlen) {
    int ranged = x->kind != K_NONE && !x->no_range && x->has_min && x->has_max;
    sqlite3_stmt *st = NULL;
    int rc = sqlite3_prepare_v2(c->db,
        "INSERT INTO ducklake_file_column_stats VALUES (?1, ?2, ?3, ?4, ?5, ?6, "
        "?7, ?8, NULL, NULL)", -1, &st, NULL);
    if (rc == SQLITE_OK) {
        char *lo = ranged ? value_text(x, x->min, x->min_len) : NULL;
        char *hi = ranged ? value_text(x, x->max, x->max_len) : NULL;
        if (!lo || !hi) {                 /* both or neither */
            sqlite3_free(lo);
            sqlite3_free(hi);
            lo = hi = NULL;
            ranged = 0;
        }
        sqlite3_bind_int64(st, 1, file_id);
        sqlite3_bind_int(st, 2, TABLE_ID);
        sqlite3_bind_int64(st, 3, x->column_id);
        sqlite3_bind_int64(st, 4, x->size);
        if (x->has_nulls) {
            sqlite3_bind_int64(st, 5, x->values - x->nulls);
            sqlite3_bind_int64(st, 6, x->nulls);
        } else {
            sqlite3_bind_int64(st, 5, x->values);
            sqlite3_bind_null(st, 6);
        }
        bind_text_or_null(st, 7, lo);
        bind_text_or_null(st, 8, hi);
        rc = sqlite3_step(st) == SQLITE_DONE ? SQLITE_OK : SQLITE_ERROR;
    }
    sqlite3_finalize(st);
    st = NULL;
    /* the table's: null seen, and the range over every file that has
     * values of the column. sp_range holds that range as the files hold
     * it; a file with values and no range ends the table's range for
     * good (a row with NULLs). */
    int known;
    cat_int(c, &known, "SELECT 1 FROM ducklake_table_column_stats WHERE "
            "table_id = %d AND column_id = %lld", TABLE_ID, (long long)x->column_id);
    int empty = x->has_nulls && x->nulls == x->values;    /* nulls alone */
    const uint8_t *lo = ranged ? x->min : NULL, *hi = ranged ? x->max : NULL;
    size_t lo_len = x->min_len, hi_len = x->max_len;
    uint8_t *old_lo = NULL, *old_hi = NULL;
    int had_range = 0;
    if (rc == SQLITE_OK
        && sqlite3_prepare_v2(c->db, "SELECT min_value, max_value FROM sp_range "
                              "WHERE column_id = ?", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_int64(st, 1, x->column_id);
        if (sqlite3_step(st) == SQLITE_ROW) {
            had_range = 1;
            if (sqlite3_column_type(st, 0) == SQLITE_BLOB
                && sqlite3_column_type(st, 1) == SQLITE_BLOB) {
                size_t a = (size_t)sqlite3_column_bytes(st, 0);
                size_t b = (size_t)sqlite3_column_bytes(st, 1);
                old_lo = (uint8_t *)copy_bytes(sqlite3_column_blob(st, 0), a);
                old_hi = (uint8_t *)copy_bytes(sqlite3_column_blob(st, 1), b);
                int ok = old_lo && old_hi;
                if (ok && empty) {
                    lo = old_lo; lo_len = a;
                    hi = old_hi; hi_len = b;
                } else if (ok && ranged) {
                    if (value_cmp(x, old_lo, a, lo, lo_len, &ok) <= 0) { lo = old_lo; lo_len = a; }
                    if (value_cmp(x, old_hi, b, hi, hi_len, &ok) >= 0) { hi = old_hi; hi_len = b; }
                }
                if (!ok) lo = hi = NULL;
            } else {
                lo = hi = NULL;           /* the table's range had ended */
            }
        }
    }
    sqlite3_finalize(st);
    st = NULL;
    int keep_range = !(empty && !had_range);  /* nulls alone start no range */
    int nulls = !x->has_nulls || x->nulls > 0 || x->values < rows
        || (!known && rows_before > 0);   /* a new column: null in the files before */
    if (rc == SQLITE_OK)
        rc = sqlite3_prepare_v2(c->db, known
            ? "UPDATE ducklake_table_column_stats SET contains_null = "
              "max(coalesce(contains_null, 0), ?3), min_value = ?4, max_value = ?5 "
              "WHERE table_id = ?1 AND column_id = ?2"
            : "INSERT INTO ducklake_table_column_stats VALUES (?1, ?2, ?3, NULL, "
              "?4, ?5, NULL)", -1, &st, NULL);
    if (rc == SQLITE_OK) {
        sqlite3_bind_int(st, 1, TABLE_ID);
        sqlite3_bind_int64(st, 2, x->column_id);
        sqlite3_bind_int(st, 3, nulls);
        bind_text_or_null(st, 4, lo ? value_text(x, lo, lo_len) : NULL);
        bind_text_or_null(st, 5, hi ? value_text(x, hi, hi_len) : NULL);
        rc = sqlite3_step(st) == SQLITE_DONE ? SQLITE_OK : SQLITE_ERROR;
    }
    sqlite3_finalize(st);
    st = NULL;
    if (rc == SQLITE_OK && keep_range)
        rc = sqlite3_prepare_v2(c->db, "INSERT OR REPLACE INTO sp_range VALUES "
                                       "(?1, ?2, ?3)", -1, &st, NULL);
    if (rc == SQLITE_OK && keep_range) {
        sqlite3_bind_int64(st, 1, x->column_id);
        if (lo && hi) {
            sqlite3_bind_blob(st, 2, lo, (int)lo_len, SQLITE_TRANSIENT);
            sqlite3_bind_blob(st, 3, hi, (int)hi_len, SQLITE_TRANSIENT);
        }
        rc = sqlite3_step(st) == SQLITE_DONE ? SQLITE_OK : SQLITE_ERROR;
    }
    sqlite3_finalize(st);
    free(old_lo);
    free(old_hi);
    if (rc != SQLITE_OK) {
        fail(err, errlen, "catalog: %s", sqlite3_errmsg(c->db));
        return -1;
    }
    return 0;
}

static int add_file(SpCatalog *c, const char *path, int64_t file_size,
                    PqFooter *f, size_t footer_len, char *err, size_t errlen) {
    if (!c->has_table) {
        char table[37];
        name_uuid(c->data_path, "table", table);
        if (cat_exec(c, err, errlen,
                "INSERT INTO ducklake_table VALUES (%d, %Q, %lld, NULL, 0, %Q, 'data/', 1)",
                TABLE_ID, table, (long long)c->snapshot, TABLE_NAME) != 0)
            return -1;
        c->created = 1;
        if (c->next_catalog_id <= TABLE_ID) c->next_catalog_id = TABLE_ID + 1;
    }
    PqNode *root = &f->nodes[0];
    int child = 1;
    for (int k = 0; k < root->num_children; k++) {
        if (match_column(c, f, child, 0, 1, path, err, errlen) != 0) return -1;
        child = next_sibling(f, child);
    }
    c->has_table = 1;
    int64_t mapping = file_mapping(c, f, err, errlen);
    if (mapping < 0) return -1;
    int64_t file_id = c->next_file_id++;
    int had;
    int64_t row_id = cat_int(c, &had, "SELECT next_row_id FROM ducklake_table_stats "
                                      "WHERE table_id = %d", TABLE_ID);
    if (cat_exec(c, err, errlen,
            "INSERT INTO ducklake_data_file VALUES (%lld, %d, %lld, NULL, NULL, %Q, "
            "0, 'parquet', %lld, %lld, %lld, %lld, NULL, NULL, %lld, NULL)",
            (long long)file_id, TABLE_ID, (long long)c->snapshot, path,
            (long long)f->num_rows, (long long)file_size, (long long)footer_len,
            (long long)row_id, (long long)mapping) != 0)
        return -1;
    if (!had && cat_exec(c, err, errlen,
            "INSERT INTO ducklake_table_stats VALUES (%d, 0, 0, 0)", TABLE_ID) != 0)
        return -1;
    if (cat_exec(c, err, errlen,
            "UPDATE ducklake_table_stats SET record_count = record_count + %lld, "
            "next_row_id = next_row_id + %lld, file_size_bytes = file_size_bytes "
            "+ %lld WHERE table_id = %d",
            (long long)f->num_rows, (long long)f->num_rows, (long long)file_size,
            TABLE_ID) != 0)
        return -1;
    for (int i = 0; i < f->n_leaves; i++)
        if (leaf_stats(c, &f->nodes[f->leaves[i]], file_id, f->num_rows,
                       row_id, err, errlen) != 0)
            return -1;
    /* a column of the catalog that this file lacks is null in all of
     * the file's rows */
    if (cat_exec(c, err, errlen,
            "INSERT INTO ducklake_file_column_stats SELECT %lld, %d, column_id, 0, 0, "
            "%lld, NULL, NULL, NULL, NULL FROM ducklake_column WHERE table_id = %d "
            "AND end_snapshot IS NULL AND column_type NOT IN ('list', 'struct') "
            "AND column_id NOT IN (SELECT column_id FROM ducklake_file_column_stats "
            "WHERE data_file_id = %lld);"
            "UPDATE ducklake_table_column_stats SET contains_null = 1 WHERE "
            "table_id = %d AND column_id IN (SELECT column_id FROM "
            "ducklake_file_column_stats WHERE data_file_id = %lld AND value_count = 0 "
            "AND null_count > 0)",
            (long long)file_id, TABLE_ID, (long long)f->num_rows, TABLE_ID,
            (long long)file_id, TABLE_ID, (long long)file_id) != 0)
        return -1;
    if (f->has_first_row)
        return cat_exec(c, err, errlen, "INSERT INTO sp_file VALUES (%lld, %Q, %lld)",
                        (long long)file_id, f->run, (long long)f->first_row);
    return cat_exec(c, err, errlen, "INSERT INTO sp_file VALUES (%lld, %Q, NULL)",
                    (long long)file_id, f->run);
}

int sp_catalog_add(SpCatalog *c, const char *path, int64_t file_size,
                   const uint8_t *footer, size_t footer_len,
                   char *err, size_t errlen) {
    if (!c || !path || !footer) { fail(err, errlen, "catalog: nothing to add"); return -1; }
    if (sp_catalog_lists(c, path)) return 0;
    PqFooter f;
    char why[160];
    if (footer_read(footer, footer_len, &f, why, sizeof why) != 0) {
        fail(err, errlen, "catalog: %s: %s", path, why);
        return -1;
    }
    /* all of a file's rows or none: the counters go back with them */
    int64_t next_file = c->next_file_id, next_catalog = c->next_catalog_id;
    int created = c->created, altered = c->altered, has_table = c->has_table;
    int rc = cat_exec(c, err, errlen, "SAVEPOINT one_file");
    if (rc == 0) {
        rc = add_file(c, path, file_size, &f, footer_len, err, errlen);
        if (rc == 0) {
            rc = cat_exec(c, err, errlen, "RELEASE one_file");
        } else {
            cat_exec(c, NULL, 0, "ROLLBACK TO one_file; RELEASE one_file");
            c->next_file_id = next_file;
            c->next_catalog_id = next_catalog;
            c->created = created;
            c->altered = altered;
            c->has_table = has_table;
        }
    }
    footer_free(&f);
    if (rc == 0) { c->inserted = 1; c->staged++; }
    return rc;
}

uint8_t *sp_catalog_bytes(SpCatalog *c, double now, size_t *out_len,
                          char *err, size_t errlen) {
    if (out_len) *out_len = 0;
    if (!c) { fail(err, errlen, "no catalog"); return NULL; }
    int rc = 0;
    if (c->created || c->altered || c->inserted) {
        char when[40];
        time_text(now, when, sizeof when);
        if (c->created || c->altered) {
            c->schema_version++;
            rc = cat_exec(c, err, errlen,
                "INSERT INTO ducklake_schema_versions VALUES (%lld, %lld, %d)",
                (long long)c->snapshot, (long long)c->schema_version, TABLE_ID);
        }
        char changes[128];
        int at = 0;
        changes[0] = 0;
        if (c->created)
            at += snprintf(changes + at, sizeof changes - (size_t)at,
                           "created_table:\"main\".\"%s\"", TABLE_NAME);
        else if (c->altered)
            at += snprintf(changes + at, sizeof changes - (size_t)at,
                           "altered_table:%d", TABLE_ID);
        if (c->inserted)
            snprintf(changes + at, sizeof changes - (size_t)at,
                     "%sinserted_into_table:%d", at ? "," : "", TABLE_ID);
        if (rc == 0) rc = cat_exec(c, err, errlen,
            "INSERT INTO ducklake_snapshot VALUES (%lld, %Q, %lld, %lld, %lld);"
            "INSERT INTO ducklake_snapshot_changes VALUES (%lld, %Q, NULL, NULL, NULL)",
            (long long)c->snapshot, when, (long long)c->schema_version,
            (long long)c->next_catalog_id, (long long)c->next_file_id,
            (long long)c->snapshot, changes);
    }
    if (rc == 0) rc = cat_exec(c, err, errlen, "COMMIT");
    if (rc != 0) return NULL;
    sqlite3_int64 n = 0;
    unsigned char *image = sqlite3_serialize(c->db, "main", &n, 0);
    uint8_t *out = image ? malloc(n > 0 ? (size_t)n : 1) : NULL;
    if (out) memcpy(out, image, (size_t)n);
    sqlite3_free(image);
    if (!out) { fail(err, errlen, "catalog: memory is out"); return NULL; }
    if (cat_begin(c, err, errlen) != 0) { free(out); return NULL; }
    if (out_len) *out_len = (size_t)n;
    return out;
}

int64_t sp_catalog_staged(const SpCatalog *c) { return c ? c->staged : 0; }
