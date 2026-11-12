/* sp.c -- the stream core. sp.h is the spec; read it first. */
#include "sp.h"

#include <errno.h>
#include <math.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ---- small things ------------------------------------------------------- */

static void fail(char *err, size_t errlen, const char *fmt, ...) {
    if (!err || errlen == 0) return;
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(err, errlen, fmt, ap);
    va_end(ap);
}

void sp_free(void *p) { free(p); }

static char *copy_text(const char *s, size_t n) {
    char *p = malloc(n + 1);
    if (!p) return NULL;
    memcpy(p, s, n);
    p[n] = 0;
    return p;
}

/* 1 when the n bytes at s are well-formed UTF-8 with no 0 byte. */
static int is_utf8(const unsigned char *s, size_t n) {
    size_t i = 0;
    while (i < n) {
        unsigned char c = s[i];
        size_t more;
        uint32_t cp, min;
        if (c == 0) return 0;
        if (c < 0x80) { i++; continue; }
        if ((c & 0xE0) == 0xC0) { more = 1; cp = c & 0x1F; min = 0x80; }
        else if ((c & 0xF0) == 0xE0) { more = 2; cp = c & 0x0F; min = 0x800; }
        else if ((c & 0xF8) == 0xF0) { more = 3; cp = c & 0x07; min = 0x10000; }
        else return 0;
        if (n - i <= more) return 0;
        for (size_t k = 1; k <= more; k++) {
            if ((s[i + k] & 0xC0) != 0x80) return 0;
            cp = (cp << 6) | (s[i + k] & 0x3F);
        }
        if (cp < min || cp > 0x10FFFF || (cp >= 0xD800 && cp <= 0xDFFF))
            return 0;
        i += more + 1;
    }
    return 1;
}

/* ---- growable text ------------------------------------------------------ */

typedef struct { char *p; size_t len, cap; int oom; } Text;

static void text_put(Text *b, const char *s, size_t n) {
    if (b->oom) return;
    if (b->len + n + 1 > b->cap) {
        size_t cap = b->cap ? b->cap : 256;
        while (cap < b->len + n + 1) cap *= 2;
        char *p = realloc(b->p, cap);
        if (!p) { b->oom = 1; return; }
        b->p = p;
        b->cap = cap;
    }
    memcpy(b->p + b->len, s, n);
    b->len += n;
    b->p[b->len] = 0;
}

static void text_str(Text *b, const char *s) { text_put(b, s, strlen(s)); }

/* A JSON string: quotes, backslashes and control bytes escaped; UTF-8
 * as it is. */
static void text_json_string(Text *b, const char *s) {
    text_put(b, "\"", 1);
    for (const unsigned char *c = (const unsigned char *)s; *c; c++) {
        char esc[8];
        switch (*c) {
        case '"':  text_put(b, "\\\"", 2); break;
        case '\\': text_put(b, "\\\\", 2); break;
        case '\n': text_put(b, "\\n", 2); break;
        case '\r': text_put(b, "\\r", 2); break;
        case '\t': text_put(b, "\\t", 2); break;
        default:
            if (*c < 0x20) {
                snprintf(esc, sizeof esc, "\\u%04x", *c);
                text_put(b, esc, 6);
            } else {
                text_put(b, (const char *)c, 1);
            }
        }
    }
    text_put(b, "\"", 1);
}

static void text_integer(Text *b, int64_t v) {
    char num[32];
    snprintf(num, sizeof num, "%lld", (long long)v);
    text_str(b, num);
}

/* A number that reads back as the same double. */
static void text_number(Text *b, double v) {
    char num[40];
    snprintf(num, sizeof num, "%.17g", v);
    text_str(b, num);
}

/* ---- the ticket --------------------------------------------------------- */

typedef struct {
    char *checkpoint;
    int64_t first_row, final_rows, rows;
    uint8_t has_first_row, has_final_rows, has_rows;
} TicketFile;

typedef struct {
    uint8_t known;
    char *text;
    int64_t integer;
    double number;
} Member;

struct SpTicket {
    Member m[SP_T_FIELDS];
    TicketFile *files;
    int32_t n_files, cap_files;
};

static const struct { const char *name; int kind; } FIELD[SP_T_FIELDS] = {
    [SP_T_NAME] = {"name", SP_TEXT},
    [SP_T_HOST] = {"host", SP_TEXT},
    [SP_T_PORT] = {"port", SP_INTEGER},
    [SP_T_RUN] = {"run", SP_TEXT},
    [SP_T_BEAT_SECONDS] = {"beat_seconds", SP_NUMBER},
    [SP_T_STALE_AFTER] = {"stale_after", SP_NUMBER},
    [SP_T_CHECKPOINT_SECONDS] = {"checkpoint_seconds", SP_NUMBER},
    [SP_T_SHIP_SECONDS] = {"ship_seconds", SP_NUMBER},
    [SP_T_STORED_ROW] = {"stored_row", SP_INTEGER},
    [SP_T_STORED_AT] = {"stored_at", SP_NUMBER},
    [SP_T_KEEP_SECONDS] = {"keep_seconds", SP_NUMBER},
    [SP_T_CATALOG_ETAG] = {"catalog_etag", SP_TEXT},
    [SP_T_REVISION] = {"revision", SP_INTEGER},
    [SP_T_TS] = {"ts", SP_NUMBER},
    [SP_T_ARCHIVE] = {"archive", SP_TEXT},
};

/* The order the members are written in; `unfinished` and `files` go
 * after stored_at. `archive` is written only when it is known. */
static const int WRITE_ORDER[] = {
    SP_T_NAME, SP_T_HOST, SP_T_PORT, SP_T_RUN, SP_T_BEAT_SECONDS,
    SP_T_STALE_AFTER, SP_T_CHECKPOINT_SECONDS, SP_T_SHIP_SECONDS,
    SP_T_STORED_ROW, SP_T_STORED_AT, -1 /* unfinished, files */,
    SP_T_KEEP_SECONDS, SP_T_CATALOG_ETAG, SP_T_REVISION, SP_T_TS,
};

static int field_ok(int field) { return field >= 0 && field < SP_T_FIELDS; }

const char *sp_ticket_field_name(int field) {
    return field_ok(field) ? FIELD[field].name : NULL;
}

int sp_ticket_field_kind(int field) {
    return field_ok(field) ? FIELD[field].kind : -1;
}

SpTicket *sp_ticket_new(void) { return calloc(1, sizeof(SpTicket)); }

void sp_ticket_free(SpTicket *t) {
    if (!t) return;
    for (int i = 0; i < SP_T_FIELDS; i++) free(t->m[i].text);
    for (int32_t i = 0; i < t->n_files; i++) free(t->files[i].checkpoint);
    free(t->files);
    free(t);
}

int sp_ticket_clear(SpTicket *t, int field) {
    if (!t || !field_ok(field)) return -1;
    free(t->m[field].text);
    memset(&t->m[field], 0, sizeof t->m[field]);
    return 0;
}

static int want(const SpTicket *t, int field, int kind, const char *what,
                char *err, size_t errlen) {
    if (!t) { fail(err, errlen, "ticket: no ticket"); return -1; }
    if (!field_ok(field)) {
        fail(err, errlen, "ticket: no member number %d", field);
        return -1;
    }
    int is = FIELD[field].kind;
    if (is == kind || (kind == SP_INTEGER && is == SP_NUMBER)) return 0;
    fail(err, errlen, "ticket: %s is %s, and was given %s", FIELD[field].name,
         is == SP_TEXT ? "text" : is == SP_INTEGER ? "an integer" : "a number",
         what);
    return -1;
}

int sp_ticket_set_text(SpTicket *t, int field, const char *text,
                       char *err, size_t errlen) {
    if (want(t, field, SP_TEXT, "text", err, errlen)) return -1;
    if (!text) return sp_ticket_clear(t, field);
    size_t n = strlen(text);
    if (!is_utf8((const unsigned char *)text, n)) {
        fail(err, errlen, "ticket: %s is not UTF-8 text", FIELD[field].name);
        return -1;
    }
    char *copy = copy_text(text, n);
    if (!copy) { fail(err, errlen, "ticket: out of memory"); return -1; }
    free(t->m[field].text);
    t->m[field].text = copy;
    t->m[field].known = 1;
    return 0;
}

int sp_ticket_set_integer(SpTicket *t, int field, int64_t value,
                          char *err, size_t errlen) {
    if (want(t, field, SP_INTEGER, "an integer", err, errlen)) return -1;
    t->m[field].known = 1;
    t->m[field].integer = value;
    t->m[field].number = (double)value;
    return 0;
}

int sp_ticket_set_number(SpTicket *t, int field, double value,
                         char *err, size_t errlen) {
    if (want(t, field, SP_NUMBER, "a number", err, errlen)) return -1;
    if (FIELD[field].kind != SP_NUMBER) {
        fail(err, errlen, "ticket: %s is an integer, and was given a number",
             FIELD[field].name);
        return -1;
    }
    if (!isfinite(value)) {
        fail(err, errlen, "ticket: %s is not a finite number",
             FIELD[field].name);
        return -1;
    }
    t->m[field].known = 1;
    t->m[field].number = value;
    return 0;
}

int sp_ticket_add_file(SpTicket *t, const char *checkpoint,
                       const int64_t *first_row, const int64_t *final_rows,
                       const int64_t *rows, char *err, size_t errlen) {
    if (!t || !checkpoint) {
        fail(err, errlen, "ticket: a file needs its checkpoint path");
        return -1;
    }
    size_t n = strlen(checkpoint);
    if (!is_utf8((const unsigned char *)checkpoint, n)) {
        fail(err, errlen, "ticket: a file's checkpoint path is not UTF-8 text");
        return -1;
    }
    if (t->n_files == t->cap_files) {
        int32_t cap = t->cap_files ? t->cap_files * 2 : 4;
        TicketFile *p = realloc(t->files, (size_t)cap * sizeof *p);
        if (!p) { fail(err, errlen, "ticket: out of memory"); return -1; }
        t->files = p;
        t->cap_files = cap;
    }
    TicketFile f = {0};
    f.checkpoint = copy_text(checkpoint, n);
    if (!f.checkpoint) { fail(err, errlen, "ticket: out of memory"); return -1; }
    if (first_row) { f.first_row = *first_row; f.has_first_row = 1; }
    if (final_rows) { f.final_rows = *final_rows; f.has_final_rows = 1; }
    if (rows) { f.rows = *rows; f.has_rows = 1; }
    t->files[t->n_files++] = f;
    return 0;
}

const char *sp_ticket_text(const SpTicket *t, int field) {
    if (!t || !field_ok(field) || FIELD[field].kind != SP_TEXT) return NULL;
    return t->m[field].known ? t->m[field].text : NULL;
}

int sp_ticket_integer(const SpTicket *t, int field, int64_t *out) {
    if (!t || !field_ok(field) || FIELD[field].kind != SP_INTEGER) return 0;
    if (!t->m[field].known) return 0;
    if (out) *out = t->m[field].integer;
    return 1;
}

int sp_ticket_number(const SpTicket *t, int field, double *out) {
    if (!t || !field_ok(field) || FIELD[field].kind == SP_TEXT) return 0;
    if (!t->m[field].known) return 0;
    if (out) *out = t->m[field].number;
    return 1;
}

int32_t sp_ticket_file_count(const SpTicket *t) { return t ? t->n_files : 0; }

int sp_ticket_file(const SpTicket *t, int32_t index, const char **checkpoint,
                   int64_t *first_row, int *has_first_row,
                   int64_t *final_rows, int *has_final_rows,
                   int64_t *rows, int *has_rows) {
    if (!t || index < 0 || index >= t->n_files) return -1;
    const TicketFile *f = &t->files[index];
    if (checkpoint) *checkpoint = f->checkpoint;
    if (first_row) *first_row = f->first_row;
    if (has_first_row) *has_first_row = f->has_first_row;
    if (final_rows) *final_rows = f->final_rows;
    if (has_final_rows) *has_final_rows = f->has_final_rows;
    if (rows) *rows = f->rows;
    if (has_rows) *has_rows = f->has_rows;
    return 0;
}

/* ---- writing ------------------------------------------------------------ */

static void write_member(Text *b, const SpTicket *t, int field) {
    const Member *m = &t->m[field];
    text_json_string(b, FIELD[field].name);
    text_str(b, ": ");
    if (!m->known) text_str(b, "null");
    else if (FIELD[field].kind == SP_TEXT) text_json_string(b, m->text);
    else if (FIELD[field].kind == SP_INTEGER) text_integer(b, m->integer);
    else text_number(b, m->number);
}

static void write_optional(Text *b, int has, int64_t v) {
    if (has) text_integer(b, v);
    else text_str(b, "null");
}

char *sp_ticket_write(const SpTicket *t, size_t *out_len) {
    if (!t) return NULL;
    Text b = {0};
    text_str(&b, "{");
    size_t n = sizeof WRITE_ORDER / sizeof WRITE_ORDER[0];
    for (size_t i = 0; i < n; i++) {
        if (i) text_str(&b, ", ");
        if (WRITE_ORDER[i] >= 0) {
            write_member(&b, t, WRITE_ORDER[i]);
            continue;
        }
        text_str(&b, "\"unfinished\": [");
        for (int32_t k = 0; k < t->n_files; k++) {
            if (k) text_str(&b, ", ");
            text_json_string(&b, t->files[k].checkpoint);
        }
        text_str(&b, "], \"files\": [");
        for (int32_t k = 0; k < t->n_files; k++) {
            const TicketFile *f = &t->files[k];
            if (k) text_str(&b, ", ");
            text_str(&b, "{\"checkpoint\": ");
            text_json_string(&b, f->checkpoint);
            text_str(&b, ", \"first_row\": ");
            write_optional(&b, f->has_first_row, f->first_row);
            text_str(&b, ", \"final_rows\": ");
            write_optional(&b, f->has_final_rows, f->final_rows);
            text_str(&b, ", \"rows\": ");
            write_optional(&b, f->has_rows, f->rows);
            text_str(&b, "}");
        }
        text_str(&b, "]");
    }
    if (t->m[SP_T_ARCHIVE].known) {
        text_str(&b, ", ");
        write_member(&b, t, SP_T_ARCHIVE);
    }
    text_str(&b, "}");
    if (b.oom) { free(b.p); return NULL; }
    if (out_len) *out_len = b.len;
    return b.p;
}

/* ---- reading: a JSON reader for one object ------------------------------ */

#define MAX_DEPTH 32

typedef struct {
    const char *p, *end;
    char *err;
    size_t errlen;
    int failed;
} Reader;

static void refuse(Reader *r, const char *fmt, ...) {
    if (r->failed) return;
    r->failed = 1;
    if (!r->err || r->errlen == 0) return;
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(r->err, r->errlen, fmt, ap);
    va_end(ap);
}

static void skip_space(Reader *r) {
    while (r->p < r->end && (*r->p == ' ' || *r->p == '\t' || *r->p == '\n'
                             || *r->p == '\r'))
        r->p++;
}

static int peek(Reader *r) {
    skip_space(r);
    return r->p < r->end ? (unsigned char)*r->p : -1;
}

static int take(Reader *r, char c) {
    if (peek(r) != (unsigned char)c) return 0;
    r->p++;
    return 1;
}

static int take_word(Reader *r, const char *word) {
    size_t n = strlen(word);
    skip_space(r);
    if ((size_t)(r->end - r->p) < n || memcmp(r->p, word, n) != 0) return 0;
    r->p += n;
    return 1;
}

static int hex4(Reader *r, uint32_t *out) {
    if (r->end - r->p < 4) return 0;
    uint32_t v = 0;
    for (int i = 0; i < 4; i++) {
        char c = r->p[i];
        v <<= 4;
        if (c >= '0' && c <= '9') v |= (uint32_t)(c - '0');
        else if (c >= 'a' && c <= 'f') v |= (uint32_t)(c - 'a' + 10);
        else if (c >= 'A' && c <= 'F') v |= (uint32_t)(c - 'A' + 10);
        else return 0;
    }
    r->p += 4;
    *out = v;
    return 1;
}

static void put_utf8(Text *b, uint32_t cp) {
    char u[4];
    if (cp < 0x80) { u[0] = (char)cp; text_put(b, u, 1); }
    else if (cp < 0x800) {
        u[0] = (char)(0xC0 | (cp >> 6));
        u[1] = (char)(0x80 | (cp & 0x3F));
        text_put(b, u, 2);
    } else if (cp < 0x10000) {
        u[0] = (char)(0xE0 | (cp >> 12));
        u[1] = (char)(0x80 | ((cp >> 6) & 0x3F));
        u[2] = (char)(0x80 | (cp & 0x3F));
        text_put(b, u, 3);
    } else {
        u[0] = (char)(0xF0 | (cp >> 18));
        u[1] = (char)(0x80 | ((cp >> 12) & 0x3F));
        u[2] = (char)(0x80 | ((cp >> 6) & 0x3F));
        u[3] = (char)(0x80 | (cp & 0x3F));
        text_put(b, u, 4);
    }
}

/* A JSON string, as UTF-8 text the caller frees. NULL on a fault. A
 * string that holds a 0 byte, or is not UTF-8, is refused: the core's
 * text is C text. */
static char *read_string(Reader *r) {
    if (!take(r, '"')) { refuse(r, "ticket: text expected"); return NULL; }
    Text b = {0};
    text_put(&b, "", 0);
    for (;;) {
        if (r->p >= r->end) { refuse(r, "ticket: text with no end"); break; }
        unsigned char c = (unsigned char)*r->p++;
        if (c == '"') {
            if (b.oom) { refuse(r, "ticket: out of memory"); break; }
            if (!is_utf8((const unsigned char *)b.p, b.len)) {
                refuse(r, "ticket: text that is not UTF-8");
                break;
            }
            return b.p;
        }
        if (c < 0x20) { refuse(r, "ticket: a control byte in text"); break; }
        if (c != '\\') { text_put(&b, (const char *)&c, 1); continue; }
        if (r->p >= r->end) { refuse(r, "ticket: text with no end"); break; }
        char e = *r->p++;
        uint32_t cp, low;
        switch (e) {
        case '"': case '\\': case '/': text_put(&b, &e, 1); break;
        case 'b': text_put(&b, "\b", 1); break;
        case 'f': text_put(&b, "\f", 1); break;
        case 'n': text_put(&b, "\n", 1); break;
        case 'r': text_put(&b, "\r", 1); break;
        case 't': text_put(&b, "\t", 1); break;
        case 'u':
            if (!hex4(r, &cp)) { refuse(r, "ticket: a bad \\u in text"); goto bad; }
            if (cp >= 0xD800 && cp <= 0xDBFF) {
                if (r->end - r->p < 2 || r->p[0] != '\\' || r->p[1] != 'u') {
                    refuse(r, "ticket: half a character in text");
                    goto bad;
                }
                r->p += 2;
                if (!hex4(r, &low) || low < 0xDC00 || low > 0xDFFF) {
                    refuse(r, "ticket: half a character in text");
                    goto bad;
                }
                cp = 0x10000 + ((cp - 0xD800) << 10) + (low - 0xDC00);
            } else if (cp >= 0xDC00 && cp <= 0xDFFF) {
                refuse(r, "ticket: half a character in text");
                goto bad;
            }
            if (cp == 0) { refuse(r, "ticket: a 0 character in text"); goto bad; }
            put_utf8(&b, cp);
            break;
        default:
            refuse(r, "ticket: a bad escape in text");
            goto bad;
        }
    }
bad:
    free(b.p);
    return NULL;
}

/* A JSON number. *is_integer is 1 when it is written as an integer that
 * fits int64, and then *integer holds it. */
static int read_number(Reader *r, double *number, int64_t *integer,
                       int *is_integer) {
    skip_space(r);
    const char *s = r->p;
    const char *q = s;
    int plain = 1;
    if (q < r->end && *q == '-') q++;
    const char *digits = q;
    while (q < r->end && *q >= '0' && *q <= '9') q++;
    if (q == digits) { refuse(r, "ticket: a number expected"); return 0; }
    if (q < r->end && *q == '.') {
        plain = 0;
        q++;
        const char *frac = q;
        while (q < r->end && *q >= '0' && *q <= '9') q++;
        if (q == frac) { refuse(r, "ticket: a bad number"); return 0; }
    }
    if (q < r->end && (*q == 'e' || *q == 'E')) {
        plain = 0;
        q++;
        if (q < r->end && (*q == '+' || *q == '-')) q++;
        const char *exp = q;
        while (q < r->end && *q >= '0' && *q <= '9') q++;
        if (q == exp) { refuse(r, "ticket: a bad number"); return 0; }
    }
    size_t n = (size_t)(q - s);
    char buf[64];
    if (n >= sizeof buf) { refuse(r, "ticket: a number too long"); return 0; }
    memcpy(buf, s, n);
    buf[n] = 0;
    *is_integer = 0;
    if (plain) {
        errno = 0;
        long long v = strtoll(buf, NULL, 10);
        if (errno == 0) { *integer = (int64_t)v; *is_integer = 1; }
    }
    *number = strtod(buf, NULL);
    if (!isfinite(*number)) { refuse(r, "ticket: a number too large"); return 0; }
    r->p = q;
    return 1;
}

/* Pass over any JSON value: a member the core does not know. */
static void skip_value(Reader *r, int depth) {
    if (r->failed) return;
    if (depth > MAX_DEPTH) { refuse(r, "ticket: nested too deep"); return; }
    int c = peek(r);
    if (c == '"') { free(read_string(r)); return; }
    if (c == '{') {
        r->p++;
        if (take(r, '}')) return;
        do {
            free(read_string(r));
            if (r->failed) return;
            if (!take(r, ':')) { refuse(r, "ticket: ':' expected"); return; }
            skip_value(r, depth + 1);
            if (r->failed) return;
        } while (take(r, ','));
        if (!take(r, '}')) refuse(r, "ticket: '}' expected");
        return;
    }
    if (c == '[') {
        r->p++;
        if (take(r, ']')) return;
        do {
            skip_value(r, depth + 1);
            if (r->failed) return;
        } while (take(r, ','));
        if (!take(r, ']')) refuse(r, "ticket: ']' expected");
        return;
    }
    if (take_word(r, "true") || take_word(r, "false") || take_word(r, "null"))
        return;
    double number;
    int64_t integer;
    int is_integer;
    read_number(r, &number, &integer, &is_integer);
}

/* An integer member of a file, or null. */
static void read_optional(Reader *r, const char *what, int64_t *out,
                          uint8_t *has) {
    if (take_word(r, "null")) { *has = 0; return; }
    double number;
    int is_integer;
    if (peek(r) == '"' || peek(r) == '{' || peek(r) == '['
        || !read_number(r, &number, out, &is_integer) || !is_integer) {
        refuse(r, "ticket: a file's %s is not an integer", what);
        return;
    }
    *has = 1;
}

static void read_files(Reader *r, SpTicket *t) {
    if (take_word(r, "null")) return;
    if (!take(r, '[')) { refuse(r, "ticket: files is not a list"); return; }
    if (take(r, ']')) return;
    do {
        TicketFile f = {0};
        if (!take(r, '{')) { refuse(r, "ticket: a file is not an object"); return; }
        if (!take(r, '}')) {
            do {
                char *key = read_string(r);
                if (!key) { free(f.checkpoint); return; }
                if (!take(r, ':')) {
                    refuse(r, "ticket: ':' expected");
                } else if (strcmp(key, "checkpoint") == 0) {
                    free(f.checkpoint);
                    f.checkpoint = read_string(r);
                    if (!f.checkpoint)
                        refuse(r, "ticket: a file's checkpoint is not text");
                } else if (strcmp(key, "first_row") == 0) {
                    read_optional(r, "first_row", &f.first_row, &f.has_first_row);
                } else if (strcmp(key, "final_rows") == 0) {
                    read_optional(r, "final_rows", &f.final_rows, &f.has_final_rows);
                } else if (strcmp(key, "rows") == 0) {
                    read_optional(r, "rows", &f.rows, &f.has_rows);
                } else {
                    skip_value(r, 2);
                }
                free(key);
                if (r->failed) { free(f.checkpoint); return; }
            } while (take(r, ','));
            if (!take(r, '}')) {
                refuse(r, "ticket: '}' expected");
                free(f.checkpoint);
                return;
            }
        }
        if (!f.checkpoint) {
            refuse(r, "ticket: a file with no checkpoint");
            return;
        }
        int rc = sp_ticket_add_file(t, f.checkpoint,
                                    f.has_first_row ? &f.first_row : NULL,
                                    f.has_final_rows ? &f.final_rows : NULL,
                                    f.has_rows ? &f.rows : NULL, NULL, 0);
        free(f.checkpoint);
        if (rc) { refuse(r, "ticket: out of memory"); return; }
    } while (take(r, ','));
    if (!take(r, ']')) refuse(r, "ticket: ']' expected");
}

static void read_member(Reader *r, SpTicket *t, const char *key) {
    if (strcmp(key, "files") == 0) {
        /* a member given twice: the last one stands */
        for (int32_t i = 0; i < t->n_files; i++) free(t->files[i].checkpoint);
        t->n_files = 0;
        read_files(r, t);
        return;
    }
    int field = -1;
    for (int i = 0; i < SP_T_FIELDS; i++)
        if (strcmp(key, FIELD[i].name) == 0) field = i;
    if (field < 0) { skip_value(r, 1); return; }   /* `unfinished` too */
    if (take_word(r, "null")) { sp_ticket_clear(t, field); return; }
    if (FIELD[field].kind == SP_TEXT) {
        if (peek(r) != '"') { refuse(r, "ticket: %s is not text", key); return; }
        char *s = read_string(r);
        if (!s) return;
        free(t->m[field].text);
        t->m[field].text = s;
        t->m[field].known = 1;
        return;
    }
    int c = peek(r);
    double number;
    int64_t integer = 0;
    int is_integer;
    if (!(c == '-' || (c >= '0' && c <= '9'))
        || !read_number(r, &number, &integer, &is_integer)) {
        refuse(r, "ticket: %s is not a number", key);
        return;
    }
    if (FIELD[field].kind == SP_INTEGER) {
        if (!is_integer) {
            /* 8815.0 is the integer 8815; 8815.5 is not one */
            if (number != floor(number) || fabs(number) > 9007199254740992.0) {
                refuse(r, "ticket: %s is not an integer", key);
                return;
            }
            integer = (int64_t)number;
        }
        t->m[field].integer = integer;
    }
    t->m[field].number = number;
    t->m[field].known = 1;
}

SpTicket *sp_ticket_read(const char *text, size_t len,
                         char *err, size_t errlen) {
    Reader r = {text, text ? text + len : NULL, err, errlen, 0};
    if (err && errlen) err[0] = 0;
    SpTicket *t = sp_ticket_new();
    if (!t) { fail(err, errlen, "ticket: out of memory"); return NULL; }
    if (!text) refuse(&r, "ticket: no text");
    else if (!take(&r, '{')) refuse(&r, "ticket: not a JSON object");
    else if (!take(&r, '}')) {
        do {
            char *key = read_string(&r);
            if (!key) break;
            if (!take(&r, ':')) refuse(&r, "ticket: ':' expected");
            else read_member(&r, t, key);
            free(key);
        } while (!r.failed && take(&r, ','));
        if (!r.failed && !take(&r, '}')) refuse(&r, "ticket: '}' expected");
    }
    if (!r.failed && peek(&r) != -1) refuse(&r, "ticket: text after the object");
    if (r.failed) {
        sp_ticket_free(t);
        return NULL;
    }
    return t;
}

/* ---- the stale rule ----------------------------------------------------- */

double sp_ticket_stale_after(const SpTicket *t, double if_absent) {
    double promised;
    if (sp_ticket_number(t, SP_T_STALE_AFTER, &promised)) return promised;
    return if_absent;
}

int sp_ticket_is_stale(const SpTicket *t, double now, double if_absent) {
    double ts;
    if (!sp_ticket_number(t, SP_T_TS, &ts)) return 1;
    return now - ts >= sp_ticket_stale_after(t, if_absent);
}
