/* sp.c -- the stream core. sp.h is the spec; read it first. */
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L   /* clock_gettime under -std=c11 */
#endif
#include "sp.h"

#include <errno.h>
#include <math.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ---- small things ------------------------------------------------------- */

/* The core's clock (report.h: TIME): the system's, or a test's. */
static ReportClock g_clock;

void sp_test_set_clock(int fixed, double wall, double mono) {
    g_clock.fixed = fixed;
    g_clock.wall = wall;
    g_clock.mono = mono;
}

double sp__wall(void) { return report_wall(&g_clock); }
double sp__mono(void) { return report_mono(&g_clock); }

/* The call failed: the status and the reason. Returns -1, for the
 * functions inside the core; a public function returns report->status. */
static int fail(Report *report, int status, const char *fmt, ...) {
    if (!report) return -1;
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(report->error, sizeof report->error, fmt, ap);
    va_end(ap);
    report->status = status;
    return -1;
}

/* The end of a public function that makes something: what was made goes
 * out, and a failure with no reason of its own is memory. */
#define MADE(report, out, value, what) do { \
        *(out) = (value); \
        if (!*(out) && !(report)->status) \
            fail((report), REPORT_NO_MEMORY, "%s: memory is out", (what)); \
        return (report)->status; \
    } while (0)

/* The start of a public function that takes a Report. */
#define BEGIN(report) do { \
        if (!report_begin(report)) return REPORT_INVALID; \
    } while (0)

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

static SpTicket *ticket_new(void) { return calloc(1, sizeof(SpTicket)); }

int sp_ticket_new(SpTicket **out, Report *report) {
    BEGIN(report);
    if (!out) { fail(report, REPORT_INVALID, "ticket: nowhere to put it"); return report->status; }
    MADE(report, out, ticket_new(), "ticket");
}

void sp_ticket_free(SpTicket *t) {
    if (!t) return;
    for (int i = 0; i < SP_T_FIELDS; i++) free(t->m[i].text);
    for (int32_t i = 0; i < t->n_files; i++) free(t->files[i].checkpoint);
    free(t->files);
    free(t);
}

void sp_ticket_clear(SpTicket *t, int field) {
    if (!t || !field_ok(field)) return;
    free(t->m[field].text);
    memset(&t->m[field], 0, sizeof t->m[field]);
}

static int want(const SpTicket *t, int field, int kind, const char *what,
                Report *report) {
    if (!t) { fail(report, REPORT_INVALID, "ticket: no ticket"); return -1; }
    if (!field_ok(field)) {
        fail(report, REPORT_INVALID, "ticket: no member number %d", field);
        return -1;
    }
    int is = FIELD[field].kind;
    if (is == kind || (kind == SP_INTEGER && is == SP_NUMBER)) return 0;
    fail(report, REPORT_REFUSED, "ticket: %s is %s, and was given %s", FIELD[field].name,
         is == SP_TEXT ? "text" : is == SP_INTEGER ? "an integer" : "a number",
         what);
    return -1;
}

int sp_ticket_set_text(SpTicket *t, int field, const char *text,
                       Report *report) {
    BEGIN(report);
    if (want(t, field, SP_TEXT, "text", report)) return report->status;
    if (!text) { sp_ticket_clear(t, field); return 0; }
    size_t n = strlen(text);
    if (!is_utf8((const unsigned char *)text, n)) {
        fail(report, REPORT_REFUSED, "ticket: %s is not UTF-8 text", FIELD[field].name);
        return report->status;
    }
    char *copy = copy_text(text, n);
    if (!copy) { fail(report, REPORT_NO_MEMORY, "ticket: out of memory"); return report->status; }
    free(t->m[field].text);
    t->m[field].text = copy;
    t->m[field].known = 1;
    return 0;
}

int sp_ticket_set_integer(SpTicket *t, int field, int64_t value,
                          Report *report) {
    BEGIN(report);
    if (want(t, field, SP_INTEGER, "an integer", report)) return report->status;
    t->m[field].known = 1;
    t->m[field].integer = value;
    t->m[field].number = (double)value;
    return 0;
}

int sp_ticket_set_number(SpTicket *t, int field, double value,
                         Report *report) {
    BEGIN(report);
    if (want(t, field, SP_NUMBER, "a number", report)) return report->status;
    if (FIELD[field].kind != SP_NUMBER) {
        fail(report, REPORT_REFUSED, "ticket: %s is an integer, and was given a number",
             FIELD[field].name);
        return report->status;
    }
    if (!isfinite(value)) {
        fail(report, REPORT_REFUSED, "ticket: %s is not a finite number",
             FIELD[field].name);
        return report->status;
    }
    t->m[field].known = 1;
    t->m[field].number = value;
    return 0;
}

static int ticket_add_file(SpTicket *t, const char *checkpoint,
                           const int64_t *first_row,
                           const int64_t *final_rows, const int64_t *rows,
                           Report *report) {
    if (!t || !checkpoint) {
        fail(report, REPORT_REFUSED, "ticket: a file needs its checkpoint path");
        return -1;
    }
    size_t n = strlen(checkpoint);
    if (!is_utf8((const unsigned char *)checkpoint, n)) {
        fail(report, REPORT_REFUSED, "ticket: a file's checkpoint path is not UTF-8 text");
        return -1;
    }
    if (t->n_files == t->cap_files) {
        int32_t cap = t->cap_files ? t->cap_files * 2 : 4;
        TicketFile *p = realloc(t->files, (size_t)cap * sizeof *p);
        if (!p) { fail(report, REPORT_NO_MEMORY, "ticket: out of memory"); return -1; }
        t->files = p;
        t->cap_files = cap;
    }
    TicketFile f = {0};
    f.checkpoint = copy_text(checkpoint, n);
    if (!f.checkpoint) { fail(report, REPORT_NO_MEMORY, "ticket: out of memory"); return -1; }
    if (first_row) { f.first_row = *first_row; f.has_first_row = 1; }
    if (final_rows) { f.final_rows = *final_rows; f.has_final_rows = 1; }
    if (rows) { f.rows = *rows; f.has_rows = 1; }
    t->files[t->n_files++] = f;
    return 0;
}

int sp_ticket_add_file(SpTicket *t, const char *checkpoint,
                       const int64_t *first_row, const int64_t *final_rows,
                       const int64_t *rows, Report *report) {
    BEGIN(report);
    ticket_add_file(t, checkpoint, first_row, final_rows, rows, report);
    return report->status;
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

static char *ticket_write(const SpTicket *t, size_t *out_len) {
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
    Report *report;           /* NULL: the reason is not wanted */
    int failed;
} Reader;

static void refuse(Reader *r, const char *fmt, ...) {
    if (r->failed) return;
    r->failed = 1;
    if (!r->report) return;
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(r->report->error, sizeof r->report->error, fmt, ap);
    va_end(ap);
    r->report->status = REPORT_REFUSED;
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
        int rc = ticket_add_file(t, f.checkpoint,
                                 f.has_first_row ? &f.first_row : NULL,
                                 f.has_final_rows ? &f.final_rows : NULL,
                                 f.has_rows ? &f.rows : NULL, NULL);
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

int sp_ticket_write(const SpTicket *t, char **out, size_t *out_len,
                    Report *report) {
    BEGIN(report);
    if (!t || !out) { fail(report, REPORT_INVALID, "ticket: no ticket to write"); return report->status; }
    MADE(report, out, ticket_write(t, out_len), "ticket");
}

static SpTicket *ticket_read(const char *text, size_t len, Report *report) {
    Reader r = {text, text ? text + len : NULL, report, 0};
    SpTicket *t = ticket_new();
    if (!t) { fail(report, REPORT_NO_MEMORY, "ticket: out of memory"); return NULL; }
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

int sp_ticket_read(const char *text, size_t len, SpTicket **out,
                   Report *report) {
    BEGIN(report);
    if (!out) { fail(report, REPORT_INVALID, "ticket: nowhere to put it"); return report->status; }
    MADE(report, out, ticket_read(text, len, report), "ticket");
}

/* ---- the stale rule ----------------------------------------------------- */

double sp_ticket_stale_after(const SpTicket *t, double if_absent) {
    double promised;
    if (sp_ticket_number(t, SP_T_STALE_AFTER, &promised)) return promised;
    return if_absent;
}

static int ticket_is_stale(const SpTicket *t, double now, double if_absent) {
    double ts;
    if (!sp_ticket_number(t, SP_T_TS, &ts)) return 1;
    return now - ts >= sp_ticket_stale_after(t, if_absent);
}

int sp_ticket_is_stale(const SpTicket *t, double if_absent) {
    return ticket_is_stale(t, sp__wall(), if_absent);
}

/* ---- the ownership decision --------------------------------------------- */

static char *lock_record(int64_t pid, const char *host, int64_t port,
                         const char *run, size_t *out_len) {
    Text b = {0};
    text_str(&b, "{\"pid\":");
    text_integer(&b, pid);
    text_str(&b, ",\"host\":");
    if (host) text_json_string(&b, host); else text_str(&b, "null");
    text_str(&b, ",\"port\":");
    text_integer(&b, port);
    text_str(&b, ",\"run\":");
    if (run) text_json_string(&b, run); else text_str(&b, "null");
    text_str(&b, "}");
    if (b.oom || !b.p) { free(b.p); return NULL; }
    if (out_len) *out_len = b.len;
    return b.p;
}

/* The pid and run of a lock record. Whatever the text is, this returns;
 * what it could not read stays unknown (*has_pid 0, *run NULL). The
 * caller frees *run. */
static void lock_holder(const char *text, size_t len, int64_t *pid,
                        int *has_pid, char **run) {
    Reader r = {text, text ? text + len : NULL, NULL, 0};
    *has_pid = 0;
    *run = NULL;
    if (!text || !take(&r, '{') || take(&r, '}')) return;
    do {
        char *key = read_string(&r);
        if (!key) return;
        if (!take(&r, ':')) { free(key); return; }
        int c = peek(&r);
        if (strcmp(key, "pid") == 0 && c != '"' && c != '{' && c != '['
                && c != 't' && c != 'f' && c != 'n') {
            double number; int64_t integer; int is_integer;
            if (read_number(&r, &number, &integer, &is_integer) && is_integer) {
                *pid = integer;
                *has_pid = 1;
            }
        } else if (strcmp(key, "run") == 0 && c == '"') {
            free(*run);
            *run = read_string(&r);
        } else {
            skip_value(&r, 0);
        }
        free(key);
    } while (!r.failed && take(&r, ','));
}

int sp_lock_record(int64_t pid, const char *host, int64_t port,
                   const char *run, char **out, size_t *out_len,
                   Report *report) {
    BEGIN(report);
    if (!out) { fail(report, REPORT_INVALID, "lock record: nowhere to put it"); return report->status; }
    MADE(report, out, lock_record(pid, host, port, run, out_len), "lock record");
}

int sp_lock_refusal(const char *who, const char *lock_path,
                    const char *holder, size_t holder_len, Report *report) {
    BEGIN(report);
    int64_t pid = 0;
    int has_pid;
    char *run;
    char pid_text[32] = "?";
    lock_holder(holder, holder_len, &pid, &has_pid, &run);
    if (has_pid) snprintf(pid_text, sizeof pid_text, "%lld", (long long)pid);
    fail(report, REPORT_REFUSED,
         "%s: another process on this server writes this stream: pid %s, "
         "run %s holds %s. One writer per stream: stop that process first.",
         who ? who : "?", pid_text, run ? run : "?",
         lock_path ? lock_path : "?");
    free(run);
    return report->status;
}

int sp_ownership(const char *who, const char *name, const char *store,
                 const SpTicket *ticket, const char *host,
                 double if_absent, int *decision, Report *report) {
    BEGIN(report);
    double now = sp__wall();
    if (decision) *decision = SP_OWN;
    if (!ticket) return REPORT_OK;
    const char *theirs = sp_ticket_text(ticket, SP_T_HOST);
    if (theirs && host && strcmp(theirs, host) == 0) return REPORT_OK;

    char port[32] = "?";
    int64_t p;
    if (sp_ticket_integer(ticket, SP_T_PORT, &p))
        snprintf(port, sizeof port, "%lld", (long long)p);
    const char *run = sp_ticket_text(ticket, SP_T_RUN);
    double ts = 0;
    sp_ticket_number(ticket, SP_T_TS, &ts);       /* no ts: stale */
    double age = now - ts;
    double stale = sp_ticket_stale_after(ticket, if_absent);

    if (!ticket_is_stale(ticket, now, if_absent)) {
        if (decision) *decision = SP_REFUSED;
        fail(report, REPORT_REFUSED,
             "%s: the server at %s:%s writes this stream: its ticket in %s "
             "is %.0fs old (run %s; a ticket is stale after %.0fs). One "
             "writer per stream: stop it there, or wait until its ticket "
             "is stale.",
             who ? who : "?", theirs ? theirs : "?", port,
             store ? store : "?", age, run ? run : "?", stale);
        return report->status;
    }
    if (decision) *decision = SP_TAKE_OVER;
    report_line(report, &g_clock, REPORT_WARNING,
         "[%s] taking over from %s:%s: its ticket in %s is %.0fs old "
         "(run %s); stale after %.0fs",
         name ? name : "?", theirs ? theirs : "?", port,
         store ? store : "?", age, run ? run : "?", stale);
    return REPORT_OK;
}

int sp_ownership_unread(const char *name, const char *store,
                        const char *why, Report *report) {
    BEGIN(report);
    report_line(report, &g_clock, REPORT_WARNING,
         "[%s] ownership: the ticket in %s could not be read (%s); "
         "starting on this server's lock alone -- another server writing "
         "this stream would not be seen",
         name ? name : "?", store ? store : "?", why ? why : "?");
    return REPORT_OK;
}

/* ---- the write plan ----------------------------------------------------- */

struct SpWrites {
    int on;
    int64_t cursor, planned, lost;
    int64_t row_group_rows;
    int has_group_seconds, has_checkpoint_seconds;
    double group_seconds, checkpoint_seconds;
    int has_open, has_planned_at;
    double open_at;       /* when the pending rows began */
    double planned_at;    /* when the last write was planned */
};

int sp_writes_new(SpWrites **out, Report *report) {
    BEGIN(report);
    if (!out) { fail(report, REPORT_INVALID, "write plan: nowhere to put it"); return report->status; }
    SpWrites *w = calloc(1, sizeof(SpWrites));
    if (w) w->row_group_rows = 1;
    MADE(report, out, w, "write plan");
}

void sp_writes_free(SpWrites *w) { free(w); }

int sp_writes_set_options(SpWrites *w, int64_t row_group_rows,
                          const double *row_group_seconds,
                          const double *checkpoint_seconds,
                          Report *report) {
    BEGIN(report);
    if (!w) { fail(report, REPORT_INVALID, "write plan: no plan"); return report->status; }
    if (row_group_rows < 1) {
        fail(report, REPORT_INVALID, "write plan: row_group_rows is %lld; it must be "
             "1 or more", (long long)row_group_rows);
        return report->status;
    }
    if (row_group_seconds && !(isfinite(*row_group_seconds)
                               && *row_group_seconds >= 0)) {
        fail(report, REPORT_INVALID, "write plan: row_group_seconds must be 0 or more");
        return report->status;
    }
    if (checkpoint_seconds && !(isfinite(*checkpoint_seconds)
                                && *checkpoint_seconds >= 0)) {
        fail(report, REPORT_INVALID, "write plan: checkpoint_seconds must be 0 or more");
        return report->status;
    }
    w->row_group_rows = row_group_rows;
    w->has_group_seconds = row_group_seconds != NULL;
    w->group_seconds = row_group_seconds ? *row_group_seconds : 0;
    w->has_checkpoint_seconds = checkpoint_seconds != NULL;
    w->checkpoint_seconds = checkpoint_seconds ? *checkpoint_seconds : 0;
    w->has_planned_at = 0;
    return 0;
}

void sp_writes_start(SpWrites *w, int64_t committed) {
    if (!w || w->on) return;
    w->on = 1;
    w->cursor = w->planned = committed;
    w->has_open = 0;
}

void sp_writes_stop(SpWrites *w) {
    if (w) w->on = 0;
}

static int writes_due(const SpWrites *w, int64_t committed, double now) {
    if (!w || !w->on || committed <= w->cursor) return SP_W_NOTHING;
    /* the subtraction cannot overflow: both are row counts, 0 or more */
    if (committed - w->cursor >= w->row_group_rows) return SP_W_ROW_GROUP;
    if (w->has_group_seconds && w->has_open
            && now - w->open_at >= w->group_seconds)
        return SP_W_ROW_GROUP;
    if (w->has_checkpoint_seconds && committed > w->planned) {
        if (w->has_planned_at) {
            if (now - w->planned_at >= w->checkpoint_seconds)
                return SP_W_CHECKPOINT;
        } else if (w->has_open) {
            if (now - w->open_at >= w->checkpoint_seconds)
                return SP_W_CHECKPOINT;
        }
    }
    return SP_W_NOTHING;
}

int sp_writes_due(const SpWrites *w, int64_t committed) {
    return writes_due(w, committed, sp__mono());
}

int sp_writes_commit(SpWrites *w, int64_t committed, int *begins) {
    if (begins) *begins = 0;
    if (!w) return SP_W_NOTHING;
    double now = sp__mono();
    int due = writes_due(w, committed, now);
    if (w->on && committed > w->cursor && !w->has_open) {
        w->has_open = 1;
        w->open_at = now;
        if (begins) *begins = 1;
    }
    return due;
}

void sp_writes_plan(SpWrites *w, int force_row_group, int64_t lo,
                    int64_t hi, SpWrite *out) {
    SpWrite none = {SP_W_NOTHING, 0, 0, 0};
    if (out) *out = none;
    if (!w || !out || !w->on || hi <= w->cursor) return;
    double now = sp__mono();
    int kind = force_row_group ? SP_W_ROW_GROUP : writes_due(w, hi, now);
    if (kind == SP_W_NOTHING) return;
    if (lo > w->cursor) {
        if (kind == SP_W_CHECKPOINT) return;   /* the next row group counts them */
        if (lo > hi) lo = hi;
        out->lost = lo - w->cursor;
        w->lost += out->lost;
        w->cursor = lo;
        if (w->planned < lo) w->planned = lo;
        if (hi <= w->cursor) return;
    }
    out->kind = kind;
    out->lo = w->cursor;
    out->hi = hi;
    if (kind == SP_W_ROW_GROUP) {
        w->cursor = hi;
        w->has_open = 0;
    }
    if (w->planned < hi) w->planned = hi;
    w->has_planned_at = 1;
    w->planned_at = now;
}

int sp_writes_on(const SpWrites *w) { return w ? w->on : 0; }
int64_t sp_writes_cursor(const SpWrites *w) { return w ? w->cursor : 0; }
int64_t sp_writes_planned(const SpWrites *w) { return w ? w->planned : 0; }
int64_t sp_writes_lost(const SpWrites *w) { return w ? w->lost : 0; }

/* ---- what the store holds ----------------------------------------------- */

typedef struct {
    int64_t version, final_rows, rows;
    char *checkpoint;
} StoreVersion;

typedef struct {
    char *name;
    int has_first;
    int64_t first_row;
    char *in_store;             /* its newest checkpoint version in the store */
    StoreVersion *versions;     /* what the versions written cover */
    int32_t n_versions;
    int awaiting, listed, superseded;
    double superseded_at;
} StoreFile;

struct SpStore {
    double keep_seconds;
    StoreFile *files;
    int32_t n_files, cap_files;
    int64_t stored_row;
    int has_stored_at;
    double stored_at;
    char *catalog;            /* the catalog version the ticket names */
};

/* A store call that has no more to say than this. */
static int store_fail(Report *report) {
    fail(report, REPORT_INVALID, "store: no store or no name was given, or "
         "memory is out");
    return report->status;
}

int sp_store_new(double keep_seconds, SpStore **out, Report *report) {
    BEGIN(report);
    if (!out || !(isfinite(keep_seconds) && keep_seconds >= 0)) {
        fail(report, REPORT_INVALID, "store: keep_seconds must be 0 or more "
             "and finite");
        return report->status;
    }
    SpStore *s = calloc(1, sizeof(SpStore));
    if (s) {
        s->keep_seconds = keep_seconds;
        s->stored_row = -1;
    }
    MADE(report, out, s, "store");
}

int sp_store_set_keep_seconds(SpStore *s, double keep_seconds,
                              Report *report) {
    BEGIN(report);
    if (!s || !(isfinite(keep_seconds) && keep_seconds >= 0)) {
        fail(report, REPORT_INVALID, "store: keep_seconds must be 0 or more "
             "and finite");
        return report->status;
    }
    s->keep_seconds = keep_seconds;
    return 0;
}

double sp_store_keep_seconds(const SpStore *s) {
    return s ? s->keep_seconds : 0;
}

static void store_file_clear(StoreFile *f) {
    for (int32_t i = 0; i < f->n_versions; i++) free(f->versions[i].checkpoint);
    free(f->versions);
    free(f->in_store);
    free(f->name);
}

void sp_store_free(SpStore *s) {
    if (!s) return;
    for (int32_t i = 0; i < s->n_files; i++) store_file_clear(&s->files[i]);
    free(s->files);
    free(s->catalog);
    free(s);
}

int sp_store_catalog(SpStore *s, const char *version, Report *report) {
    BEGIN(report);
    if (!s) return store_fail(report);
    char *mine = version ? copy_text(version, strlen(version)) : NULL;
    if (version && !mine) return store_fail(report);
    free(s->catalog);
    s->catalog = mine;
    return 0;
}

static StoreFile *store_find(const SpStore *s, const char *name) {
    if (!s || !name) return NULL;
    for (int32_t i = 0; i < s->n_files; i++)
        if (strcmp(s->files[i].name, name) == 0) return &s->files[i];
    return NULL;
}

/* The file's record, made when there is none. NULL when memory is out. */
static StoreFile *store_get(SpStore *s, const char *name) {
    StoreFile *f = store_find(s, name);
    if (f || !s || !name) return f;
    if (s->n_files == s->cap_files) {
        int32_t cap = s->cap_files ? s->cap_files * 2 : 4;
        StoreFile *grown = realloc(s->files, (size_t)cap * sizeof(StoreFile));
        if (!grown) return NULL;
        s->files = grown;
        s->cap_files = cap;
    }
    char *copy = copy_text(name, strlen(name));
    if (!copy) return NULL;
    f = &s->files[s->n_files++];
    memset(f, 0, sizeof *f);
    f->name = copy;
    return f;
}

static void store_remove(SpStore *s, StoreFile *f) {
    store_file_clear(f);
    int32_t i = (int32_t)(f - s->files);
    memmove(f, f + 1, (size_t)(s->n_files - i - 1) * sizeof(StoreFile));
    s->n_files--;
}

/* A record nothing needs any more: not in the ticket, not awaiting its
 * catalog, no checkpoints to delete. */
static void store_settle(SpStore *s, StoreFile *f) {
    if (!f->in_store && !f->awaiting && !f->superseded) store_remove(s, f);
}

int sp_store_begins(SpStore *s, const char *file, int64_t first_row,
                    Report *report) {
    BEGIN(report);
    StoreFile *f = store_get(s, file);
    if (!f) return store_fail(report);
    f->has_first = 1;
    f->first_row = first_row;
    return 0;
}

int sp_store_covers(SpStore *s, const char *file, int64_t version,
                    const char *checkpoint, int64_t final_rows, int64_t rows,
                    Report *report) {
    BEGIN(report);
    if (!checkpoint) return store_fail(report);
    StoreFile *f = store_get(s, file);
    if (!f) return store_fail(report);
    StoreVersion *v = NULL;
    for (int32_t i = 0; i < f->n_versions; i++)
        if (f->versions[i].version == version) v = &f->versions[i];
    if (!v) {
        StoreVersion *grown = realloc(f->versions,
                                      (size_t)(f->n_versions + 1) * sizeof *grown);
        if (!grown) return store_fail(report);
        f->versions = grown;
        v = &f->versions[f->n_versions];
        v->checkpoint = NULL;
        v->version = version;
        f->n_versions++;
    }
    char *copy = copy_text(checkpoint, strlen(checkpoint));
    if (!copy) {
        if (!v->checkpoint) f->n_versions--;      /* the new one: take it back */
        return store_fail(report);
    }
    free(v->checkpoint);
    v->checkpoint = copy;
    v->final_rows = final_rows;
    v->rows = rows;
    return 0;
}

int sp_store_stored(SpStore *s, const char *file, int64_t version,
                    const char *checkpoint, int64_t rows, Report *report) {
    BEGIN(report);
    if (!checkpoint) return store_fail(report);
    StoreFile *f = store_get(s, file);
    if (!f) return store_fail(report);
    char *copy = copy_text(checkpoint, strlen(checkpoint));
    if (!copy) return store_fail(report);
    free(f->in_store);
    f->in_store = copy;
    /* the versions before it are named by no later ticket */
    int32_t kept = 0;
    for (int32_t i = 0; i < f->n_versions; i++) {
        if (f->versions[i].version < version) free(f->versions[i].checkpoint);
        else f->versions[kept++] = f->versions[i];
    }
    f->n_versions = kept;
    if (f->has_first && rows > 0 && f->first_row <= INT64_MAX - rows
            && f->first_row + rows - 1 > s->stored_row) {
        s->stored_row = f->first_row + rows - 1;
        s->stored_at = sp__wall();
        s->has_stored_at = 1;
    }
    return 0;
}

int sp_store_finished(SpStore *s, const char *file, Report *report) {
    BEGIN(report);
    if (!s || !file) return store_fail(report);
    StoreFile *f = store_find(s, file);
    if (!f) return 0;
    free(f->in_store);
    f->in_store = NULL;
    store_settle(s, f);
    return 0;
}

int sp_store_awaits(SpStore *s, const char *file, Report *report) {
    BEGIN(report);
    StoreFile *f = store_get(s, file);
    if (!f) return store_fail(report);
    f->awaiting = 1;
    return 0;
}

int sp_store_listed(SpStore *s, const char *file) {
    StoreFile *f = store_find(s, file);
    if (!f || !f->awaiting) return 0;
    free(f->in_store);
    f->in_store = NULL;
    f->listed = 1;
    return 1;
}

int sp_store_dropped(SpStore *s) {
    int n = 0;
    if (!s) return 0;
    double now = sp__mono();
    for (int32_t i = 0; i < s->n_files; i++) {
        StoreFile *f = &s->files[i];
        if (!f->awaiting || !f->listed) continue;
        f->awaiting = f->listed = 0;
        f->superseded = 1;
        f->superseded_at = now;
        n++;
    }
    return n;
}

const char *sp_store_due(const SpStore *s, int32_t index) {
    if (!s || index < 0) return NULL;
    double now = sp__mono();
    for (int32_t i = 0; i < s->n_files; i++) {
        const StoreFile *f = &s->files[i];
        if (f->superseded && now - f->superseded_at >= s->keep_seconds
                && index-- == 0)
            return f->name;
    }
    return NULL;
}

int sp_store_discarded(SpStore *s, const char *file, Report *report) {
    BEGIN(report);
    if (!s || !file) return store_fail(report);
    StoreFile *f = store_find(s, file);
    if (!f) return 0;
    f->superseded = 0;
    store_settle(s, f);
    return 0;
}

int64_t sp_store_stored_row(const SpStore *s) { return s ? s->stored_row : -1; }

int sp_store_stored_at(const SpStore *s, double *wall) {
    if (!s || !s->has_stored_at) return 0;
    if (wall) *wall = s->stored_at;
    return 1;
}

int32_t sp_store_files(const SpStore *s) { return s ? s->n_files : 0; }

static int by_name(const void *a, const void *b) {
    const StoreFile *x = *(const StoreFile *const *)a;
    const StoreFile *y = *(const StoreFile *const *)b;
    return strcmp(x->name, y->name);
}

int sp_store_fill(const SpStore *s, SpTicket *t, const char *where,
                  Report *report) {
    BEGIN(report);
    if (!s || !t) { fail(report, REPORT_INVALID, "store: nothing to fill"); return report->status; }
    if (!where) where = "";
    if (sp_ticket_set_integer(t, SP_T_STORED_ROW, s->stored_row, report)
            || sp_ticket_set_number(t, SP_T_KEEP_SECONDS, s->keep_seconds,
                                    report))
        return report->status;
    if (s->has_stored_at) {
        if (sp_ticket_set_number(t, SP_T_STORED_AT, s->stored_at, report))
            return report->status;
    } else {
        sp_ticket_clear(t, SP_T_STORED_AT);
    }
    if (s->catalog) {
        if (sp_ticket_set_text(t, SP_T_CATALOG_ETAG, s->catalog, report))
            return report->status;
    } else {
        sp_ticket_clear(t, SP_T_CATALOG_ETAG);
    }
    for (int32_t i = 0; i < t->n_files; i++) free(t->files[i].checkpoint);
    t->n_files = 0;
    if (s->n_files == 0) return 0;
    const StoreFile **order = malloc((size_t)s->n_files * sizeof *order);
    if (!order) { fail(report, REPORT_NO_MEMORY, "store: out of memory"); return report->status; }
    int32_t n = 0;
    for (int32_t i = 0; i < s->n_files; i++)
        if (s->files[i].in_store) order[n++] = &s->files[i];
    qsort(order, (size_t)n, sizeof *order, by_name);
    int rc = 0;
    for (int32_t i = 0; i < n && rc == 0; i++) {
        const StoreFile *f = order[i];
        const StoreVersion *v = NULL;
        for (int32_t k = 0; k < f->n_versions; k++)
            if (strcmp(f->versions[k].checkpoint, f->in_store) == 0)
                v = &f->versions[k];
        Text path = {0};
        text_str(&path, where);
        text_str(&path, f->in_store);
        if (path.oom || !path.p) {
            free(path.p);
            fail(report, REPORT_NO_MEMORY, "store: out of memory");
            rc = -1;
            break;
        }
        rc = sp_ticket_add_file(t, path.p, f->has_first ? &f->first_row : NULL,
                                v ? &v->final_rows : NULL, v ? &v->rows : NULL,
                                report);
        free(path.p);
    }
    free(order);
    return rc;
}

/* ---- the reader --------------------------------------------------------- */

#define READER_TEXT 640
#define BACKOFF_FIRST 0.5
#define BACKOFF_MOST 10.0

struct SpReader {
    char *who, *name, *store, *folder;
    int direct;
    int state;
    int folder_known;         /* the folder exists, or the reader waited */
    int asked_folder;         /* SP_DO_LIST is out */
    int has_ring;             /* a ring of rows of `run` is in hand */
    int local;                /* reading a local stream */
    char *run;
    double backoff;
    int waiting;              /* a wait for a writer is on */
    int warned;
    double waiting_since;
    int unreadable;           /* the store's error is logged */
    char reason[READER_TEXT];
    Report *report;           /* of the call in hand: where its lines go */
};

static const char *const READER_STATE[] = {
    "no-ticket", "store-unreachable", "waiting", "connecting", "streaming",
    "reconnecting", "closed",
};

const char *sp_reader_state_name(int state) {
    if (state < 0 || state > SP_R_CLOSED) return NULL;
    return READER_STATE[state];
}

static void reader_say(SpReader *r, int level, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    report_vline(r->report, &g_clock, level, fmt, ap);
    va_end(ap);
}

static void reader_reason(SpReader *r, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(r->reason, sizeof r->reason, fmt, ap);
    va_end(ap);
}

static const char *reader_store(const SpReader *r) {
    return r->store ? r->store : "(a direct ticket)";
}

static SpReader *reader_new(const char *who, const char *name,
                            const char *store, int direct) {
    if (!name) return NULL;
    SpReader *r = calloc(1, sizeof(SpReader));
    if (!r) return NULL;
    if (!who) who = "connect";
    r->who = copy_text(who, strlen(who));
    r->name = copy_text(name, strlen(name));
    r->store = store ? copy_text(store, strlen(store)) : NULL;
    const char *slash = strrchr(name, '/');
    r->folder = copy_text(name, slash ? (size_t)(slash - name) : 0);
    if (!r->who || !r->name || !r->folder || (store && !r->store)) {
        sp_reader_free(r);
        return NULL;
    }
    r->direct = direct ? 1 : 0;
    r->state = direct ? SP_R_CONNECTING : SP_R_NO_TICKET;
    r->backoff = BACKOFF_FIRST;
    /* a name with no folder is in the store itself, which exists */
    r->folder_known = (r->folder[0] == 0) || r->direct;
    return r;
}

int sp_reader_new(const char *who, const char *name, const char *store,
                  int direct, SpReader **out, Report *report) {
    BEGIN(report);
    if (!out || !name) {
        fail(report, REPORT_INVALID, "reader: no name of a stream");
        return report->status;
    }
    MADE(report, out, reader_new(who, name, store, direct), "reader");
}

void sp_reader_free(SpReader *r) {
    if (!r) return;
    free(r->who);
    free(r->name);
    free(r->store);
    free(r->folder);
    free(r->run);
    free(r);
}

const char *sp_reader_folder(const SpReader *r) { return r ? r->folder : ""; }
int sp_reader_state(const SpReader *r) { return r ? r->state : SP_R_CLOSED; }
const char *sp_reader_reason(const SpReader *r) {
    return (r && r->state != SP_R_STREAMING) ? r->reason : "";
}
const char *sp_reader_run(const SpReader *r) { return r ? r->run : NULL; }

static void step_set(SpReader *r, SpStep *step, int action, int state,
                     double wait) {
    r->state = state;
    if (!step) return;
    step->action = action;
    step->state = state;
    step->wait_seconds = wait;
}

/* A step of a wait for a writer: the line at its start, the warning
 * when it has lasted. `quiet`: the start has its own line already. */
static void reader_wait(SpReader *r, SpStep *step, int state, double mono,
                        int quiet) {
    r->folder_known = 1;
    if (!r->waiting) {
        r->waiting = 1;
        r->warned = 0;
        r->waiting_since = mono;
        if (!quiet)
            reader_say(r, REPORT_INFO, "[%s] %s: waiting for a writer",
                       r->name, r->reason);
    } else if (!r->warned
               && mono - r->waiting_since >= SP_READER_WARN_SECONDS) {
        r->warned = 1;
        reader_say(r, REPORT_WARNING,
                   "[%s] %s has waited %.0f s for a writer: %s. A reader "
                   "waits for any name whose folder exists; if no writer "
                   "is to come, check the name.",
                   r->name, r->who, mono - r->waiting_since, r->reason);
    }
    step_set(r, step, SP_DO_WAIT, state, SP_READER_POLL_SECONDS);
}

static void reader_drop_ring(SpReader *r, SpStep *step) {
    if (r->has_ring && step) step->fresh_ring = 1;
    r->has_ring = 0;
}

static void reader_poll(SpReader *r, int found, const SpTicket *ticket,
                        const char *why, double now, double mono,
                        double if_absent, SpStep *step) {
    if (r->state == SP_R_CLOSED) {
        step_set(r, step, SP_DO_WAIT, SP_R_CLOSED, 0);
        return;
    }
    if (found == SP_FOUND_TICKET && !ticket) found = SP_FOUND_NONE;

    if (found == SP_FOUND_UNREADABLE) {
        reader_reason(r, "store %s unreadable: %s", reader_store(r),
                      why ? why : "?");
        if (!r->unreadable) {
            r->unreadable = 1;
            reader_say(r, REPORT_ERROR, "[%s] STORE UNREADABLE: %s: %s",
                       r->name, reader_store(r), why ? why : "?");
        }
        r->asked_folder = 0;
        int known = r->folder_known;      /* the store said nothing */
        reader_wait(r, step, SP_R_STORE_UNREACHABLE, mono, 1);
        r->folder_known = known;
        return;
    }
    if (r->unreadable) {
        r->unreadable = 0;
        reader_say(r, REPORT_INFO, "[%s] store readable again: %s",
                   r->name, reader_store(r));
    }

    if (found == SP_FOUND_LOCAL) {
        reader_drop_ring(r, step);
        free(r->run);
        r->run = NULL;
        r->local = 1;
        r->waiting = 0;
        r->folder_known = 1;
        r->backoff = BACKOFF_FIRST;
        r->reason[0] = 0;
        step_set(r, step, SP_DO_LOCAL, SP_R_STREAMING, 0);
        return;
    }
    r->local = 0;

    if (found == SP_FOUND_NONE) {
        reader_reason(r, "no ticket for %s under %s: no writer is up",
                      r->name, reader_store(r));
        if (!r->folder_known) {
            r->asked_folder = 1;
            step_set(r, step, SP_DO_LIST, SP_R_NO_TICKET, 0);
            return;
        }
        reader_wait(r, step, SP_R_NO_TICKET, mono, 0);
        return;
    }

    /* a ticket */
    const char *host = sp_ticket_text(ticket, SP_T_HOST);
    char port[32] = "?";
    int64_t p;
    if (sp_ticket_integer(ticket, SP_T_PORT, &p))
        snprintf(port, sizeof port, "%lld", (long long)p);
    double ts;
    if (!r->direct && sp_ticket_number(ticket, SP_T_TS, &ts)
            && ticket_is_stale(ticket, now, if_absent)) {
        reader_reason(r, "ticket stale: the writer at %s:%s last wrote it "
                      "%.0f s ago (stale after %.0f s)",
                      host ? host : "?", port, now - ts,
                      sp_ticket_stale_after(ticket, if_absent));
        reader_wait(r, step, SP_R_WAITING, mono, 0);
        return;
    }

    const char *run = sp_ticket_text(ticket, SP_T_RUN);
    if (r->has_ring && r->run && (!run || strcmp(run, r->run) != 0)) {
        reader_say(r, REPORT_ERROR,
                   "[%s] WRITER RESTARTED: the ticket at %s:%s names run %s, "
                   "this reader streamed run %s; starting again on a fresh "
                   "ring. Rows of the earlier run are dropped here (the "
                   "files keep them); make any view over this reader again.",
                   r->name, host ? host : "?", port, run ? run : "?", r->run);
        reader_drop_ring(r, step);
    }
    char *mine = run ? copy_text(run, strlen(run)) : NULL;
    free(r->run);
    r->run = mine;
    r->waiting = 0;
    r->folder_known = 1;
    reader_reason(r, "connecting to %s:%s", host ? host : "?", port);
    step_set(r, step, SP_DO_CONNECT, SP_R_CONNECTING, 0);
}

/* The start of a reader's step: the report takes the step's lines. */
#define READER_STEP(r, step, report) do { \
        BEGIN(report); \
        if (step) memset((step), 0, sizeof *(step)); \
        if (!(r) || !(step)) { \
            fail((report), REPORT_INVALID, "reader: no reader or no step"); \
            return (report)->status; \
        } \
        (r)->report = (report); \
    } while (0)

int sp_reader_poll(SpReader *r, int found, const SpTicket *ticket,
                   const char *why, double if_absent, SpStep *step,
                   Report *report) {
    READER_STEP(r, step, report);
    reader_poll(r, found, ticket, why, sp__wall(), sp__mono(), if_absent,
                step);
    r->report = NULL;
    return REPORT_OK;
}

static void reader_folder_is(SpReader *r, int exists, double mono,
                             SpStep *step) {
    if (r->state == SP_R_CLOSED) {
        step_set(r, step, SP_DO_WAIT, SP_R_CLOSED, 0);
        return;
    }
    r->asked_folder = 0;
    if (!exists && !r->folder_known) {
        reader_reason(r,
            "%s: the folder %s does not exist in the store %s: the store "
            "holds no ticket and no file under %s, and this process has no "
            "stream there. The folder is made the first time a writer of a "
            "stream under %s runs. Start the writer first, or check the "
            "path.",
            r->who, r->folder, reader_store(r), r->folder, r->folder);
        step_set(r, step, SP_DO_FAIL, SP_R_CLOSED, 0);
        return;
    }
    reader_wait(r, step, SP_R_NO_TICKET, mono, 0);
}

int sp_reader_folder_is(SpReader *r, int exists, SpStep *step,
                        Report *report) {
    READER_STEP(r, step, report);
    reader_folder_is(r, exists, sp__mono(), step);
    r->report = NULL;
    return REPORT_OK;
}

void sp_reader_connected(SpReader *r) {
    if (!r || r->state == SP_R_CLOSED) return;
    r->state = SP_R_STREAMING;
    r->has_ring = 1;
    r->waiting = 0;
    r->backoff = BACKOFF_FIRST;
    r->reason[0] = 0;
}

static void reader_lost(SpReader *r, const char *why, SpStep *step) {
    if (r->state == SP_R_CLOSED) {
        step_set(r, step, SP_DO_WAIT, SP_R_CLOSED, 0);
        return;
    }
    if (r->local) {
        r->local = 0;
        reader_reason(r, "the stream %s of this process closed", r->name);
        if (step) step->fresh_ring = 1;       /* the ring was the writer's */
        step_set(r, step, SP_DO_WAIT, SP_R_RECONNECTING, 0);
        return;
    }
    if (why) {
        reader_reason(r, "the connection for %s failed: %s", r->name, why);
        reader_say(r, REPORT_ERROR, "[%s] READER CONNECTION: %s", r->name,
                   why);
    } else {
        reader_reason(r, "the connection for %s ended", r->name);
    }
    double wait = r->backoff;
    r->backoff = wait * 2 > BACKOFF_MOST ? BACKOFF_MOST : wait * 2;
    step_set(r, step, SP_DO_WAIT, SP_R_RECONNECTING, wait);
}

int sp_reader_lost(SpReader *r, const char *why, SpStep *step,
                   Report *report) {
    READER_STEP(r, step, report);
    reader_lost(r, why, step);
    r->report = NULL;
    return REPORT_OK;
}

void sp_reader_close(SpReader *r) {
    if (r) r->state = SP_R_CLOSED;
}

/* ---- the view ----------------------------------------------------------- */

#define VIEW_TEXT 1024

/* One unfinished file the text reads: the checkpoint version named,
 * and the file's last row to read (unknown: the whole file). */
typedef struct {
    char *path;
    int64_t last_row;
    int has_last_row;
} ViewPiece;

/* The checkpoint version the text names for a file, and when a newer
 * one was first seen. */
typedef struct {
    char *file;
    char *path;
    double since;
    int has_since;
} ViewNamed;

typedef struct {
    int live;
    char *etag;
    char *run;
    int64_t seam;
    int use_ring;
    ViewPiece *pieces;
    int32_t n_pieces;
} ViewFacts;

struct SpView {
    char *name, *stream, *memory, *empty;
    int row_ids;              /* the text adds the two row id columns */
    ViewNamed *named;
    int32_t n_named;
    int64_t named_seam;
    int has_named_seam;
    ViewFacts plan;           /* what the next text names */
    ViewFacts built;          /* what the view's text names now */
    int is_built;
    int has_ticket, has_ts;   /* of the ticket last planned from */
    double ts, beat;
    int failed;
    char note[VIEW_TEXT];
    Report *report;           /* of the call in hand: where its lines go */
};

static void facts_clear(ViewFacts *f) {
    free(f->etag);
    free(f->run);
    for (int32_t i = 0; i < f->n_pieces; i++) free(f->pieces[i].path);
    free(f->pieces);
    memset(f, 0, sizeof *f);
}

static int text_same(const char *a, const char *b) {
    if (!a || !b) return a == b;
    return strcmp(a, b) == 0;
}

static int facts_same(const ViewFacts *a, const ViewFacts *b) {
    if (a->live != b->live || a->seam != b->seam
        || a->use_ring != b->use_ring || a->n_pieces != b->n_pieces
        || !text_same(a->etag, b->etag) || !text_same(a->run, b->run))
        return 0;
    for (int32_t i = 0; i < a->n_pieces; i++) {
        const ViewPiece *x = &a->pieces[i], *y = &b->pieces[i];
        if (strcmp(x->path, y->path) != 0
            || x->has_last_row != y->has_last_row
            || (x->has_last_row && x->last_row != y->last_row))
            return 0;
    }
    return 1;
}

static void view_say(SpView *v, int level, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    report_vline(v->report, &g_clock, level, fmt, ap);
    va_end(ap);
}

static SpView *view_new(const char *view, const char *stream,
                        const char *memory, const char *empty) {
    if (!view || !stream) return NULL;
    SpView *v = calloc(1, sizeof(SpView));
    if (!v) return NULL;
    v->name = copy_text(view, strlen(view));
    v->stream = copy_text(stream, strlen(stream));
    v->memory = memory ? copy_text(memory, strlen(memory)) : NULL;
    v->empty = empty ? copy_text(empty, strlen(empty)) : NULL;
    if (!v->name || !v->stream || (memory && !v->memory)
        || (empty && !v->empty)) {
        sp_view_free(v);
        return NULL;
    }
    v->plan.seam = v->built.seam = -1;
    return v;
}

int sp_view_new(const char *view, const char *stream, const char *memory,
                const char *empty, SpView **out, Report *report) {
    BEGIN(report);
    if (!out || !view || !stream) {
        fail(report, REPORT_INVALID, "view: no name of the view or of the "
             "stream");
        return report->status;
    }
    MADE(report, out, view_new(view, stream, memory, empty), "view");
}

static void named_clear(ViewNamed *named, int32_t n) {
    for (int32_t i = 0; i < n; i++) {
        free(named[i].file);
        free(named[i].path);
    }
    free(named);
}

void sp_view_free(SpView *v) {
    if (!v) return;
    free(v->name);
    free(v->stream);
    free(v->memory);
    free(v->empty);
    named_clear(v->named, v->n_named);
    facts_clear(&v->plan);
    facts_clear(&v->built);
    free(v);
}

int64_t sp_view_seam(const SpTicket *t, const int64_t *window, int *use_ring,
                     int *why) {
    int64_t stored = -1;
    int ring = 0, reason = SP_SEAM_AS_WANTED;
    if (t) sp_ticket_integer(t, SP_T_STORED_ROW, &stored);
    int64_t seam = stored;
    if (window) {
        int64_t oldest = window[0], newest = window[1], capacity = window[2];
        if (newest < oldest) {                    /* the ring holds no rows yet */
            ring = 1;
        } else {
            /* the first row above the oldest quarter of a full ring: the
             * writer reuses row r once it has written row r + capacity */
            int64_t above = newest + 1 - capacity
                + (int64_t)((double)capacity * SP_VIEW_RING_MARGIN_SHARE);
            int found = 0;
            int64_t best = 0;
#define SEAM_CANDIDATE(c) do { \
                int64_t c_ = (c); \
                if (c_ <= stored && c_ + 1 >= oldest && c_ + 1 >= above \
                    && (!found || c_ < best)) { found = 1; best = c_; } \
            } while (0)
            SEAM_CANDIDATE(stored);
            for (int32_t i = 0; t && i < t->n_files; i++) {
                const TicketFile *f = &t->files[i];
                if (!f->has_first_row) continue;
                SEAM_CANDIDATE(f->first_row - 1);
                if (f->has_final_rows)
                    SEAM_CANDIDATE(f->first_row + f->final_rows - 1);
            }
#undef SEAM_CANDIDATE
            if (found) {
                seam = best;
                ring = 1;
            } else if (oldest <= stored + 1) {
                ring = 1;
                reason = SP_SEAM_TIGHT;
            } else {
                reason = (newest - oldest + 1 >= capacity) ? SP_SEAM_SHORT
                                                           : SP_SEAM_LATE;
            }
        }
    }
    if (use_ring) *use_ring = ring;
    if (why) *why = reason;
    return seam;
}

/* Say, once for each change, why the ring is not used as wanted.
 * SP_SEAM_LATE is not said: a later ticket ends it. */
static void view_note_ring(SpView *v, int why, int64_t stored,
                           const int64_t *window) {
    char note[VIEW_TEXT];
    note[0] = 0;
    if (why == SP_SEAM_SHORT || why == SP_SEAM_TIGHT) {
        int64_t oldest = window ? window[0] : 0;
        int64_t newest = window ? window[1] : -1;
        int64_t capacity = window ? window[2] : 0;
        int64_t behind = newest - stored;
        int64_t want = (int64_t)((double)behind
                                 / (1 - SP_VIEW_RING_MARGIN_SHARE)) + 1;
        if (why == SP_SEAM_SHORT)
            snprintf(note, sizeof note,
                "the ring holds rows %lld to %lld and the store ends at row "
                "%lld: the ring is too short for the store's lag of %lld "
                "rows, so the view reads the files only and is that far "
                "behind. Connect with MemoryOptions(rows=%lld) or more "
                "(now %lld).",
                (long long)oldest, (long long)newest, (long long)stored,
                (long long)behind, (long long)want, (long long)capacity);
        else
            snprintf(note, sizeof note,
                "the store is %lld rows behind a ring of %lld: a running "
                "query has less than a quarter of the ring to reach memory. "
                "Connect with MemoryOptions(rows=%lld) or more.",
                (long long)behind, (long long)capacity, (long long)want);
    }
    if ((note[0] == 0) != (v->note[0] == 0)) {
        if (note[0])
            view_say(v, REPORT_WARNING, "[%s] view %s: %s", v->stream,
                     v->name, note);
        else
            view_say(v, REPORT_INFO, "[%s] view %s: the ring is in use again",
                     v->stream, v->name);
    }
    memcpy(v->note, note, sizeof note);
}

static int view_plan(SpView *v, const SpTicket *t, const char *const *files,
                     const char *ring_run, const int64_t *window, double now,
                     double mono, double if_absent, double keep_if_absent,
                     SpViewPlan *out) {
    if (out) memset(out, 0, sizeof *out);
    int64_t revision, stored = -1;
    double keep = 0;
    /* A live writer's ticket names the catalog version to read. No
     * ticket (a clean close deletes it), a stale one (a dead writer) or
     * one without a revision (an older writer) vouches for none: the
     * view reads the store's current catalog. */
    int live = t && sp_ticket_integer(t, SP_T_REVISION, &revision)
        && !ticket_is_stale(t, now, if_absent);
    const char *etag = live ? sp_ticket_text(t, SP_T_CATALOG_ETAG) : NULL;
    const char *run = t ? sp_ticket_text(t, SP_T_RUN) : NULL;
    if (t) sp_ticket_integer(t, SP_T_STORED_ROW, &stored);
    v->has_ticket = t != NULL;
    v->has_ts = t && sp_ticket_number(t, SP_T_TS, &v->ts);
    if (!t || !sp_ticket_number(t, SP_T_BEAT_SECONDS, &v->beat)
        || v->beat == 0)
        v->beat = SP_VIEW_POLL_DEFAULT_SECONDS;

    int64_t seam = stored;
    int use_ring = 0, why = SP_SEAM_AS_WANTED;
    if (v->memory && live && run && ring_run && strcmp(run, ring_run) == 0)
        seam = sp_view_seam(t, window, &use_ring, &why);
    view_note_ring(v, why, stored, window);
    if (!t || !sp_ticket_number(t, SP_T_KEEP_SECONDS, &keep) || keep == 0)
        keep = keep_if_absent;
    double repoint = keep * SP_VIEW_REPOINT_SHARE;

    /* The unfinished files the text reads: those that begin at or
     * before the seam, each up to the seam. A file keeps the checkpoint
     * version the text already names until the seam moves or that
     * version has been replaced for `repoint` seconds: every version
     * holds the rows up to the seam it was named for. */
    int32_t n = t ? t->n_files : 0;
    ViewFacts next;
    memset(&next, 0, sizeof next);
    ViewNamed *keeps = n ? calloc((size_t)n, sizeof *keeps) : NULL;
    next.pieces = n ? calloc((size_t)n, sizeof *next.pieces) : NULL;
    int32_t n_keeps = 0;
    int oom = n && (!keeps || !next.pieces);
    int moved = !v->has_named_seam || seam != v->named_seam;
    for (int32_t i = 0; !oom && i < n; i++) {
        const TicketFile *f = &t->files[i];
        if (f->has_first_row && f->first_row > seam)
            continue;                     /* after the seam: the ring has it */
        const char *key = (files && files[i]) ? files[i] : f->checkpoint;
        const char *path = f->checkpoint;
        double since = 0;
        int has_since = 0;
        const ViewNamed *old = NULL;
        for (int32_t k = 0; k < v->n_named; k++)
            if (strcmp(v->named[k].file, key) == 0) old = &v->named[k];
        if (old && !moved && strcmp(old->path, path) != 0) {
            since = old->has_since ? old->since : mono;
            if (mono - since < repoint) {
                path = old->path;         /* still there: keep naming it */
                has_since = 1;
            }
        }
        ViewNamed *kept = NULL;
        for (int32_t k = 0; k < n_keeps; k++)
            if (strcmp(keeps[k].file, key) == 0) kept = &keeps[k];
        char *mine = copy_text(path, strlen(path));
        if (kept) {
            if (mine) { free(kept->path); kept->path = mine; }
        } else {
            kept = &keeps[n_keeps];
            kept->file = copy_text(key, strlen(key));
            kept->path = mine;
            if (kept->file) n_keeps++;
        }
        kept->since = since;
        kept->has_since = has_since;
        ViewPiece *p = &next.pieces[next.n_pieces];
        p->path = copy_text(path, strlen(path));
        p->has_last_row = f->has_first_row;
        p->last_row = f->has_first_row ? seam - f->first_row : 0;
        if (!mine || !kept->file || !p->path) {
            if (!kept->file) free(mine);
            free(p->path);
            p->path = NULL;
            oom = 1;
            break;
        }
        next.n_pieces++;
    }
    next.live = live;
    next.seam = seam;
    next.use_ring = use_ring;
    if (!oom && etag && !(next.etag = copy_text(etag, strlen(etag)))) oom = 1;
    if (!oom && run && !(next.run = copy_text(run, strlen(run)))) oom = 1;
    if (oom) {
        named_clear(keeps, n_keeps);
        facts_clear(&next);
        return -1;
    }
    named_clear(v->named, v->n_named);
    v->named = keeps;
    v->n_named = n_keeps;
    v->named_seam = seam;
    v->has_named_seam = 1;
    facts_clear(&v->plan);
    v->plan = next;
    if (out) {
        /* a view of no live writer is made again at every look: the
         * store's current catalog may be another */
        out->build = !(live && v->is_built && facts_same(&v->plan, &v->built));
        out->catalog = !live ? SP_CATALOG_CURRENT
            : (etag ? SP_CATALOG_EXACT : SP_CATALOG_NONE);
        out->seam = seam;
        out->use_ring = use_ring;
    }
    return 0;
}

int sp_view_plan(SpView *v, const SpTicket *t, const char *const *files,
                 const char *ring_run, const int64_t *window,
                 double if_absent, double keep_if_absent, SpViewPlan *out,
                 Report *report) {
    BEGIN(report);
    if (!v) { fail(report, REPORT_INVALID, "view: no view"); return report->status; }
    v->report = report;
    int rc = view_plan(v, t, files, ring_run, window, sp__wall(), sp__mono(),
                       if_absent, keep_if_absent, out);
    v->report = NULL;
    if (rc != 0)
        fail(report, REPORT_NO_MEMORY, "%s: memory is out", v->name);
    return report->status;
}

void sp_view_set_row_ids(SpView *v, int on) {
    if (v) v->row_ids = on != 0;
}

const char *sp_view_catalog_etag(const SpView *v) {
    return v ? v->plan.etag : NULL;
}

/* SQL text in single quotes: each quote doubled. */
static void text_sql_quoted(Text *b, const char *s) {
    for (; *s; s++) {
        if (*s == '\'') text_put(b, "''", 2);
        else text_put(b, s, 1);
    }
}

/* One of a file's footer keys, read by DuckDB from the file itself. */
static void text_footer_key(Text *b, const char *path, const char *key) {
    text_str(b, "(SELECT decode(value) FROM parquet_kv_metadata('pqc://");
    text_sql_quoted(b, path);
    text_str(b, "') WHERE decode(key) = '");
    text_str(b, key);
    text_str(b, "' LIMIT 1)");
}

static char *view_sql(const SpView *v, const char *catalog, Report *report) {
    if (!v) { fail(report, REPORT_INVALID, "view: no view"); return NULL; }
    const ViewFacts *f = &v->plan;
    Text b = {0};
    int pieces = 0;
#define PIECE() do { \
        text_str(&b, pieces++ ? " UNION ALL BY NAME (" : "("); } while (0)
    text_str(&b, "CREATE OR REPLACE VIEW \"");
    text_str(&b, v->name);
    text_str(&b, "\" AS ");
    if (catalog && v->row_ids) {
        /* Each finished file's run and first row are in the catalog's
         * table sp_file; DuckLake gives each row its file and its row
         * number in the file. A file the table does not know (written
         * before files recorded their run) reads as null in both. */
        PIECE();
        text_str(&b, "SELECT d.* EXCLUDE (filename, file_row_number), "
                     "CAST(f.run AS TIMESTAMPTZ) AS " SP_ROW_ID_RUN ", "
                     "f.first_row + d.file_row_number AS " SP_ROW_ID_ROW
                     " FROM (SELECT *, filename, file_row_number FROM \"");
        text_str(&b, catalog);
        text_str(&b, "\".data) d LEFT JOIN (SELECT df.path, sf.run, "
                     "sf.first_row FROM \"__ducklake_metadata_");
        text_str(&b, catalog);
        text_str(&b, "\".sp_file sf JOIN \"__ducklake_metadata_");
        text_str(&b, catalog);
        text_str(&b, "\".ducklake_data_file df USING (data_file_id)) f "
                     "ON d.filename = f.path)");
    } else if (catalog) {
        PIECE();
        text_str(&b, "SELECT * FROM \"");
        text_str(&b, catalog);
        text_str(&b, "\".data)");
    }
    for (int32_t i = 0; i < f->n_pieces; i++) {
        PIECE();
        if (v->row_ids) {
            /* an unfinished file says its own run and first row (the
             * footer keys sp_run and sp_base_seq) */
            text_str(&b, "SELECT * EXCLUDE (file_row_number), CAST(");
            text_footer_key(&b, f->pieces[i].path, "sp_run");
            text_str(&b, " AS TIMESTAMPTZ) AS " SP_ROW_ID_RUN ", CAST(");
            text_footer_key(&b, f->pieces[i].path, "sp_base_seq");
            text_str(&b, " AS BIGINT) + file_row_number AS " SP_ROW_ID_ROW
                         " FROM read_parquet('pqc://");
            text_sql_quoted(&b, f->pieces[i].path);
            text_str(&b, "', file_row_number=true)");
            if (f->pieces[i].has_last_row) {
                text_str(&b, " WHERE file_row_number <= ");
                text_integer(&b, f->pieces[i].last_row);
            }
            text_str(&b, ")");
        } else if (!f->pieces[i].has_last_row) {
            text_str(&b, "SELECT * FROM read_parquet('pqc://");
            text_sql_quoted(&b, f->pieces[i].path);
            text_str(&b, "'))");
        } else {
            /* the file up to the seam: DuckDB opens no object of the
             * file past that row */
            text_str(&b, "SELECT * EXCLUDE (file_row_number) FROM "
                         "read_parquet('pqc://");
            text_sql_quoted(&b, f->pieces[i].path);
            text_str(&b, "', file_row_number=true) "
                         "WHERE file_row_number <= ");
            text_integer(&b, f->pieces[i].last_row);
            text_str(&b, ")");
        }
    }
    if (f->use_ring && v->memory) {
        /* The view reads the store up to the seam and the ring after
         * it. If the ring begins past the row after the seam, rows are
         * in neither: the query fails, it does not answer short. */
        PIECE();
        /* with row ids the ring's relation has the run's start time in
         * a column of that name already */
        text_str(&b, v->row_ids
                 ? "SELECT * EXCLUDE (_seq, _first), _seq AS " SP_ROW_ID_ROW
                   " FROM \""
                 : "SELECT * EXCLUDE (_seq, _first) FROM \"");
        text_str(&b, v->memory);
        text_str(&b, "\" WHERE _seq > ");
        text_integer(&b, f->seam);
        text_str(&b, " AND CASE WHEN _first > ");
        text_integer(&b, f->seam + 1);
        text_str(&b, " THEN error('");
        text_sql_quoted(&b, v->name);
        text_str(&b, ": the ring does not hold row ");
        text_integer(&b, f->seam + 1);
        text_str(&b, "; it begins at row ' || _first || '. This query read "
                     "the rows up to ");
        text_integer(&b, f->seam);
        text_str(&b, " from the store. Either it reached the ring after the "
                     "stream had written more rows than the ring keeps for "
                     "a running query, or this reader connected a moment "
                     "ago and the store has not reached its first row yet. "
                     "Run it again; for longer queries connect with a "
                     "larger MemoryOptions(rows=...).') ELSE true END)");
    }
    if (!pieces) {
        if (!v->empty) {
            free(b.p);
            fail(report, REPORT_REFUSED, "%s: '%s' has no catalog, no unfinished file "
                 "and no memory yet; nothing to read the columns from",
                 v->name, v->stream);
            return NULL;
        }
        PIECE();
        text_str(&b, v->row_ids
                 ? "SELECT *, CAST(NULL AS TIMESTAMPTZ) AS " SP_ROW_ID_RUN
                   ", CAST(NULL AS BIGINT) AS " SP_ROW_ID_ROW " FROM \""
                 : "SELECT * FROM \"");
        text_str(&b, v->empty);
        text_str(&b, "\")");
    }
#undef PIECE
    if (b.oom) {
        free(b.p);
        fail(report, REPORT_NO_MEMORY, "%s: memory is out", v->name);
        return NULL;
    }
    return b.p;
}

int sp_view_sql(const SpView *v, const char *catalog, char **out,
                Report *report) {
    BEGIN(report);
    if (!out) { fail(report, REPORT_INVALID, "view: nowhere to put the text"); return report->status; }
    MADE(report, out, view_sql(v, catalog, report), "view");
}

static char *view_live_sql(const char *view, const char *memory) {
    if (!view || !memory) return NULL;
    Text b = {0};
    text_str(&b, "CREATE OR REPLACE VIEW \"");
    text_str(&b, view);
    text_str(&b, "\" AS SELECT * EXCLUDE (_seq, _first), _seq AS "
                 SP_ROW_ID_ROW " FROM \"");
    text_str(&b, memory);
    text_str(&b, "\"");
    if (b.oom) { free(b.p); return NULL; }
    return b.p;
}

int sp_view_live_sql(const char *view, const char *memory, char **out,
                     Report *report) {
    BEGIN(report);
    if (!out || !view || !memory) {
        fail(report, REPORT_INVALID, "view: no name of the view or of the "
             "ring's relation");
        return report->status;
    }
    MADE(report, out, view_live_sql(view, memory), "view");
}

void sp_view_built(SpView *v) {
    if (!v) return;
    ViewFacts copy;
    memset(&copy, 0, sizeof copy);
    const ViewFacts *f = &v->plan;
    copy.live = f->live;
    copy.seam = f->seam;
    copy.use_ring = f->use_ring;
    int oom = 0;
    if (f->etag && !(copy.etag = copy_text(f->etag, strlen(f->etag)))) oom = 1;
    if (f->run && !(copy.run = copy_text(f->run, strlen(f->run)))) oom = 1;
    if (f->n_pieces) {
        copy.pieces = calloc((size_t)f->n_pieces, sizeof *copy.pieces);
        if (!copy.pieces) oom = 1;
    }
    for (int32_t i = 0; !oom && i < f->n_pieces; i++) {
        copy.pieces[i] = f->pieces[i];
        copy.pieces[i].path = copy_text(f->pieces[i].path,
                                        strlen(f->pieces[i].path));
        if (!copy.pieces[i].path) oom = 1;
        else copy.n_pieces++;
    }
    facts_clear(&v->built);
    if (oom) {                            /* the next look builds again */
        facts_clear(&copy);
        v->is_built = 0;
        return;
    }
    v->built = copy;
    v->is_built = 1;
}

int64_t sp_view_seam_row(const SpView *v) {
    return (v && v->is_built) ? v->built.seam : -1;
}

const char *sp_view_ring_note(const SpView *v) { return v ? v->note : ""; }

double sp_view_wait_seconds(const SpView *v) {
    /* no live writer: nothing says when the store changes next */
    if (!v || !v->has_ticket || !v->plan.live)
        return SP_VIEW_POLL_DEFAULT_SECONDS;
    double now = sp__wall();
    double due = (v->has_ts ? v->ts : 0) + v->beat
        + SP_VIEW_POLL_MARGIN_SECONDS - now;
    if (due < SP_VIEW_POLL_MIN_SECONDS) due = SP_VIEW_POLL_MIN_SECONDS;
    double most = v->beat + SP_VIEW_POLL_MARGIN_SECONDS;
    return due < most ? due : most;
}

int sp_view_failed(SpView *v, const char *why, double *wait_seconds,
                   Report *report) {
    BEGIN(report);
    if (wait_seconds) *wait_seconds = SP_VIEW_POLL_MIN_SECONDS;
    if (v && !v->failed) {
        v->failed = 1;
        v->report = report;
        view_say(v, REPORT_ERROR, "[%s] view %s could not follow the "
                 "ticket: %s", v->stream, v->name, why ? why : "?");
        v->report = NULL;
    }
    return REPORT_OK;
}

int sp_view_followed(SpView *v, Report *report) {
    BEGIN(report);
    if (!v || !v->failed) return REPORT_OK;
    v->failed = 0;
    v->report = report;
    view_say(v, REPORT_INFO, "[%s] view %s current again", v->stream,
             v->name);
    v->report = NULL;
    return REPORT_OK;
}
