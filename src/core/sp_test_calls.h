/* sp_test_calls.h -- the stream core's calls as its C tests make them.
 *
 * A test states the time of each call and reads each call's reason and
 * lines. These helpers do that around the real calls: t_<name> sets the
 * core's clock (sp_test_set_clock) to the time the test gives, makes
 * the call with the one Report T, copies the reason into the test's
 * buffer, and keeps the lines the call made, for each object, until the
 * test reads them (t_reader_message, t_view_message). Nothing here is
 * part of the core. */
#ifndef SP_TEST_CALLS_H
#define SP_TEST_CALLS_H

#include "sp.h"

#include <stdio.h>
#include <string.h>

static Report T;                    /* the report of the last call */
static double t_wall = 1000.0, t_mono = 1000.0;

static inline void t_clock(void) { sp_test_set_clock(1, t_wall, t_mono); }
static inline void t_at_wall(double wall) { t_wall = wall; t_clock(); }
static inline void t_at_mono(double mono) { t_mono = mono; t_clock(); }

static inline int t_err(int status, char *err, size_t errlen) {
    if (err && errlen) snprintf(err, errlen, "%s", T.error);
    return status == REPORT_OK ? 0 : -1;
}

/* ---- the lines an object's calls made, until the test reads them ---- */
#define T_BOXES 32
#define T_KEPT 4
static struct {
    const void *object;
    int n;
    struct { int level; char text[REPORT_LINE_TEXT]; } line[T_KEPT];
} t_box[T_BOXES];

static inline void t_keep(const void *object) {
    int at = -1;
    for (int i = 0; i < T_BOXES; i++)
        if (t_box[i].object == object) at = i;
    for (int i = 0; at < 0 && i < T_BOXES; i++)
        if (!t_box[i].object) { at = i; t_box[i].object = object; t_box[i].n = 0; }
    if (at < 0) return;
    for (int k = 0; k < T.lines; k++) {
        if (t_box[at].n == T_KEPT) {              /* keep the newest */
            memmove(&t_box[at].line[0], &t_box[at].line[1],
                    (T_KEPT - 1) * sizeof t_box[at].line[0]);
            t_box[at].n--;
        }
        t_box[at].line[t_box[at].n].level = T.line[k].level;
        snprintf(t_box[at].line[t_box[at].n].text, REPORT_LINE_TEXT, "%s",
                 T.line[k].text);
        t_box[at].n++;
    }
}

static inline int t_message(const void *object, char *msg, size_t msglen) {
    if (msg && msglen) msg[0] = 0;
    for (int i = 0; i < T_BOXES; i++) {
        if (t_box[i].object != object || t_box[i].n == 0) continue;
        int level = t_box[i].line[0].level;
        if (msg && msglen) snprintf(msg, msglen, "%s", t_box[i].line[0].text);
        t_box[i].n--;
        memmove(&t_box[i].line[0], &t_box[i].line[1],
                (size_t)t_box[i].n * sizeof t_box[i].line[0]);
        return level;
    }
    return 0;
}

static inline void t_forget(const void *object) {
    for (int i = 0; i < T_BOXES; i++)
        if (t_box[i].object == object) { t_box[i].object = NULL; t_box[i].n = 0; }
}

/* ---- the ticket ---- */
static inline SpTicket *t_ticket_new(void) {
    SpTicket *t = NULL;
    sp_ticket_new(&t, &T);
    return t;
}
static inline int t_ticket_set_text(SpTicket *t, int f, const char *text, char *err, size_t n) {
    return t_err(sp_ticket_set_text(t, f, text, &T), err, n);
}
static inline int t_ticket_set_integer(SpTicket *t, int f, int64_t v, char *err, size_t n) {
    return t_err(sp_ticket_set_integer(t, f, v, &T), err, n);
}
static inline int t_ticket_set_number(SpTicket *t, int f, double v, char *err, size_t n) {
    return t_err(sp_ticket_set_number(t, f, v, &T), err, n);
}
static inline int t_ticket_add_file(SpTicket *t, const char *c, const int64_t *a,
                             const int64_t *b, const int64_t *r, char *err, size_t n) {
    return t_err(sp_ticket_add_file(t, c, a, b, r, &T), err, n);
}
static inline char *t_ticket_write(const SpTicket *t, size_t *len) {
    char *out = NULL;
    sp_ticket_write(t, &out, len, &T);
    return out;
}
static inline SpTicket *t_ticket_read(const char *text, size_t len, char *err, size_t n) {
    SpTicket *t = NULL;
    t_err(sp_ticket_read(text, len, &t, &T), err, n);
    return t;
}
static inline int t_ticket_is_stale(const SpTicket *t, double now, double if_absent) {
    t_at_wall(now);
    return sp_ticket_is_stale(t, if_absent);
}

/* ---- ownership ---- */
static inline char *t_lock_record(int64_t pid, const char *host, int64_t port,
                           const char *run, size_t *len) {
    char *out = NULL;
    sp_lock_record(pid, host, port, run, &out, len, &T);
    return out;
}
static inline void t_lock_refusal(const char *who, const char *path, const char *holder,
                           size_t holder_len, char *msg, size_t n) {
    sp_lock_refusal(who, path, holder, holder_len, &T);
    if (msg && n) snprintf(msg, n, "%s", T.error);
}
/* the decision, with its message: the error of a refusal, the warning
 * line of a take-over */
static inline int t_ownership(const char *who, const char *name, const char *store,
                       const SpTicket *ticket, const char *host, double now,
                       double if_absent, char *msg, size_t n) {
    int decision = -1;
    t_at_wall(now);
    sp_ownership(who, name, store, ticket, host, if_absent, &decision, &T);
    if (msg && n)
        snprintf(msg, n, "%s", T.status ? T.error : T.lines ? T.line[0].text : "");
    return decision;
}
static inline void t_ownership_unread(const char *name, const char *store,
                               const char *why, char *msg, size_t n) {
    sp_ownership_unread(name, store, why, &T);
    if (msg && n) snprintf(msg, n, "%s", T.lines ? T.line[0].text : "");
}

/* ---- the write plan: `now` is the monotonic clock ---- */
static inline SpWrites *t_writes_new(void) {
    SpWrites *w = NULL;
    sp_writes_new(&w, &T);
    return w;
}
static inline int t_writes_set_options(SpWrites *w, int64_t rows, const double *gs,
                                const double *cs, char *err, size_t n) {
    return t_err(sp_writes_set_options(w, rows, gs, cs, &T), err, n);
}
static inline int t_writes_due(const SpWrites *w, int64_t committed, double now) {
    t_at_mono(now);
    return sp_writes_due(w, committed);
}
static inline int t_writes_commit(SpWrites *w, int64_t committed, double now, int *begins) {
    t_at_mono(now);
    return sp_writes_commit(w, committed, begins);
}
static inline void t_writes_plan(SpWrites *w, int force, int64_t lo, int64_t hi,
                          double now, SpWrite *out) {
    t_at_mono(now);
    sp_writes_plan(w, force, lo, hi, out);
}

/* ---- the store: `wall` for the ticket's time, `now` monotonic ---- */
static inline SpStore *t_store_new(double keep) {
    SpStore *s = NULL;
    sp_store_new(keep, &s, &T);
    return s;
}
static inline int t_store_set_keep_seconds(SpStore *s, double keep) {
    return t_err(sp_store_set_keep_seconds(s, keep, &T), NULL, 0);
}
static inline int t_store_begins(SpStore *s, const char *file, int64_t first) {
    return t_err(sp_store_begins(s, file, first, &T), NULL, 0);
}
static inline int t_store_covers(SpStore *s, const char *file, int64_t version,
                          const char *ckpt, int64_t final_rows, int64_t rows) {
    return t_err(sp_store_covers(s, file, version, ckpt, final_rows, rows, &T), NULL, 0);
}
static inline int t_store_stored(SpStore *s, const char *file, int64_t version,
                          const char *ckpt, int64_t rows, double wall) {
    t_at_wall(wall);
    return t_err(sp_store_stored(s, file, version, ckpt, rows, &T), NULL, 0);
}
static inline int t_store_finished(SpStore *s, const char *file) {
    return t_err(sp_store_finished(s, file, &T), NULL, 0);
}
static inline int t_store_awaits(SpStore *s, const char *file) {
    return t_err(sp_store_awaits(s, file, &T), NULL, 0);
}
static inline int t_store_catalog(SpStore *s, const char *version) {
    return t_err(sp_store_catalog(s, version, &T), NULL, 0);
}
static inline int t_store_dropped(SpStore *s, double now) {
    t_at_mono(now);
    return sp_store_dropped(s);
}
static inline const char *t_store_due(const SpStore *s, double now, int32_t index) {
    t_at_mono(now);
    return sp_store_due(s, index);
}
static inline int t_store_discarded(SpStore *s, const char *file) {
    return t_err(sp_store_discarded(s, file, &T), NULL, 0);
}
static inline int t_store_fill(const SpStore *s, SpTicket *t, const char *where,
                        char *err, size_t n) {
    return t_err(sp_store_fill(s, t, where, &T), err, n);
}

/* ---- the reader ---- */
static inline SpReader *t_reader_new(const char *who, const char *name,
                              const char *store, int direct) {
    SpReader *r = NULL;
    sp_reader_new(who, name, store, direct, &r, &T);
    if (r) t_forget(r);
    return r;
}
static inline void t_reader_poll(SpReader *r, int found, const SpTicket *ticket,
                          const char *why, double now, double mono,
                          double if_absent, SpStep *step) {
    t_wall = now;
    t_at_mono(mono);
    if (sp_reader_poll(r, found, ticket, why, if_absent, step, &T) == REPORT_OK)
        t_keep(r);
}
static inline void t_reader_folder_is(SpReader *r, int exists, double mono, SpStep *step) {
    t_at_mono(mono);
    if (sp_reader_folder_is(r, exists, step, &T) == REPORT_OK) t_keep(r);
}
static inline void t_reader_lost(SpReader *r, const char *why, double mono, SpStep *step) {
    t_at_mono(mono);
    if (sp_reader_lost(r, why, step, &T) == REPORT_OK) t_keep(r);
}
static inline int t_reader_message(SpReader *r, char *msg, size_t n) {
    return t_message(r, msg, n);
}

/* ---- the view ---- */
static inline SpView *t_view_new(const char *view, const char *stream,
                          const char *memory, const char *empty) {
    SpView *v = NULL;
    sp_view_new(view, stream, memory, empty, &v, &T);
    if (v) t_forget(v);
    return v;
}
static inline int t_view_plan(SpView *v, const SpTicket *t, const char *const *files,
                       const char *ring_run, const int64_t *window, double now,
                       double mono, double if_absent, double keep_if_absent,
                       SpViewPlan *out) {
    t_wall = now;
    t_at_mono(mono);
    int rc = sp_view_plan(v, t, files, ring_run, window, if_absent,
                          keep_if_absent, out, &T);
    if (v) t_keep(v);
    return rc == REPORT_OK ? 0 : -1;
}
static inline char *t_view_sql(const SpView *v, const char *catalog, char *err, size_t n) {
    char *out = NULL;
    t_err(sp_view_sql(v, catalog, &out, &T), err, n);
    return out;
}
static inline char *t_view_live_sql(const char *view, const char *memory) {
    char *out = NULL;
    sp_view_live_sql(view, memory, &out, &T);
    return out;
}
static inline double t_view_wait_seconds(const SpView *v, double now) {
    t_at_wall(now);
    return sp_view_wait_seconds(v);
}
static inline double t_view_failed(SpView *v, const char *why) {
    double wait = 0;
    sp_view_failed(v, why, &wait, &T);
    if (v) t_keep(v);
    return wait;
}
static inline void t_view_followed(SpView *v) {
    sp_view_followed(v, &T);
    if (v) t_keep(v);
}
static inline int t_view_message(SpView *v, char *msg, size_t n) {
    return t_message(v, msg, n);
}

/* ---- the catalog ---- */
static inline int64_t t_parquet_footer_length(const uint8_t *tail, size_t len,
                                       char *err, size_t n) {
    int64_t out = 0;
    return t_err(sp_parquet_footer_length(tail, len, &out, &T), err, n) ? -1 : out;
}
static inline SpCatalog *t_catalog_open(const uint8_t *bytes, size_t len,
                                 const char *data_path, double now,
                                 char *err, size_t n) {
    SpCatalog *c = NULL;
    t_at_wall(now);
    t_err(sp_catalog_open(bytes, len, data_path, &c, &T), err, n);
    return c;
}
static inline char *t_catalog_file(const SpCatalog *c, int64_t index) {
    char *out = NULL;
    sp_catalog_file(c, index, &out, &T);
    return out;
}
static inline int t_catalog_add(SpCatalog *c, const char *path, int64_t size,
                         const uint8_t *footer, size_t footer_len,
                         char *err, size_t n) {
    return t_err(sp_catalog_add(c, path, size, footer, footer_len, &T), err, n);
}
static inline uint8_t *t_catalog_bytes(SpCatalog *c, double now, size_t *len,
                                char *err, size_t n) {
    uint8_t *out = NULL;
    t_at_wall(now);
    t_err(sp_catalog_bytes(c, &out, len, &T), err, n);
    return out;
}

#endif
