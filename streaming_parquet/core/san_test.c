/* san_test.c -- the stream core under AddressSanitizer and UBSan.
 * `make san`. Every check prints nothing; a failure names its line. */
#include "sp.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;

#define CHECK(cond) do { if (!(cond)) { \
    fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); \
    failures++; } } while (0)

static SpTicket *full_ticket(void) {
    char err[256];
    SpTicket *t = sp_ticket_new();
    CHECK(sp_ticket_set_text(t, SP_T_NAME, "plant/line-1/press", err, sizeof err) == 0);
    CHECK(sp_ticket_set_text(t, SP_T_HOST, "10.0.0.5", err, sizeof err) == 0);
    CHECK(sp_ticket_set_integer(t, SP_T_PORT, 8815, err, sizeof err) == 0);
    CHECK(sp_ticket_set_text(t, SP_T_RUN, "2026-11-12T20:31:12.123456Z", err, sizeof err) == 0);
    CHECK(sp_ticket_set_number(t, SP_T_BEAT_SECONDS, 15, err, sizeof err) == 0);
    CHECK(sp_ticket_set_number(t, SP_T_STALE_AFTER, 45, err, sizeof err) == 0);
    CHECK(sp_ticket_set_number(t, SP_T_CHECKPOINT_SECONDS, 0.1, err, sizeof err) == 0);
    CHECK(sp_ticket_set_integer(t, SP_T_STORED_ROW, 123456789012345LL, err, sizeof err) == 0);
    CHECK(sp_ticket_set_number(t, SP_T_STORED_AT, 1762979472.123456, err, sizeof err) == 0);
    CHECK(sp_ticket_set_number(t, SP_T_KEEP_SECONDS, 600, err, sizeof err) == 0);
    CHECK(sp_ticket_set_text(t, SP_T_CATALOG_ETAG, "\"abc-1\"", err, sizeof err) == 0);
    CHECK(sp_ticket_set_integer(t, SP_T_REVISION, 7, err, sizeof err) == 0);
    CHECK(sp_ticket_set_number(t, SP_T_TS, 1762979473.5, err, sizeof err) == 0);
    int64_t first = 1000, final = 800, rows = 950;
    CHECK(sp_ticket_add_file(t, "s3://b/streams/x/a.parquet.000003.ckpt",
                             &first, &final, &rows, err, sizeof err) == 0);
    CHECK(sp_ticket_add_file(t, "s3://b/streams/x/b \xc3\xa9.parquet.000001.ckpt",
                             NULL, NULL, NULL, err, sizeof err) == 0);
    return t;
}

static void same(const SpTicket *a, const SpTicket *b) {
    for (int f = 0; f < SP_T_FIELDS; f++) {
        const char *ta = sp_ticket_text(a, f), *tb = sp_ticket_text(b, f);
        CHECK((ta == NULL) == (tb == NULL));
        if (ta && tb) CHECK(strcmp(ta, tb) == 0);
        double na = 0, nb = 0;
        CHECK(sp_ticket_number(a, f, &na) == sp_ticket_number(b, f, &nb));
        CHECK(na == nb);
        int64_t ia = 0, ib = 0;
        CHECK(sp_ticket_integer(a, f, &ia) == sp_ticket_integer(b, f, &ib));
        CHECK(ia == ib);
    }
    CHECK(sp_ticket_file_count(a) == sp_ticket_file_count(b));
    for (int32_t i = 0; i < sp_ticket_file_count(a); i++) {
        const char *ca, *cb;
        int64_t a1, a2, a3, b1, b2, b3;
        int ha1, ha2, ha3, hb1, hb2, hb3;
        CHECK(sp_ticket_file(a, i, &ca, &a1, &ha1, &a2, &ha2, &a3, &ha3) == 0);
        CHECK(sp_ticket_file(b, i, &cb, &b1, &hb1, &b2, &hb2, &b3, &hb3) == 0);
        CHECK(strcmp(ca, cb) == 0);
        CHECK(ha1 == hb1 && ha2 == hb2 && ha3 == hb3);
        if (ha1) CHECK(a1 == b1);
        if (ha2) CHECK(a2 == b2);
        if (ha3) CHECK(a3 == b3);
    }
}

static void refused(const char *text, const char *reason) {
    char err[256] = "";
    SpTicket *t = sp_ticket_read(text, strlen(text), err, sizeof err);
    if (t != NULL || strstr(err, reason) == NULL) {
        fprintf(stderr, "FAIL: %s was %s, error \"%s\", want \"%s\"\n", text,
                t ? "read" : "refused", err, reason);
        failures++;
    }
    sp_ticket_free(t);
}

int main(void) {
    char err[256];

    /* written, read back: the same ticket */
    SpTicket *t = full_ticket();
    size_t len;
    char *text = sp_ticket_write(t, &len);
    CHECK(text != NULL && strlen(text) == len);
    CHECK(strstr(text, "\"ship_seconds\": null") != NULL);  /* unknown: null */
    CHECK(strstr(text, "\"archive\"") == NULL);             /* not set: absent */
    CHECK(strstr(text, "\"unfinished\": [\"s3://b/streams/x/a.parquet.000003.ckpt\", ") != NULL);
    SpTicket *back = sp_ticket_read(text, len, err, sizeof err);
    CHECK(back != NULL);
    if (back) same(t, back);
    char *again = back ? sp_ticket_write(back, NULL) : NULL;
    CHECK(again && strcmp(text, again) == 0);
    sp_free(again);
    sp_ticket_free(back);
    sp_free(text);

    /* the stale rule */
    CHECK(sp_ticket_stale_after(t, 30) == 45);
    CHECK(!sp_ticket_is_stale(t, 1762979473.5 + 44.9, 30));
    CHECK(sp_ticket_is_stale(t, 1762979473.5 + 45, 30));
    sp_ticket_clear(t, SP_T_STALE_AFTER);
    CHECK(sp_ticket_stale_after(t, 30) == 30);
    CHECK(sp_ticket_is_stale(t, 1762979473.5 + 30, 30));
    sp_ticket_clear(t, SP_T_TS);
    CHECK(sp_ticket_is_stale(t, 0, 30));                    /* no ts: stale */
    sp_ticket_free(t);

    /* an old ticket: few members, one the core does not know, escapes */
    const char *old = " {\"name\":\"air/dfw\",\"host\":\"10.0.0.5\","
        "\"port\":8815.0,\"ts\":1.7607456e9,\"extra\":{\"a\":[1,true,null,\"x\"]},"
        "\"archive\":\"s3:\\/\\/b\\u00e9\\ud83c\\udfaf\"} ";
    t = sp_ticket_read(old, strlen(old), err, sizeof err);
    CHECK(t != NULL);
    if (t) {
        int64_t port;
        double ts;
        CHECK(sp_ticket_integer(t, SP_T_PORT, &port) && port == 8815);
        CHECK(sp_ticket_number(t, SP_T_TS, &ts) && ts == 1760745600.0);
        CHECK(strcmp(sp_ticket_text(t, SP_T_ARCHIVE),
                     "s3://b\xc3\xa9\xf0\x9f\x8e\xaf") == 0);
        CHECK(sp_ticket_text(t, SP_T_RUN) == NULL);
        CHECK(sp_ticket_file_count(t) == 0);
        text = sp_ticket_write(t, NULL);
        CHECK(text && strstr(text, "\"archive\": \"s3://b") != NULL);
        sp_free(text);
    }
    sp_ticket_free(t);

    /* setters refuse the wrong kind, bad text, numbers not finite */
    t = sp_ticket_new();
    CHECK(sp_ticket_set_text(t, SP_T_PORT, "x", err, sizeof err) == -1);
    CHECK(strstr(err, "port is an integer") != NULL);
    CHECK(sp_ticket_set_number(t, SP_T_PORT, 1.5, err, sizeof err) == -1);
    CHECK(sp_ticket_set_integer(t, SP_T_NAME, 1, err, sizeof err) == -1);
    CHECK(sp_ticket_set_text(t, SP_T_NAME, "\xff\xfe", err, sizeof err) == -1);
    CHECK(sp_ticket_set_number(t, SP_T_TS, NAN, err, sizeof err) == -1);
    CHECK(sp_ticket_set_number(t, SP_T_TS, INFINITY, err, sizeof err) == -1);
    CHECK(sp_ticket_set_integer(t, SP_T_TS, 5, err, sizeof err) == 0);
    CHECK(sp_ticket_set_text(t, 99, "x", err, sizeof err) == -1);
    CHECK(sp_ticket_add_file(t, NULL, NULL, NULL, NULL, err, sizeof err) == -1);
    CHECK(sp_ticket_field_name(SP_T_FIELDS) == NULL);
    CHECK(sp_ticket_field_kind(-1) == -1);
    sp_ticket_free(t);

    /* the reader refuses, naming the member */
    refused("", "not a JSON object");
    refused("[]", "not a JSON object");
    refused("{\"port\": \"8815\"}", "port is not a number");
    refused("{\"port\": 8815.5}", "port is not an integer");
    refused("{\"name\": 5}", "name is not text");
    refused("{\"ts\": 1e999}", "number too large");
    refused("{\"files\": {}}", "files is not a list");
    refused("{\"files\": [{\"rows\": 5}]}", "a file with no checkpoint");
    refused("{\"files\": [{\"checkpoint\": \"a\", \"rows\": 1.5}]}", "rows is not an integer");
    refused("{\"name\": \"a\"} x", "text after the object");
    refused("{\"name\": \"a\\u0000b\"}", "0 character");
    refused("{\"name\": \"\\ud83c\"}", "half a character");
    refused("{\"name\": \"a", "text with no end");
    refused("{\"name\" \"a\"}", "':' expected");

    /* ---- the ownership decision ---- */
    {
        char msg[1024];
        size_t len;
        char *rec = sp_lock_record(4242, "10.0.0.5", 8815,
                                   "2026-11-12T20:31:12.123456Z", &len);
        CHECK(strcmp(rec, "{\"pid\":4242,\"host\":\"10.0.0.5\",\"port\":8815,"
                          "\"run\":\"2026-11-12T20:31:12.123456Z\"}") == 0);
        sp_lock_refusal("create_stream('a/b')", "/s/streams/a/b/writer.lock",
                        rec, len, msg, sizeof msg);
        CHECK(strcmp(msg, "create_stream('a/b'): another process on this "
            "server writes this stream: pid 4242, run "
            "2026-11-12T20:31:12.123456Z holds /s/streams/a/b/writer.lock. "
            "One writer per stream: stop that process first.") == 0);
        sp_free(rec);
        /* Python's own spacing, an empty file, damage: all answer */
        const char *py = "{\"pid\": 7, \"host\": \"h\", \"port\": 1, \"run\": \"r1\"}";
        sp_lock_refusal("w", "p", py, strlen(py), msg, sizeof msg);
        CHECK(strstr(msg, "pid 7, run r1 holds p") != NULL);
        sp_lock_refusal("w", "p", "", 0, msg, sizeof msg);
        CHECK(strstr(msg, "pid ?, run ? holds p") != NULL);
        sp_lock_refusal("w", "p", NULL, 0, msg, sizeof msg);
        CHECK(strstr(msg, "pid ?, run ?") != NULL);
        const char *bad = "{\"pid\": \"x\", \"run\": 5, \"z\": [1,{\"a\":";
        sp_lock_refusal("w", "p", bad, strlen(bad), msg, sizeof msg);
        CHECK(strstr(msg, "pid ?, run ?") != NULL);
        char tiny[8];
        sp_lock_refusal("w", "p", py, strlen(py), tiny, sizeof tiny);
        CHECK(strlen(tiny) == 7);
        rec = sp_lock_record(1, NULL, 0, NULL, NULL);
        CHECK(strstr(rec, "\"host\":null") && strstr(rec, "\"run\":null"));
        sp_free(rec);

        SpTicket *o = full_ticket();              /* host 10.0.0.5, ts ...473.5, stale 45 */
        double ts = 1762979473.5;
        CHECK(sp_ownership("w", "n", "s", NULL, "10.0.0.9", ts, 30, msg, sizeof msg) == SP_OWN);
        CHECK(msg[0] == 0);
        CHECK(sp_ownership("w", "n", "s", o, "10.0.0.5", ts + 1, 30, msg, sizeof msg) == SP_OWN);
        CHECK(msg[0] == 0);
        CHECK(sp_ownership("create_stream('n')", "n", "s3://b", o, "10.0.0.9",
                           ts + 12.4, 30, msg, sizeof msg) == SP_REFUSED);
        CHECK(strcmp(msg, "create_stream('n'): the server at 10.0.0.5:8815 "
            "writes this stream: its ticket in s3://b is 12s old (run "
            "2026-11-12T20:31:12.123456Z; a ticket is stale after 45s). One "
            "writer per stream: stop it there, or wait until its ticket is "
            "stale.") == 0);
        CHECK(sp_ownership("w", "n", "s", o, "10.0.0.9", ts + 44.9, 30, msg, sizeof msg) == SP_REFUSED);
        CHECK(sp_ownership("w", "n", "s3://b", o, "10.0.0.9", ts + 45, 30, msg, sizeof msg) == SP_TAKE_OVER);
        CHECK(strcmp(msg, "[n] taking over from 10.0.0.5:8815: its ticket in "
            "s3://b is 45s old (run 2026-11-12T20:31:12.123456Z); stale "
            "after 45s") == 0);
        /* a ticket that does not say: the host's rule; no ts: stale */
        sp_ticket_clear(o, SP_T_STALE_AFTER);
        CHECK(sp_ownership("w", "n", "s", o, "10.0.0.9", ts + 29, 30, msg, sizeof msg) == SP_REFUSED);
        CHECK(sp_ownership("w", "n", "s", o, "10.0.0.9", ts + 30, 30, msg, sizeof msg) == SP_TAKE_OVER);
        sp_ticket_clear(o, SP_T_TS);
        CHECK(sp_ownership("w", "n", "s", o, "10.0.0.9", ts, 30, msg, sizeof msg) == SP_TAKE_OVER);
        /* a ticket with no host is another writer's */
        sp_ticket_clear(o, SP_T_HOST);
        sp_ticket_clear(o, SP_T_PORT);
        sp_ticket_clear(o, SP_T_RUN);
        CHECK(sp_ownership("w", "n", "s", o, "10.0.0.9", ts, 30, msg, sizeof msg) == SP_TAKE_OVER);
        CHECK(strstr(msg, "from ?:?") && strstr(msg, "(run ?)"));
        CHECK(sp_ownership(NULL, NULL, NULL, o, NULL, ts, 30, NULL, 0) == SP_TAKE_OVER);
        sp_ticket_free(o);

        sp_ownership_unread("n", "s3://b", "timeout", msg, sizeof msg);
        CHECK(strcmp(msg, "[n] ownership: the ticket in s3://b could not be "
            "read (timeout); starting on this server's lock alone -- another "
            "server writing this stream would not be seen") == 0);
    }

    /* ---- the write plan ---- */
    {
        SpWrite o;
        int begins;
        double rgs = 10, cs = 2;
        SpWrites *w = sp_writes_new();
        CHECK(sp_writes_set_options(w, 0, NULL, NULL, err, sizeof err) == -1);
        CHECK(strstr(err, "row_group_rows is 0") != NULL);
        double bad = -1;
        CHECK(sp_writes_set_options(w, 5, &bad, NULL, err, sizeof err) == -1);
        bad = NAN;
        CHECK(sp_writes_set_options(w, 5, NULL, &bad, err, sizeof err) == -1);
        CHECK(sp_writes_set_options(w, 100, &rgs, &cs, err, sizeof err) == 0);

        /* off: nothing is due, nothing is planned */
        CHECK(sp_writes_commit(w, 500, 0, &begins) == SP_W_NOTHING && !begins);
        sp_writes_plan(w, 1, 0, 500, 0, &o);
        CHECK(o.kind == SP_W_NOTHING);

        /* on at row 500: the earlier rows are not planned */
        sp_writes_start(w, 500);
        CHECK(sp_writes_on(w) && sp_writes_cursor(w) == 500);
        CHECK(sp_writes_commit(w, 500, 1, &begins) == SP_W_NOTHING && !begins);
        /* the first pending row starts the clock; the answer is from before */
        CHECK(sp_writes_commit(w, 510, 1, &begins) == SP_W_NOTHING && begins);
        CHECK(sp_writes_commit(w, 520, 2, &begins) == SP_W_NOTHING && !begins);
        /* a checkpoint: cadence from the first pending row */
        CHECK(sp_writes_due(w, 520, 2.9) == SP_W_NOTHING);
        CHECK(sp_writes_due(w, 520, 3.0) == SP_W_CHECKPOINT);
        sp_writes_plan(w, 0, 500, 520, 3.0, &o);
        CHECK(o.kind == SP_W_CHECKPOINT && o.lo == 500 && o.hi == 520 && o.lost == 0);
        CHECK(sp_writes_cursor(w) == 500 && sp_writes_planned(w) == 520);
        /* no new rows: nothing to claim, however long */
        CHECK(sp_writes_due(w, 520, 9.0) == SP_W_NOTHING);
        /* new rows: the cadence runs from the last planned write */
        CHECK(sp_writes_due(w, 530, 4.9) == SP_W_NOTHING);
        CHECK(sp_writes_due(w, 530, 5.0) == SP_W_CHECKPOINT);
        /* the row group by time, from the first pending row (at 1) */
        CHECK(sp_writes_due(w, 530, 11.0) == SP_W_ROW_GROUP);
        /* the row group by rows */
        CHECK(sp_writes_due(w, 599, 4.0) == SP_W_NOTHING);
        CHECK(sp_writes_due(w, 600, 4.0) == SP_W_ROW_GROUP);
        sp_writes_plan(w, 0, 500, 640, 4.0, &o);
        CHECK(o.kind == SP_W_ROW_GROUP && o.lo == 500 && o.hi == 640 && o.lost == 0);
        CHECK(sp_writes_cursor(w) == 640 && sp_writes_planned(w) == 640);
        /* the clock starts again with the next pending row */
        CHECK(sp_writes_commit(w, 640, 30, &begins) == SP_W_NOTHING && !begins);
        /* (and after an idle spell the first new row is claimed at once) */
        CHECK(sp_writes_commit(w, 641, 30, &begins) == SP_W_CHECKPOINT && begins);
        CHECK(sp_writes_due(w, 641, 39.9) != SP_W_ROW_GROUP);
        CHECK(sp_writes_due(w, 641, 40.0) == SP_W_ROW_GROUP);

        /* a forced row group (a cut): due or not; nothing pending: nothing */
        sp_writes_plan(w, 1, 640, 650, 31, &o);
        CHECK(o.kind == SP_W_ROW_GROUP && o.lo == 640 && o.hi == 650);
        sp_writes_plan(w, 1, 650, 650, 31, &o);
        CHECK(o.kind == SP_W_NOTHING && o.lost == 0);

        /* lost rows: a checkpoint is not planned over them ... */
        CHECK(sp_writes_commit(w, 700, 40, &begins) == SP_W_CHECKPOINT && begins);
        CHECK(sp_writes_due(w, 700, 42) == SP_W_CHECKPOINT);
        sp_writes_plan(w, 0, 670, 700, 42, &o);
        CHECK(o.kind == SP_W_NOTHING && o.lost == 0 && sp_writes_lost(w) == 0);
        CHECK(sp_writes_cursor(w) == 650 && sp_writes_planned(w) == 650);
        /* ... the next row group counts them and goes on */
        sp_writes_plan(w, 0, 670, 760, 43, &o);
        CHECK(o.kind == SP_W_ROW_GROUP && o.lo == 670 && o.hi == 760 && o.lost == 20);
        CHECK(sp_writes_lost(w) == 20 && sp_writes_cursor(w) == 760);
        /* every pinned row lost: counted, no write */
        sp_writes_plan(w, 1, 900, 900, 44, &o);
        CHECK(o.kind == SP_W_NOTHING && o.lost == 140 && sp_writes_cursor(w) == 900);
        CHECK(sp_writes_lost(w) == 160 && sp_writes_planned(w) == 900);

        /* stop: off; a second start is at the new row */
        sp_writes_stop(w);
        CHECK(!sp_writes_on(w) && sp_writes_due(w, 5000, 99) == SP_W_NOTHING);
        sp_writes_start(w, 5000);
        CHECK(sp_writes_cursor(w) == 5000 && sp_writes_planned(w) == 5000);
        sp_writes_start(w, 6000);                 /* on already: no change */
        CHECK(sp_writes_cursor(w) == 5000);

        /* no cadences: only rows make a write due */
        CHECK(sp_writes_set_options(w, 100, NULL, NULL, err, sizeof err) == 0);
        CHECK(sp_writes_commit(w, 5050, 100, &begins) == SP_W_NOTHING && begins);
        CHECK(sp_writes_due(w, 5050, 1e9) == SP_W_NOTHING);
        /* new options start the checkpoint cadence again, from the
         * first pending row */
        sp_writes_set_options(w, 100, NULL, &cs, err, sizeof err);
        CHECK(sp_writes_due(w, 5050, 101.9) == SP_W_NOTHING);
        CHECK(sp_writes_due(w, 5050, 102) == SP_W_CHECKPOINT);
        sp_writes_free(w);

        /* NULL is taken everywhere */
        CHECK(sp_writes_due(NULL, 1, 1) == SP_W_NOTHING);
        CHECK(sp_writes_commit(NULL, 1, 1, NULL) == SP_W_NOTHING);
        sp_writes_plan(NULL, 1, 0, 1, 1, &o);
        CHECK(o.kind == SP_W_NOTHING);
        sp_writes_start(NULL, 1); sp_writes_stop(NULL); sp_writes_free(NULL);
        CHECK(sp_writes_cursor(NULL) == 0 && sp_writes_on(NULL) == 0);

        /* a long random run: the rows planned as row groups are every
         * row from the start, once, in order, less the lost ones */
        w = sp_writes_new();
        double c2 = 0.5, g2 = 3;
        sp_writes_set_options(w, 64, &g2, &c2, err, sizeof err);
        sp_writes_start(w, 0);
        uint64_t seed = 88172645463325252ULL;
        int64_t committed = 0, next = 0, lost = 0;
        double now = 0;
        for (int i = 0; i < 200000; i++) {
            seed ^= seed << 13; seed ^= seed >> 7; seed ^= seed << 17;
            committed += (int64_t)(seed % 40);
            now += (double)((seed >> 8) % 100) / 100.0;
            int due = sp_writes_commit(w, committed, now, &begins);
            int force = (seed >> 20) % 50 == 0;
            if (!due && !force) continue;
            int64_t lo = sp_writes_cursor(w);
            if ((seed >> 30) % 97 == 0) lo += (int64_t)((seed >> 40) % 30);
            if (lo > committed) lo = committed;
            sp_writes_plan(w, force, lo, committed, now, &o);
            lost += o.lost;
            if (o.kind == SP_W_ROW_GROUP) {
                CHECK(o.lo == next + o.lost && o.hi > o.lo && o.hi == committed);
                next = o.hi;
            } else if (o.kind == SP_W_CHECKPOINT) {
                CHECK(o.lo == next && o.lost == 0 && o.hi == committed);
            } else {
                next += o.lost;
            }
            CHECK(sp_writes_cursor(w) == next);
            CHECK(sp_writes_planned(w) >= next && sp_writes_planned(w) <= committed);
        }
        CHECK(sp_writes_lost(w) == lost && lost > 0 && next > 1000000);
        sp_writes_free(w);
    }

    /* ---- what the store holds ---- */
    {
        double at;
        CHECK(sp_store_new(-1) == NULL && sp_store_new(NAN) == NULL);
        SpStore *st = sp_store_new(600);
        SpTicket *k = sp_ticket_new();
        CHECK(sp_store_stored_row(st) == -1 && !sp_store_stored_at(st, &at));
        CHECK(sp_store_fill(st, k, "s3://b/x/", err, sizeof err) == 0);
        char *text = sp_ticket_write(k, NULL);
        CHECK(strstr(text, "\"stored_row\": -1") && strstr(text, "\"stored_at\": null"));
        CHECK(strstr(text, "\"files\": []") && strstr(text, "\"keep_seconds\": 600"));
        sp_free(text);

        /* file b begins at row 1000; two versions written, the first stored */
        CHECK(sp_store_begins(st, "b.parquet", 1000) == 0);
        CHECK(sp_store_covers(st, "b.parquet", 1, "b.parquet.000001.ckpt", 0, 50) == 0);
        CHECK(sp_store_fill(st, k, "w/", err, sizeof err) == 0);
        CHECK(sp_ticket_file_count(k) == 0);       /* written, not yet in the store */
        CHECK(sp_store_stored(st, "b.parquet", 1, "b.parquet.000001.ckpt", 50, 111.5) == 0);
        CHECK(sp_store_stored_row(st) == 1049);
        CHECK(sp_store_stored_at(st, &at) && at == 111.5);
        CHECK(sp_store_covers(st, "b.parquet", 2, "b.parquet.000002.ckpt", 64, 80) == 0);
        CHECK(sp_store_fill(st, k, "w/", err, sizeof err) == 0);
        const char *cp; int64_t fr, fin, rows; int hf, hfin, hr;
        CHECK(sp_ticket_file_count(k) == 1);
        sp_ticket_file(k, 0, &cp, &fr, &hf, &fin, &hfin, &rows, &hr);
        CHECK(strcmp(cp, "w/b.parquet.000001.ckpt") == 0 && hf && fr == 1000);
        CHECK(hfin && fin == 0 && hr && rows == 50);
        /* the second lands: the ticket names it; the first is forgotten */
        CHECK(sp_store_stored(st, "b.parquet", 2, "b.parquet.000002.ckpt", 80, 112.5) == 0);
        CHECK(sp_store_stored_row(st) == 1079);
        /* a version that holds no later row does not move the stored row */
        CHECK(sp_store_stored(st, "b.parquet", 2, "b.parquet.000002.ckpt", 80, 999) == 0);
        CHECK(sp_store_stored_at(st, &at) && at == 112.5);
        /* file a: found by a start-up sweep, nothing known of it */
        CHECK(sp_store_stored(st, "a.parquet", 7, "a.parquet.000007.ckpt", 0, 113) == 0);
        CHECK(sp_store_stored_row(st) == 1079);
        CHECK(sp_store_fill(st, k, "w/", err, sizeof err) == 0);
        CHECK(sp_ticket_file_count(k) == 2);       /* in the order of the names */
        sp_ticket_file(k, 0, &cp, &fr, &hf, &fin, &hfin, &rows, &hr);
        CHECK(strcmp(cp, "w/a.parquet.000007.ckpt") == 0 && !hf && !hfin && !hr);
        sp_ticket_file(k, 1, &cp, &fr, &hf, &fin, &hfin, &rows, &hr);
        CHECK(strcmp(cp, "w/b.parquet.000002.ckpt") == 0 && fin == 64 && rows == 80);

        /* a is finished and nothing lists finished files: it is gone */
        CHECK(sp_store_finished(st, "a.parquet") == 0);
        CHECK(sp_store_files(st) == 1);
        /* b is finished and awaits its catalog: it stays in the ticket */
        CHECK(sp_store_awaits(st, "b.parquet") == 0);
        CHECK(sp_store_fill(st, k, "w/", err, sizeof err) == 0);
        CHECK(sp_ticket_file_count(k) == 1);
        CHECK(sp_store_listed(st, "zzz.parquet") == 0);
        CHECK(sp_store_listed(st, "b.parquet") == 1);
        CHECK(sp_store_fill(st, k, "w/", err, sizeof err) == 0);
        CHECK(sp_ticket_file_count(k) == 0);
        /* the ticket write failed: nothing is superseded; a second
         * catalog pass finds it again */
        CHECK(sp_store_due(st, 1e9, 0) == NULL);
        CHECK(sp_store_listed(st, "b.parquet") == 1);
        CHECK(sp_store_dropped(st, 50.0) == 1);
        CHECK(sp_store_dropped(st, 60.0) == 0);
        CHECK(sp_store_listed(st, "b.parquet") == 0);
        /* its checkpoints stay keep_seconds, then are due */
        CHECK(sp_store_due(st, 649.9, 0) == NULL);
        CHECK(strcmp(sp_store_due(st, 650.0, 0), "b.parquet") == 0);
        CHECK(sp_store_due(st, 650.0, 1) == NULL);
        /* a shorter keep time counts from the same moment */
        CHECK(sp_store_set_keep_seconds(st, -1) == -1);
        CHECK(sp_store_set_keep_seconds(st, 10) == 0 && sp_store_keep_seconds(st) == 10);
        CHECK(strcmp(sp_store_due(st, 60.0, 0), "b.parquet") == 0);
        CHECK(sp_store_set_keep_seconds(st, 600) == 0);
        /* a delete that failed leaves it due; a delete that worked ends it */
        CHECK(strcmp(sp_store_due(st, 700.0, 0), "b.parquet") == 0);
        CHECK(sp_store_discarded(st, "b.parquet") == 0);
        CHECK(sp_store_due(st, 700.0, 0) == NULL && sp_store_files(st) == 0);
        /* the stored row outlives its file */
        CHECK(sp_store_stored_row(st) == 1079);

        /* a long run does not grow the state */
        for (int i = 0; i < 5000; i++) {
            char name[40], ck[60];
            snprintf(name, sizeof name, "f%06d.parquet", i);
            snprintf(ck, sizeof ck, "%s.000001.ckpt", name);
            CHECK(sp_store_begins(st, name, 2000 + (int64_t)i * 10) == 0);
            CHECK(sp_store_covers(st, name, 1, ck, 10, 10) == 0);
            CHECK(sp_store_stored(st, name, 1, ck, 10, 200.0 + i) == 0);
            if (i % 2) {
                CHECK(sp_store_finished(st, name) == 0);
            } else {
                CHECK(sp_store_awaits(st, name) == 0);
                CHECK(sp_store_listed(st, name) == 1);
                CHECK(sp_store_dropped(st, (double)i) == 1);
                const char *due;
                while ((due = sp_store_due(st, (double)i + 600, 0)) != NULL)
                    CHECK(sp_store_discarded(st, due) == 0);
            }
            CHECK(sp_store_files(st) <= 1);
        }
        CHECK(sp_store_stored_row(st) == 2000 + 4999 * 10 + 9);
        CHECK(sp_store_files(st) == 0);

        /* NULL is taken everywhere */
        CHECK(sp_store_begins(NULL, "a", 0) == -1 && sp_store_begins(st, NULL, 0) == -1);
        CHECK(sp_store_covers(st, "a", 1, NULL, 0, 0) == -1);
        CHECK(sp_store_stored(st, "a", 1, NULL, 0, 0) == -1);
        CHECK(sp_store_finished(st, "nothing") == 0 && sp_store_discarded(st, "nothing") == 0);
        CHECK(sp_store_listed(NULL, "a") == 0 && sp_store_dropped(NULL, 0) == 0);
        CHECK(sp_store_due(NULL, 0, 0) == NULL && sp_store_due(st, 0, -1) == NULL);
        CHECK(sp_store_fill(NULL, k, "", err, sizeof err) == -1);
        CHECK(sp_store_stored_row(NULL) == -1 && sp_store_files(NULL) == 0);
        sp_store_free(NULL);
        sp_ticket_free(k);
        sp_store_free(st);
    }

    /* ---- the reader ---- */
    {
        SpStep st;
        char msg[700];
        const double T0 = 1762979473.5;       /* full_ticket's ts */
        SpTicket *k = full_ticket();          /* run ...12.123456Z, stale after 45 */

        /* the folder above the name */
        SpReader *r = sp_reader_new("connect('a/b/c')", "a/b/c", "/data", 0);
        CHECK(r && strcmp(sp_reader_folder(r), "a/b") == 0);
        CHECK(sp_reader_state(r) == SP_R_NO_TICKET);
        CHECK(strcmp(sp_reader_state_name(SP_R_ROOT_UNREACHABLE), "root-unreachable") == 0);
        CHECK(sp_reader_state_name(99) == NULL && sp_reader_state_name(-1) == NULL);

        /* no ticket at the first poll: look for the folder; none: fail */
        sp_reader_poll(r, SP_FOUND_NONE, NULL, NULL, T0, 100, 30, &st);
        CHECK(st.action == SP_DO_LIST && st.state == SP_R_NO_TICKET);
        CHECK(sp_reader_message(r, msg, sizeof msg) == 0);
        sp_reader_folder_is(r, 0, 100, &st);
        CHECK(st.action == SP_DO_FAIL && st.state == SP_R_CLOSED);
        CHECK(strstr(sp_reader_reason(r), "connect('a/b/c'): the folder a/b does not exist in the store /data: ") != NULL);
        CHECK(strstr(sp_reader_reason(r), "the folder a/b") != NULL);
        sp_reader_free(r);

        /* the folder exists: wait, one line at the start, one warning at 30 s */
        r = sp_reader_new("connect('a/b/c')", "a/b/c", "/data", 0);
        sp_reader_poll(r, SP_FOUND_NONE, NULL, NULL, T0, 100, 30, &st);
        CHECK(st.action == SP_DO_LIST);
        sp_reader_folder_is(r, 1, 100, &st);
        CHECK(st.action == SP_DO_WAIT && st.state == SP_R_NO_TICKET && st.wait_seconds == SP_READER_POLL_SECONDS);
        CHECK(sp_reader_message(r, msg, sizeof msg) == SP_LOG_INFO);
        CHECK(strcmp(msg, "[a/b/c] no ticket for a/b/c under /data: no writer is up: waiting for a writer") == 0);
        CHECK(sp_reader_message(r, msg, sizeof msg) == 0);
        for (int i = 1; i < 30; i++) {
            sp_reader_poll(r, SP_FOUND_NONE, NULL, NULL, T0 + i, 100 + i, 30, &st);
            CHECK(st.action == SP_DO_WAIT);             /* not asked again */
            CHECK(sp_reader_message(r, msg, sizeof msg) == 0);
        }
        sp_reader_poll(r, SP_FOUND_NONE, NULL, NULL, T0 + 30, 130, 30, &st);
        CHECK(sp_reader_message(r, msg, sizeof msg) == SP_LOG_WARNING);
        CHECK(strstr(msg, "connect('a/b/c') has waited 30 s for a writer") != NULL);
        for (int i = 31; i < 200; i++) {                /* one warning for a wait */
            sp_reader_poll(r, SP_FOUND_NONE, NULL, NULL, T0 + i, 100 + i, 30, &st);
            CHECK(sp_reader_message(r, msg, sizeof msg) == 0);
        }

        /* a ticket: connect; connected: streaming, no reason */
        sp_reader_poll(r, SP_FOUND_TICKET, k, NULL, T0 + 1, 300, 30, &st);
        CHECK(st.action == SP_DO_CONNECT && st.state == SP_R_CONNECTING && !st.fresh_ring);
        CHECK(strcmp(sp_reader_run(r), "2026-11-12T20:31:12.123456Z") == 0);
        sp_reader_connected(r);
        CHECK(sp_reader_state(r) == SP_R_STREAMING && sp_reader_reason(r)[0] == 0);

        /* lost: 0.5 s, then twice as long, up to 10 s; a connection sets it back */
        double want[] = {0.5, 1, 2, 4, 8, 10, 10};
        for (int i = 0; i < 7; i++) {
            sp_reader_lost(r, "refused", 301, &st);
            CHECK(st.action == SP_DO_WAIT && st.state == SP_R_RECONNECTING && st.wait_seconds == want[i]);
            CHECK(sp_reader_message(r, msg, sizeof msg) == SP_LOG_ERROR);
            CHECK(strcmp(msg, "[a/b/c] READER CONNECTION: refused") == 0);
            sp_reader_poll(r, SP_FOUND_TICKET, k, NULL, T0 + 2, 302, 30, &st);
            CHECK(st.action == SP_DO_CONNECT && !st.fresh_ring);     /* the same run */
        }
        sp_reader_connected(r);
        sp_reader_lost(r, NULL, 303, &st);
        CHECK(st.wait_seconds == 0.5 && sp_reader_message(r, msg, sizeof msg) == 0);

        /* the ticket goes stale: wait, a line; a new wait, so a new warning */
        sp_reader_poll(r, SP_FOUND_TICKET, k, NULL, T0 + 45, 400, 30, &st);
        CHECK(st.action == SP_DO_WAIT && st.state == SP_R_WAITING);
        CHECK(sp_reader_message(r, msg, sizeof msg) == SP_LOG_INFO);
        CHECK(strstr(msg, "ticket stale: the writer at 10.0.0.5:8815 last wrote it 45 s ago (stale after 45 s): waiting for a writer") != NULL);
        sp_reader_poll(r, SP_FOUND_TICKET, k, NULL, T0 + 80, 435, 30, &st);
        CHECK(sp_reader_message(r, msg, sizeof msg) == SP_LOG_WARNING);

        /* the writer started again: a fresh ring, once */
        char err[128];
        CHECK(sp_ticket_set_text(k, SP_T_RUN, "2026-11-12T21:00:00.000000Z", err, sizeof err) == 0);
        sp_reader_poll(r, SP_FOUND_TICKET, k, NULL, T0 + 1, 500, 30, &st);
        CHECK(st.action == SP_DO_CONNECT && st.fresh_ring == 1);
        CHECK(sp_reader_message(r, msg, sizeof msg) == SP_LOG_ERROR);
        CHECK(strstr(msg, "WRITER RESTARTED") && strstr(msg, "names run 2026-11-12T21:00:00.000000Z") && strstr(msg, "streamed run 2026-11-12T20:31:12.123456Z"));
        sp_reader_poll(r, SP_FOUND_TICKET, k, NULL, T0 + 1, 501, 30, &st);
        CHECK(st.action == SP_DO_CONNECT && st.fresh_ring == 0);     /* no ring yet */
        CHECK(sp_reader_message(r, msg, sizeof msg) == 0);
        sp_reader_connected(r);

        /* the store cannot be read: one error, then a line when it can */
        sp_reader_lost(r, NULL, 600, &st);
        sp_reader_poll(r, SP_FOUND_UNREADABLE, NULL, "timed out", T0, 600, 30, &st);
        CHECK(st.action == SP_DO_WAIT && st.state == SP_R_ROOT_UNREACHABLE);
        CHECK(sp_reader_message(r, msg, sizeof msg) == SP_LOG_ERROR);
        CHECK(strcmp(msg, "[a/b/c] STORE UNREADABLE: /data: timed out") == 0);
        sp_reader_poll(r, SP_FOUND_UNREADABLE, NULL, "timed out", T0, 601, 30, &st);
        CHECK(sp_reader_message(r, msg, sizeof msg) == 0);
        CHECK(strcmp(sp_reader_reason(r), "store /data unreadable: timed out") == 0);
        sp_reader_poll(r, SP_FOUND_UNREADABLE, NULL, "timed out", T0, 631, 30, &st);
        CHECK(sp_reader_message(r, msg, sizeof msg) == SP_LOG_WARNING);
        sp_reader_poll(r, SP_FOUND_TICKET, k, NULL, T0 + 1, 632, 30, &st);
        CHECK(st.action == SP_DO_CONNECT && st.fresh_ring == 0);
        CHECK(sp_reader_message(r, msg, sizeof msg) == SP_LOG_INFO);
        CHECK(strcmp(msg, "[a/b/c] store readable again: /data") == 0);
        sp_reader_connected(r);

        /* a local stream wins: the ring in hand is dropped; it closes: poll again */
        sp_reader_lost(r, NULL, 700, &st);
        sp_reader_poll(r, SP_FOUND_LOCAL, NULL, NULL, T0, 700, 30, &st);
        CHECK(st.action == SP_DO_LOCAL && st.state == SP_R_STREAMING && st.fresh_ring == 1);
        CHECK(sp_reader_run(r) == NULL && sp_reader_reason(r)[0] == 0);
        sp_reader_lost(r, NULL, 701, &st);
        CHECK(st.action == SP_DO_WAIT && st.wait_seconds == 0 && st.fresh_ring == 1);
        CHECK(strcmp(sp_reader_reason(r), "the stream a/b/c of this process closed") == 0);
        sp_reader_poll(r, SP_FOUND_TICKET, k, NULL, T0 + 1, 702, 30, &st);
        CHECK(st.action == SP_DO_CONNECT && st.fresh_ring == 0);
        CHECK(sp_reader_message(r, msg, sizeof msg) == 0);

        /* closed: every step is a wait in the closed state */
        sp_reader_close(r);
        sp_reader_poll(r, SP_FOUND_TICKET, k, NULL, T0, 800, 30, &st);
        CHECK(st.action == SP_DO_WAIT && st.state == SP_R_CLOSED);
        sp_reader_lost(r, "x", 800, &st);
        CHECK(st.state == SP_R_CLOSED);
        sp_reader_folder_is(r, 0, 800, &st);
        CHECK(st.state == SP_R_CLOSED && st.action == SP_DO_WAIT);
        sp_reader_connected(r);
        CHECK(sp_reader_state(r) == SP_R_CLOSED);
        sp_reader_free(r);

        /* an unreadable store at the first poll says nothing of the folder */
        r = sp_reader_new("connect('a/b/c')", "a/b/c", "/data", 0);
        sp_reader_poll(r, SP_FOUND_UNREADABLE, NULL, "down", T0, 0, 30, &st);
        CHECK(st.action == SP_DO_WAIT);
        sp_reader_poll(r, SP_FOUND_NONE, NULL, NULL, T0, 1, 30, &st);
        CHECK(st.action == SP_DO_LIST);
        sp_reader_free(r);

        /* a name with no folder waits at once */
        r = sp_reader_new(NULL, "events", "s3://b", 0);
        CHECK(sp_reader_folder(r)[0] == 0);
        sp_reader_poll(r, SP_FOUND_NONE, NULL, NULL, T0, 0, 30, &st);
        CHECK(st.action == SP_DO_WAIT);
        sp_reader_free(r);

        /* a direct ticket is never stale; a ticket with no ts is not stale */
        r = sp_reader_new("connect(ticket=...)", "a/b/c", NULL, 1);
        CHECK(sp_reader_state(r) == SP_R_CONNECTING);
        sp_reader_poll(r, SP_FOUND_TICKET, k, NULL, T0 + 1e6, 0, 30, &st);
        CHECK(st.action == SP_DO_CONNECT);
        sp_reader_free(r);
        r = sp_reader_new("connect('x')", "x", "/data", 0);
        sp_ticket_clear(k, SP_T_TS);
        sp_reader_poll(r, SP_FOUND_TICKET, k, NULL, T0 + 1e6, 0, 30, &st);
        CHECK(st.action == SP_DO_CONNECT);
        /* a ticket with no run after one with a run: the writer started again */
        sp_reader_connected(r);
        sp_ticket_clear(k, SP_T_RUN);
        sp_reader_poll(r, SP_FOUND_TICKET, k, NULL, T0, 0, 30, &st);
        CHECK(st.fresh_ring == 1 && sp_reader_run(r) == NULL);
        while (sp_reader_message(r, msg, sizeof msg)) {}
        /* more lines than the reader keeps: the newest stay, nothing breaks */
        for (int i = 0; i < 20; i++) sp_reader_lost(r, "again", 0, &st);
        int n = 0;
        while (sp_reader_message(r, msg, sizeof msg)) n++;
        CHECK(n == 4);
        /* a very long reason is cut, not overrun */
        char big[4000];
        memset(big, 'x', sizeof big - 1);
        big[sizeof big - 1] = 0;
        sp_reader_poll(r, SP_FOUND_UNREADABLE, NULL, big, T0, 0, 30, &st);
        CHECK(sp_reader_message(r, msg, sizeof msg) == SP_LOG_ERROR && strlen(msg) < sizeof msg);
        CHECK(sp_reader_message(r, NULL, 0) == 0);
        sp_reader_free(r);

        /* NULL is taken everywhere */
        CHECK(sp_reader_new("w", NULL, "/d", 0) == NULL);
        sp_reader_poll(NULL, SP_FOUND_NONE, NULL, NULL, 0, 0, 30, &st);
        sp_reader_poll(NULL, SP_FOUND_NONE, NULL, NULL, 0, 0, 30, NULL);
        sp_reader_folder_is(NULL, 1, 0, &st);
        sp_reader_lost(NULL, NULL, 0, &st);
        sp_reader_connected(NULL);
        sp_reader_close(NULL);
        sp_reader_free(NULL);
        CHECK(sp_reader_state(NULL) == SP_R_CLOSED && sp_reader_run(NULL) == NULL);
        CHECK(sp_reader_reason(NULL)[0] == 0 && sp_reader_folder(NULL)[0] == 0);
        CHECK(sp_reader_message(NULL, msg, sizeof msg) == 0);
        r = sp_reader_new("w", "a/b", "/d", 0);
        sp_reader_poll(r, SP_FOUND_TICKET, NULL, NULL, 0, 0, 30, NULL);   /* no ticket, no step */
        sp_reader_lost(r, NULL, 0, NULL);
        sp_reader_folder_is(r, 1, 0, NULL);
        sp_reader_free(r);
        sp_ticket_free(k);
    }

    /* ---- the view ---- */
    {
        char err[256], msg[2048];
        int use, why;
        int64_t full[3] = {600, 999, 400}, none[3] = {0, -1, 400};
        SpTicket *t = sp_ticket_new();
        int64_t first = 500, final = 250, rows = 480;
        sp_ticket_set_text(t, SP_T_RUN, "r1", err, sizeof err);
        sp_ticket_set_integer(t, SP_T_REVISION, 3, err, sizeof err);
        sp_ticket_set_number(t, SP_T_TS, 1000.0, err, sizeof err);
        sp_ticket_set_number(t, SP_T_STALE_AFTER, 45.0, err, sizeof err);
        sp_ticket_set_number(t, SP_T_BEAT_SECONDS, 10.0, err, sizeof err);
        sp_ticket_set_number(t, SP_T_KEEP_SECONDS, 100.0, err, sizeof err);
        sp_ticket_set_integer(t, SP_T_STORED_ROW, 990, err, sizeof err);
        sp_ticket_set_text(t, SP_T_CATALOG_ETAG, "e1", err, sizeof err);
        sp_ticket_add_file(t, "/d/a.parquet.000003.ckpt", &first, &final, &rows,
                           err, sizeof err);

        /* the seam */
        CHECK(sp_view_seam(t, NULL, &use, &why) == 990 && !use && why == SP_SEAM_AS_WANTED);
        CHECK(sp_view_seam(t, none, &use, &why) == 990 && use);
        CHECK(sp_view_seam(t, full, &use, &why) == 749 && use && why == SP_SEAM_AS_WANTED);
        CHECK(sp_view_seam(NULL, full, &use, &why) == -1 && !use && why == SP_SEAM_SHORT);
        CHECK(sp_view_seam(NULL, NULL, NULL, NULL) == -1);
        {
            SpTicket *k = sp_ticket_new();
            sp_ticket_set_integer(k, SP_T_STORED_ROW, 650, err, sizeof err);
            CHECK(sp_view_seam(k, full, &use, &why) == 650 && use && why == SP_SEAM_TIGHT);
            sp_ticket_set_integer(k, SP_T_STORED_ROW, 500, err, sizeof err);
            CHECK(sp_view_seam(k, full, &use, &why) == 500 && !use && why == SP_SEAM_SHORT);
            int64_t part[3] = {600, 700, 400};
            CHECK(sp_view_seam(k, part, &use, &why) == 500 && !use && why == SP_SEAM_LATE);
            sp_ticket_free(k);
        }

        /* a view with a ring, of the ticket's run */
        const char *files[1] = {"/d/a.parquet"};
        SpView *v = sp_view_new("sp_x", "a/b", "__mem", "__empty");
        SpViewPlan p;
        CHECK(sp_view_seam_row(v) == -1);
        CHECK(sp_view_plan(v, t, files, "r1", full, 1001, 50, 30, 600, &p) == 0);
        CHECK(p.build && p.catalog == SP_CATALOG_EXACT && p.seam == 749 && p.use_ring);
        CHECK(strcmp(sp_view_catalog_etag(v), "e1") == 0);
        char *sql = sp_view_sql(v, "cat1", err, sizeof err);
        CHECK(sql && strstr(sql, "CREATE OR REPLACE VIEW \"sp_x\" AS (SELECT * FROM \"cat1\".data)"
                                 " UNION ALL BY NAME (SELECT * EXCLUDE (file_row_number) FROM "
                                 "read_parquet('pqc:///d/a.parquet.000003.ckpt', file_row_number=true) "
                                 "WHERE file_row_number <= 249) UNION ALL BY NAME "
                                 "(SELECT * EXCLUDE (_seq, _first) FROM \"__mem\" WHERE _seq > 749 "
                                 "AND CASE WHEN _first > 750 THEN error('sp_x: the ring does not hold "
                                 "row 750; it begins at row ' || _first || '. This query read the rows "
                                 "up to 749 from the store.") == sql);
        sp_free(sql);
        sp_view_built(v);
        CHECK(sp_view_seam_row(v) == 749);
        /* the next write is due at ts + beat + margin */
        CHECK(sp_view_wait_seconds(v, 1001) == 10.0);
        CHECK(sp_view_wait_seconds(v, 1010.5) == 0.5);
        CHECK(sp_view_wait_seconds(v, 900) == 11.0);

        /* the same facts: nothing to do */
        CHECK(sp_view_plan(v, t, files, "r1", full, 1002, 51, 30, 600, &p) == 0 && !p.build);

        /* a newer checkpoint version, the seam where it was: the text
         * keeps the version it names for half the keep time */
        const char *text2 = "{\"run\":\"r1\",\"revision\":4,\"ts\":1003,"
            "\"stale_after\":45,\"beat_seconds\":10,\"keep_seconds\":100,\"stored_row\":995,"
            "\"catalog_etag\":\"e1\",\"files\":[{\"checkpoint\":\"/d/a.parquet.000004.ckpt\","
            "\"first_row\":500,\"final_rows\":250,\"rows\":496}]}";
        SpTicket *t2 = sp_ticket_read(text2, strlen(text2), err, sizeof err);
        CHECK(t2 != NULL);
        CHECK(sp_view_plan(v, t2, files, "r1", full, 1003, 52, 30, 600, &p) == 0 && !p.build);
        CHECK(sp_view_plan(v, t2, files, "r1", full, 1003, 101.9, 30, 600, &p) == 0 && !p.build);
        CHECK(sp_view_plan(v, t2, files, "r1", full, 1003, 102, 30, 600, &p) == 0 && p.build);
        sql = sp_view_sql(v, "cat1", err, sizeof err);
        CHECK(sql && strstr(sql, "a.parquet.000004.ckpt") && !strstr(sql, "000003"));
        sp_free(sql);
        sp_view_built(v);
        /* the seam moves: the newest version at once */
        int64_t later[3] = {900, 1299, 400};
        CHECK(sp_view_plan(v, t, files, "r1", later, 1003, 103, 30, 600, &p) == 0);
        CHECK(p.build && p.seam == 990 && p.use_ring);
        sql = sp_view_sql(v, "cat1", err, sizeof err);
        CHECK(sql && strstr(sql, "000003.ckpt") && strstr(sql, "file_row_number <= 490"));
        sp_free(sql);

        /* another run in the ring: no ring piece, the seam at stored_row */
        CHECK(sp_view_plan(v, t, files, "r0", full, 1003, 104, 30, 600, &p) == 0);
        CHECK(p.build && p.seam == 990 && !p.use_ring);
        CHECK(sp_view_plan(v, t, files, NULL, full, 1003, 104, 30, 600, &p) == 0 && !p.use_ring);

        /* a stale ticket: the store's current catalog, built at every look */
        CHECK(sp_view_plan(v, t, files, "r1", full, 1045, 105, 30, 600, &p) == 0);
        CHECK(p.build && p.catalog == SP_CATALOG_CURRENT && !p.use_ring
              && sp_view_catalog_etag(v) == NULL);
        sp_view_built(v);
        CHECK(sp_view_plan(v, t, files, "r1", full, 1046, 106, 30, 600, &p) == 0 && p.build);
        CHECK(sp_view_wait_seconds(v, 1046) == SP_VIEW_POLL_DEFAULT_SECONDS);

        /* no ticket: the catalog alone, or the empty relation */
        CHECK(sp_view_plan(v, NULL, NULL, "r1", full, 1046, 107, 30, 600, &p) == 0);
        CHECK(p.build && p.catalog == SP_CATALOG_CURRENT && p.seam == -1);
        sql = sp_view_sql(v, NULL, err, sizeof err);
        CHECK(sql && strcmp(sql, "CREATE OR REPLACE VIEW \"sp_x\" AS (SELECT * FROM \"__empty\")") == 0);
        sp_free(sql);
        sql = sp_view_sql(v, "c", err, sizeof err);
        CHECK(sql && strcmp(sql, "CREATE OR REPLACE VIEW \"sp_x\" AS (SELECT * FROM \"c\".data)") == 0);
        sp_free(sql);
        CHECK(sp_view_wait_seconds(v, 1046) == SP_VIEW_POLL_DEFAULT_SECONDS);
        while (sp_view_message(v, msg, sizeof msg)) {}

        /* the ring too short: said once, and once when it ends */
        sp_ticket_set_integer(t, SP_T_STORED_ROW, 10, err, sizeof err);
        int64_t shortw[3] = {200, 599, 400}, good[3] = {0, 20, 400};
        sp_view_plan(v, t, files, "r1", shortw, 1001, 108, 30, 600, &p);
        CHECK(!p.use_ring && strstr(sp_view_ring_note(v), "lag of 589 rows")
              && strstr(sp_view_ring_note(v), "lookback=786"));
        CHECK(sp_view_message(v, msg, sizeof msg) == SP_LOG_WARNING
              && strstr(msg, "[a/b] view sp_x: the ring holds rows 200 to 599") == msg);
        sp_view_plan(v, t, files, "r1", shortw, 1001, 109, 30, 600, &p);
        CHECK(sp_view_message(v, msg, sizeof msg) == 0);
        sp_view_plan(v, t, files, "r1", good, 1001, 110, 30, 600, &p);
        CHECK(sp_view_ring_note(v)[0] == 0 && p.use_ring);
        CHECK(sp_view_message(v, msg, sizeof msg) == SP_LOG_INFO
              && strstr(msg, "the ring is in use again"));

        /* a look that fails is said once; then that it works again */
        CHECK(sp_view_failed(v, "no route") == SP_VIEW_POLL_MIN_SECONDS);
        sp_view_failed(v, "no route");
        CHECK(sp_view_message(v, msg, sizeof msg) == SP_LOG_ERROR
              && strcmp(msg, "[a/b] view sp_x could not follow the ticket: no route") == 0);
        CHECK(sp_view_message(v, msg, sizeof msg) == 0);
        sp_view_followed(v);
        sp_view_followed(v);
        CHECK(sp_view_message(v, msg, sizeof msg) == SP_LOG_INFO
              && strcmp(msg, "[a/b] view sp_x current again") == 0);
        CHECK(sp_view_message(v, msg, sizeof msg) == 0);
        sp_view_free(v);

        /* no ring, columns not known: nothing to read is an error; a
         * quote in a path is doubled; a file of unknown place is read whole */
        v = sp_view_new("v", "it's", NULL, NULL);
        CHECK(sp_view_plan(v, NULL, NULL, NULL, NULL, 0, 0, 30, 600, &p) == 0 && p.build);
        CHECK(sp_view_sql(v, NULL, err, sizeof err) == NULL
              && strcmp(err, "v: 'it's' has no catalog, no unfinished file and no memory "
                             "yet; nothing to read the columns from") == 0);
        SpTicket *q = sp_ticket_new();
        sp_ticket_set_integer(q, SP_T_REVISION, 1, err, sizeof err);
        sp_ticket_set_number(q, SP_T_TS, 1000.0, err, sizeof err);
        sp_ticket_add_file(q, "/d/o'x.parquet.000001.ckpt", NULL, NULL, NULL, err, sizeof err);
        CHECK(sp_view_plan(v, q, NULL, "r1", full, 1001, 1, 30, 600, &p) == 0);
        CHECK(p.build && p.catalog == SP_CATALOG_NONE && !p.use_ring && p.seam == -1);
        sql = sp_view_sql(v, NULL, err, sizeof err);
        CHECK(sql && strcmp(sql, "CREATE OR REPLACE VIEW \"v\" AS (SELECT * FROM "
                                 "read_parquet('pqc:///d/o''x.parquet.000001.ckpt'))") == 0);
        sp_free(sql);
        sp_view_free(v);
        sp_ticket_free(q);

        /* NULL is taken everywhere */
        CHECK(sp_view_new(NULL, "s", NULL, NULL) == NULL && sp_view_new("v", NULL, NULL, NULL) == NULL);
        CHECK(sp_view_plan(NULL, t, files, "r1", full, 0, 0, 30, 600, &p) == -1);
        CHECK(sp_view_sql(NULL, NULL, err, sizeof err) == NULL);
        CHECK(sp_view_catalog_etag(NULL) == NULL && sp_view_seam_row(NULL) == -1);
        CHECK(sp_view_ring_note(NULL)[0] == 0 && sp_view_message(NULL, msg, sizeof msg) == 0);
        CHECK(sp_view_wait_seconds(NULL, 0) == SP_VIEW_POLL_DEFAULT_SECONDS);
        sp_view_failed(NULL, NULL);
        sp_view_followed(NULL);
        sp_view_built(NULL);
        sp_view_free(NULL);
        v = sp_view_new("v", "s", "m", NULL);
        CHECK(sp_view_plan(v, t, NULL, "r1", full, 1001, 0, 30, 600, NULL) == 0);
        sp_view_failed(v, NULL);
        sp_view_free(v);
        sp_ticket_free(t2);
        sp_ticket_free(t);
    }

    if (failures == 0) printf("san_test: all checks passed\n");
    return failures ? 1 : 0;
}
