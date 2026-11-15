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

    if (failures == 0) printf("san_test: all checks passed\n");
    return failures ? 1 : 0;
}
