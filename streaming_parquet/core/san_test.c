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

    if (failures == 0) printf("san_test: all checks passed\n");
    return failures ? 1 : 0;
}
