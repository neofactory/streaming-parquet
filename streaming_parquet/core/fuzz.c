/* fuzz.c -- the ticket reader against text it did not write. `make
 * fuzz`: real tickets, mutated, each read under the sanitizers. A
 * ticket that is read is written and read again, and must come back
 * the same text. */
#include "sp.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static uint64_t state = 0x9E3779B97F4A7C15ULL;
static uint32_t rnd(void) {
    state ^= state << 13; state ^= state >> 7; state ^= state << 17;
    return (uint32_t)(state >> 16);
}

static const char *SEEDS[] = {
    ("{\"name\": \"plant/line-1/press\", \"host\": \"10.0.0.5\", \"port\": 8815, "
    "\"run\": \"2026-11-12T20:31:12.123456Z\", \"beat_seconds\": 15, "
    "\"stale_after\": 45, \"checkpoint_seconds\": 0.1, \"ship_seconds\": null, "
    "\"stored_row\": 12345, \"stored_at\": 1762979472.123456, "
    "\"unfinished\": [\"s3://b/streams/x/a.parquet.000003.ckpt\"], "
    "\"files\": [{\"checkpoint\": \"s3://b/streams/x/a.parquet.000003.ckpt\", "
    "\"first_row\": 1000, \"final_rows\": 800, \"rows\": null}], "
    "\"keep_seconds\": 600, \"catalog_etag\": \"\\\"abc-1\\\"\", \"revision\": 7, "
    "\"ts\": 1762979473.5}"),
    ("{\"name\":\"air/dfw\",\"host\":\"h\",\"port\":1,\"ts\":1.5e9,"
     "\"x\":{\"a\":[1,true,null,\"\\u00e9\\ud83c\\udfaf\"]},\"archive\":\"s3:\\/\\/b\"}"),
    "{}",
};

int main(int argc, char **argv) {
    long iters = argc > 1 ? atol(argv[1]) : 300000;
    static const char JUNK[] = "{}[]\",:\\ue0123456789.-+ntfa\x00\xff\x80";
    long read_ok = 0;
    for (long i = 0; i < iters; i++) {
        const char *seed = SEEDS[rnd() % (sizeof SEEDS / sizeof SEEDS[0])];
        size_t n = strlen(seed);
        char *buf = malloc(n + 16);
        memcpy(buf, seed, n);
        int edits = 1 + (int)(rnd() % 4);
        for (int e = 0; e < edits && n > 0; e++) {
            size_t at = rnd() % n;
            switch (rnd() % 4) {
            case 0: buf[at] = JUNK[rnd() % (sizeof JUNK - 1)]; break;
            case 1: buf[at] = (char)rnd(); break;
            case 2: memmove(buf + at, buf + at + 1, n - at - 1); n--; break;
            default: n = at; break;                      /* cut short */
            }
        }
        /* an exact-size copy, so a read past the end is caught */
        char *exact = malloc(n ? n : 1);
        memcpy(exact, buf, n);
        char err[128];
        SpTicket *t = sp_ticket_read(exact, n, err, sizeof err);
        if (t) {
            read_ok++;
            size_t len;
            char *text = sp_ticket_write(t, &len);
            SpTicket *back = sp_ticket_read(text, len, err, sizeof err);
            if (!back) {
                fprintf(stderr, "FAIL: wrote a ticket it cannot read: %s\n%s\n", err, text);
                return 1;
            }
            char *again = sp_ticket_write(back, NULL);
            if (strcmp(text, again) != 0) {
                fprintf(stderr, "FAIL: a ticket changed on the way round:\n%s\n%s\n", text, again);
                return 1;
            }
            sp_free(again);
            sp_ticket_free(back);
            sp_free(text);
        }
        sp_ticket_free(t);
        free(exact);
        free(buf);
    }
    printf("fuzz: %ld texts, %ld read as tickets, no fault\n", iters, read_ok);
    return 0;
}
