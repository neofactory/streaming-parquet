/* san_test.c — pure-C memory-safety soak for the pqc core.
 *
 * Build+run: make san   (ASan+UBSan)   /   make leaks   (macOS leaks)
 *
 * No parquet library in C, so the "mini files" are hand-crafted:
 * structurally valid FileMetaData thrift (num_rows, row_groups with
 * columns, offsets on the real offset path, plus junk fields of every
 * compact type to exercise the verbatim-copy paths), wrapped in
 * PAR1 ... PAR1. Content is never decoded as parquet here; the format
 * parity with real files is run_c_compat.py's job. This harness
 * hammers the allocation, recursion, and bounds paths.
 */

#include "pqc.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ---- tiny thrift builder ---------------------------------------------- */

typedef struct { uint8_t b[1 << 16]; size_t n; } TB;

static void put(TB *t, uint8_t v) { assert(t->n < sizeof(t->b)); t->b[t->n++] = v; }

static void varint(TB *t, uint64_t v) {
    while (v > 0x7F) { put(t, (uint8_t)((v & 0x7F) | 0x80)); v >>= 7; }
    put(t, (uint8_t)v);
}

static void zigzag(TB *t, int64_t v) {
    varint(t, ((uint64_t)v << 1) ^ (uint64_t)(v >> 63));
}

static void fhead(TB *t, int delta, int type) {
    put(t, (uint8_t)((delta << 4) | type));
}

static void binary(TB *t, const char *s) {
    size_t n = strlen(s);
    varint(t, n);
    memcpy(t->b + t->n, s, n);
    t->n += n;
}

/* ColumnMetaData: junk + data_page_offset(9) + dictionary(11) */
static void col_meta(TB *t, int64_t off) {
    fhead(t, 1, 5); zigzag(t, 7);              /* f1 type: i32          */
    fhead(t, 1, 9); put(t, 0x25);              /* f2 encodings: list i32 x2 */
    zigzag(t, 0); zigzag(t, 4);
    fhead(t, 2, 6); zigzag(t, 999);            /* f4 num_values         */
    fhead(t, 5, 6); zigzag(t, off);            /* f9 data_page_offset   */
    fhead(t, 2, 6); zigzag(t, off - 16);       /* f11 dictionary_page   */
    put(t, 0);
}

static void column_chunk(TB *t, int64_t off) {
    fhead(t, 2, 6); zigzag(t, off);            /* f2 file_offset        */
    fhead(t, 1, 12); col_meta(t, off);         /* f3 meta_data          */
    fhead(t, 1, 6); zigzag(t, off + 100);      /* f4 offset_index (fake)*/
    put(t, 0);
}

static void row_group(TB *t, int64_t base, int ncols, int64_t nbytes) {
    fhead(t, 1, 9);                            /* f1 columns            */
    put(t, (uint8_t)((ncols << 4) | 12));
    for (int c = 0; c < ncols; c++)
        column_chunk(t, base + c * 64 + 16);
    fhead(t, 1, 6); zigzag(t, nbytes);         /* f2 total_byte_size    */
    fhead(t, 1, 6); zigzag(t, 42);             /* f3 num_rows           */
    fhead(t, 2, 6); zigzag(t, base);           /* f5 file_offset        */
    put(t, 0);
}

/* FileMetaData with num_rows, row_groups, and junk of many types. */
static size_t footer(uint8_t *out, int nrg, int ncols, int64_t rows,
                     int64_t base) {
    TB t = {0};
    fhead(&t, 1, 5); zigzag(&t, 2);            /* f1 version            */
    fhead(&t, 1, 9);                           /* f2 schema: list<struct> */
    put(&t, 0x1C);
    fhead(&t, 4, 8); binary(&t, "root");       /* SchemaElement.name    */
    put(&t, 0);
    fhead(&t, 1, 6); zigzag(&t, rows);         /* f3 num_rows           */
    fhead(&t, 1, 9);                           /* f4 row_groups         */
    if (nrg < 15) put(&t, (uint8_t)((nrg << 4) | 12));
    else { put(&t, 0xFC); varint(&t, (uint64_t)nrg); }
    for (int g = 0; g < nrg; g++)
        row_group(&t, base + g * 1000, ncols, 900);
    fhead(&t, 1, 9);                           /* f5 key_value: junk    */
    put(&t, 0x1C);
    fhead(&t, 1, 8); binary(&t, "created");
    fhead(&t, 1, 8); binary(&t, "san_test");
    put(&t, 0);
    fhead(&t, 1, 8); binary(&t, "pqc-san");    /* f6 created_by         */
    fhead(&t, 1, 1);                           /* bool-true field       */
    fhead(&t, 1, 7);                           /* double field          */
    for (int i = 0; i < 8; i++) put(&t, (uint8_t)i);
    put(&t, 0);
    memcpy(out, t.b, t.n);
    return t.n;
}

/* Complete mini file: PAR1 + pages + footer + len + PAR1 */
static size_t mini_file(uint8_t *out, size_t pages, int nrg, int ncols,
                        int64_t rows) {
    size_t n = 0;
    memcpy(out, "PAR1", 4); n = 4;
    for (size_t i = 0; i < pages; i++) out[n++] = (uint8_t)(i * 7);
    uint8_t f[1 << 15];
    size_t flen = footer(f, nrg, ncols, rows, 4);
    memcpy(out + n, f, flen); n += flen;
    out[n++] = (uint8_t)flen; out[n++] = (uint8_t)(flen >> 8);
    out[n++] = (uint8_t)(flen >> 16); out[n++] = (uint8_t)(flen >> 24);
    memcpy(out + n, "PAR1", 4); n += 4;
    return n;
}

static uint32_t rng_state = 0xC0FFEE1u;
static uint32_t rnd(void) {
    rng_state = rng_state * 1664525u + 1013904223u;
    return rng_state >> 8;
}

int main(void) {
    char err[512];
    uint8_t mini[1 << 16];
    uint8_t *store[2048];       /* pretend sink for object bodies */
    int n_store = 0;

    /* a dead writer: three beats, at least 30 seconds */
    assert(pqc_dead_after(0.0) == 30.0);
    assert(pqc_dead_after(5.0) == 30.0);
    assert(pqc_dead_after(15.0) == 45.0);
    assert(pqc_dead_after(60.0) == 180.0);
    assert(pqc_dead_after(-1.0) == 30.0);

    /* shift round: idempotence-ish (shift by d then -d = original) */
    {
        size_t n = mini_file(mini, 2000, 3, 4, 126);
        uint32_t flen = (uint32_t)mini[n - 8] | ((uint32_t)mini[n - 7] << 8)
                      | ((uint32_t)mini[n - 6] << 16)
                      | ((uint32_t)mini[n - 5] << 24);
        const uint8_t *th = mini + n - 8 - flen;
        /* shift is canonicalize+drop on first application; after that,
         * shifting by d then -d must reproduce the delta-0 form */
        size_t o0, o1, o2;
        uint8_t *s0 = pqc_shift_footer(th, flen, 0, &o0, err, 512);
        assert(s0);
        uint8_t *s1 = pqc_shift_footer(th, flen, 5000, &o1, err, 512);
        assert(s1);
        uint8_t *s2 = pqc_shift_footer(s1, o1, -5000, &o2, err, 512);
        assert(s2);
        assert(o2 == o0 && memcmp(s2, s0, o0) == 0);
        pqc_free(s0);
        pqc_free(s1);
        pqc_free(s2);
    }

    /* writer soak: many lifecycles, random sizes, chunk drains */
    for (int iter = 0; iter < 300; iter++) {
        PqcWriter *w = pqc_writer_new("soak.parquet",
                                      1024 + rnd() % 8192, err, 512);
        assert(w);
        char name[PQC_NAME_MAX];
        const uint8_t *data;
        uint64_t len;
        uint32_t crc;
        int rc;
        int groups = 1 + (int)(rnd() % 12);
        for (int g = 0; g < groups; g++) {
            size_t pages = 200 + rnd() % 4000;
            int nrg = 1 + (int)(rnd() % 3);
            size_t n = mini_file(mini, pages, nrg, 1 + (int)(rnd() % 6),
                                 (int64_t)(rnd() % 500));
            int64_t rows = pqc_add_group(w, mini, n, err, 512);
            assert(rows >= 0);
            /* a checkpoint may not claim what is not stored */
            if (rnd() % 5 == 0) {
                uint64_t cl0, wm0;
                assert(pqc_build_checkpoint(w, NULL, 0, &cl0, &wm0,
                                            err, 512) == NULL);
            }
            /* drain */
            while ((rc = pqc_next_object(w, name, &data, &len, &crc)) == 1) {
                assert(pqc_crc32(data, len) == crc);
                if (rnd() % 4 == 0) {          /* failed store: retry   */
                    const uint8_t *d2;
                    uint64_t l2;
                    uint32_t c2;
                    assert(pqc_next_object(w, name, &d2, &l2, &c2) == 1);
                    assert(l2 == len && c2 == crc);
                }
                assert(n_store < 2048);
                store[n_store] = malloc(len);
                memcpy(store[n_store++], data, len);
                pqc_object_stored(w);
            }
            assert(rc == 0);
            assert(pqc_rows_unfinished(w) == 0);
            /* checkpoints of the unfinished row group: each one's new
             * rows as an object, merged in steps */
            int ckpts = (int)(rnd() % 40);
            uint64_t unfinished = 0;
            for (int c = 0; c < ckpts; c++) {
                uint64_t fresh = 1 + rnd() % 30, first, span;
                pqc_next_span(w, fresh, &first, &span);
                unfinished += fresh;
                assert(first >= pqc_rows_final(w));
                assert(first + span == pqc_rows_final(w) + unfinished);
                size_t tn = mini_file(mini, 100 + rnd() % 500, 1, 2,
                                      (int64_t)span);
                if (rnd() % 7 == 0) {          /* the wrong rows: refused */
                    size_t bad = mini_file(mini, 100, 1, 2,
                                           (int64_t)span + 1);
                    assert(pqc_add_unfinished(w, mini, bad, err, 512) < 0);
                    tn = mini_file(mini, 100 + rnd() % 500, 1, 2,
                                   (int64_t)span);
                }
                assert(pqc_add_unfinished(w, mini, tn, err, 512)
                       == (int64_t)span);
                uint64_t cl, wm;
                assert(pqc_build_checkpoint(w, NULL, 0, &cl, &wm, err, 512)
                       == NULL);               /* its object is not stored */
                int stored = 0;
                while ((rc = pqc_next_object(w, name, &data, &len, &crc))
                       == 1) {
                    assert(pqc_crc32(data, len) == crc);
                    assert(n_store < 2048);
                    store[n_store] = malloc(len);
                    memcpy(store[n_store++], data, len);
                    pqc_object_stored(w);
                    stored++;
                }
                assert(rc == 0 && stored == 1);
                assert(pqc_rows_unfinished(w) == unfinished);
                uint8_t *ck = pqc_build_checkpoint(w, NULL, 0, &cl, &wm,
                                                   err, 512);
                assert(ck);
                assert(wm == pqc_rows_final(w) + unfinished);
                PqcReader *r = pqc_reader_open(ck, cl, err, 512);
                assert(r);
                assert(strcmp(pqc_reader_name(r), "soak.parquet") == 0);
                assert(pqc_reader_n_chunks(r) >= pqc_chunks_cut(w) + 1);
                uint64_t off, dl;
                pqc_reader_data(r, &off, &dl);
                assert(off + dl == cl);
                /* corrupt copy must refuse */
                ck[cl / 2] ^= 0xFF;
                assert(pqc_reader_open(ck, cl, err, 512) == NULL);
                pqc_reader_free(r);
                pqc_free(ck);
            }
            if (rnd() % 6 == 0) {              /* claim the final rows only */
                pqc_drop_unfinished(w);
                assert(pqc_rows_unfinished(w) == 0);
            }
        }
        pqc_drop_unfinished(w);
        uint64_t cl, wm;
        uint8_t *ck = pqc_build_checkpoint(w, NULL, 0, &cl, &wm, err, 512);
        assert(ck && wm == pqc_rows_final(w));
        pqc_free(ck);
        pqc_writer_free(w);
        while (n_store) free(store[--n_store]);
    }

    /* refusals */
    assert(pqc_writer_new("nope.txt", 1024, err, 512) == NULL);
    {
        PqcWriter *w = pqc_writer_new("e.parquet", 1024, err, 512);
        uint64_t cl, wm;
        assert(pqc_build_checkpoint(w, NULL, 0, &cl, &wm, err, 512)
               == NULL);                        /* no rows: needs a mini */
        pqc_writer_free(w);
    }
    printf("san_test: OK\n");
    return 0;
}
