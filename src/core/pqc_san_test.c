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

#include "pqc_test_calls.h"

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

/* ---- checkpoint versions, the upload, the sweep --------------------------- */

static Report R;                 /* for the calls with no helper */

/* A pretend copy of a file's objects: names and sizes. */
typedef struct { char name[PQC_OBJECT_MAX]; uint64_t size; } Obj;
typedef struct { Obj o[512]; int n; } Sink;

static int sink_find(const Sink *s, const char *name) {
    for (int i = 0; i < s->n; i++)
        if (strcmp(s->o[i].name, name) == 0) return i;
    return -1;
}
static void sink_put(Sink *s, const char *name, uint64_t size) {
    int i = sink_find(s, name);
    if (i < 0) { assert(s->n < 512); i = s->n++; }
    snprintf(s->o[i].name, sizeof s->o[i].name, "%s", name);
    s->o[i].size = size;
}
static void sink_delete(Sink *s, const char *name) {
    int i = sink_find(s, name);
    assert(i >= 0);
    s->o[i] = s->o[--s->n];
}

/* A writer of "f.parquet" whose objects go to `local`; each call adds
 * row groups and answers a new checkpoint version: its object name and
 * bytes (free them). */
typedef struct { PqcWriter *w; Sink *local; int seq; } Source;

static uint8_t *next_version(Source *src, int groups, char object[PQC_OBJECT_MAX],
                             uint64_t *len, uint64_t *rows) {
    static uint8_t mini[1 << 16];
    char name[PQC_NAME_MAX], err[512];
    const uint8_t *data;
    uint64_t n;
    uint32_t crc;
    for (int g = 0; g < groups; g++) {
        size_t m = mini_file(mini, 200 + rnd() % 900, 1, 2, 42);
        assert(t_pqc_add_group(src->w, mini, m, err, 512) >= 0);
        while (t_pqc_next_object(src->w, name, &data, &n, &crc) == 1) {
            sink_put(src->local, name, n);
            pqc_object_stored(src->w);
        }
    }
    uint8_t *ck = t_pqc_build_checkpoint(src->w, NULL, 0, len, rows, err, 512);
    assert(ck);
    assert(pqc_ckpt_object("f.parquet", ++src->seq, object, &R) == REPORT_OK);
    sink_put(src->local, object, *len);
    return ck;
}

/* What a pass did. */
typedef struct {
    int stats, uploads, versions, stored, deletes, local, remote, flag;
    int last;                    /* the action that ended it */
    uint64_t stored_rows;
    char stored_object[PQC_OBJECT_MAX];
} Pass;

/* One pass of the upload between two pretend copies. fail_at: the
 * number (from 1) of the step on the remote or the disk that fails;
 * 0 for none. Checks THE ORDER at each version sent. */
static Pass run_pass(PqcUpload *m, Sink *local, Sink *remote,
                     const char *object, const uint8_t *ck, uint64_t len,
                     int final_exists, int live, int fail_at) {
    Pass p = {0};
    PqcUploadStep st;
    int acts = 0;
    assert(pqc_upload_local(m, object, ck, len, final_exists, live, &R)
           == REPORT_OK);
    for (;;) {
        assert(pqc_upload_next(m, &st, &R) == REPORT_OK);
        p.last = st.action;
        if (st.action == PQC_UPLOAD_NOTHING || st.action == PQC_UPLOAD_DONE) {
            assert(pqc_upload_doing(m)[0] == 0);
            return p;
        }
        /* the same step again until it is answered */
        PqcUploadStep again;
        assert(pqc_upload_next(m, &again, &R) == REPORT_OK);
        assert(again.action == st.action && strcmp(again.name, st.name) == 0);
        if (st.action != PQC_UPLOAD_STORED && fail_at && ++acts == fail_at) {
            pqc_upload_failed(m);
            assert(pqc_upload_next(m, &st, &R) == REPORT_OK);
            assert(st.action == PQC_UPLOAD_NOTHING);
            p.last = -1;
            return p;
        }
        switch (st.action) {
        case PQC_UPLOAD_STAT: {
            int i = sink_find(remote, st.name);
            p.stats++;
            assert(pqc_upload_remote_has(m, i >= 0,
                                         i >= 0 ? remote->o[i].size : 0, &R)
                   == REPORT_OK);
            continue;
        }
        case PQC_UPLOAD_OBJECT: {
            int i = sink_find(local, st.name);
            assert(i >= 0 && local->o[i].size == st.size);
            assert(strstr(pqc_upload_doing(m), "uploading object"));
            sink_put(remote, st.name, st.size);
            p.uploads++;
            break;
        }
        case PQC_UPLOAD_VERSION: {
            /* THE ORDER: everything it names is there already */
            PqcReader *r = t_pqc_reader_open(ck, len, NULL, 0);
            assert(r && strcmp(st.name, object) == 0 && st.size == len);
            for (uint32_t k = 0; k < pqc_reader_n_chunks(r); k++) {
                const char *name;
                uint64_t length;
                uint32_t crc;
                pqc_reader_chunk(r, k, &name, &length, &crc);
                int i = sink_find(remote, name);
                assert(i >= 0 && remote->o[i].size == length);
            }
            pqc_reader_free(r);
            assert(strstr(pqc_upload_doing(m), "uploading checkpoint"));
            sink_put(remote, st.name, st.size);
            p.versions++;
            break;
        }
        case PQC_UPLOAD_STORED:
            p.stored++;
            p.stored_rows = st.size;
            snprintf(p.stored_object, sizeof p.stored_object, "%s", st.name);
            break;
        case PQC_UPLOAD_DELETE:
            sink_delete(remote, st.name);
            p.deletes++;
            break;
        case PQC_UPLOAD_ASSEMBLE_LOCAL:
            p.local++;
            break;
        case PQC_UPLOAD_ASSEMBLE_REMOTE:
            assert(strstr(pqc_upload_doing(m), "assembling f.parquet"));
            p.remote++;
            p.flag = st.flag;
            break;
        default:
            assert(!"an action the upload does not have");
        }
        assert(pqc_upload_did(m, &R) == REPORT_OK);
    }
}

static void version_names_tests(void) {
    char out[PQC_OBJECT_MAX], name[PQC_OBJECT_MAX];
    int64_t seq = -1;
    assert(pqc_ckpt_object("a/b.parquet", 7, out, &R) == REPORT_OK);
    assert(strcmp(out, "a/b.parquet.000007.ckpt") == 0);
    assert(pqc_ckpt_object("b.parquet", 12345678, out, &R) == REPORT_OK);
    assert(strcmp(out, "b.parquet.12345678.ckpt") == 0);
    assert(pqc_ckpt_object("b.parquet", -1, out, &R) == REPORT_INVALID);
    assert(pqc_ckpt_object(NULL, 1, out, &R) == REPORT_INVALID);
    assert(pqc_ckpt_object("b.parquet", 1, out, NULL) == REPORT_INVALID);
    char big[400];
    memset(big, 'x', sizeof big);
    memcpy(big + 380, ".parquet", 9);
    assert(pqc_ckpt_object(big, 1, out, &R) == REPORT_INVALID);

    assert(pqc_ckpt_object_parse("a/b.parquet.000007.ckpt", name, &seq) == 1);
    assert(strcmp(name, "a/b.parquet") == 0 && seq == 7);
    assert(pqc_ckpt_object_parse("b.parquet.12345678.ckpt", name, &seq) == 1);
    assert(strcmp(name, "b.parquet") == 0 && seq == 12345678);
    /* a checkpoint written before versions is version 0 */
    assert(pqc_ckpt_object_parse("b.parquet.ckpt", name, &seq) == 1);
    assert(strcmp(name, "b.parquet") == 0 && seq == 0);
    assert(pqc_ckpt_object_parse("b.parquet.ckpt", NULL, NULL) == 1);
    static const char *not_one[] = {
        "b.parquet", "b.parquet.00007.ckpt", "b.parquet.x00007.ckpt",
        "b.parquet.chunks/000001", ".parquet.ckpt", ".parquet.000001.ckpt",
        "b.txt.000001.ckpt", "000001.ckpt", ".ckpt", "", "b.parquet.000001",
        "b.parquet.0000000000000000001.ckpt", NULL};
    for (int i = 0; not_one[i]; i++)
        assert(pqc_ckpt_object_parse(not_one[i], name, &seq) == 0);
    assert(pqc_ckpt_object_parse(NULL, name, &seq) == 0);
}

/* Does version bytes ck name the object? */
static int names_it(const uint8_t *ck, uint64_t len, const char *object) {
    PqcReader *r = t_pqc_reader_open(ck, len, NULL, 0);
    int found = 0;
    for (uint32_t k = 0; r && k < pqc_reader_n_chunks(r); k++) {
        const char *name;
        uint64_t length;
        uint32_t crc;
        pqc_reader_chunk(r, k, &name, &length, &crc);
        found |= strcmp(name, object) == 0;
    }
    pqc_reader_free(r);
    return found;
}

static void versions_tests(void) {
    char err[512];
    Sink local = {0};
    Source src = {t_pqc_writer_new("f.parquet", 4096, err, 512), &local, 0};
    char o1[PQC_OBJECT_MAX], o2[PQC_OBJECT_MAX], o3[PQC_OBJECT_MAX];
    uint64_t l1, l2, l3, rows;
    uint8_t *c1 = next_version(&src, 1, o1, &l1, &rows);   /* loose groups */
    uint8_t *c2 = next_version(&src, 1, o2, &l2, &rows);
    uint8_t *c3 = next_version(&src, 9, o3, &l3, &rows);   /* chunks replace them */

    PqcVersions *v = NULL;
    assert(pqc_versions_new(-1, &v, &R) == REPORT_INVALID && !v);
    assert(pqc_versions_new(600, NULL, &R) == REPORT_INVALID);
    assert(pqc_versions_new(600, &v, NULL) == REPORT_INVALID);
    assert(pqc_versions_new(600, &v, &R) == REPORT_OK && v);
    pqc_test_set_clock(1, 1000, 100);
    assert(pqc_versions_stored(v, o1, c1, l1, &R) == REPORT_OK);
    assert(!pqc_versions_due(v, 0));
    pqc_test_set_clock(1, 1010, 110);
    assert(pqc_versions_stored(v, o2, c2, l2, &R) == REPORT_OK);
    assert(!pqc_versions_due(v, 0));           /* replaced 0 s ago: kept */
    /* 599 s after the first was replaced: still kept */
    pqc_test_set_clock(1, 1609, 709);
    assert(pqc_versions_stored(v, o2, c2, l2, &R) == REPORT_OK);  /* again: itself */
    assert(!pqc_versions_due(v, 0));
    /* 600 s after: the first goes, and the objects only it named */
    pqc_test_set_clock(1, 1610, 710);
    assert(pqc_versions_stored(v, o3, c3, l3, &R) == REPORT_OK);
    assert(pqc_versions_due(v, 0) && strcmp(pqc_versions_due(v, 0), o1) == 0);
    int n_due = 0;
    for (int32_t i = 0; pqc_versions_due(v, i); i++, n_due++) {
        const char *d = pqc_versions_due(v, i);
        if (i == 0) continue;
        assert(names_it(c1, l1, d));           /* the first named it */
        assert(!names_it(c2, l2, d) && !names_it(c3, l3, d));
        assert(!pqc_versions_names(v, d));
    }
    /* what a kept version names is not in the list */
    {
        PqcReader *r = t_pqc_reader_open(c3, l3, err, 512);
        assert(pqc_reader_n_chunks(r) > 0);
        const char *name;
        uint64_t length;
        uint32_t crc;
        pqc_reader_chunk(r, 0, &name, &length, &crc);
        assert(pqc_versions_names(v, name));
        for (int32_t i = 0; pqc_versions_due(v, i); i++)
            assert(strcmp(pqc_versions_due(v, i), name) != 0);
        pqc_reader_free(r);
    }
    /* a delete that failed stays; one that worked leaves */
    assert(pqc_versions_deleted(v, "not-listed", &R) == REPORT_INVALID);
    assert(pqc_versions_deleted(v, o1, &R) == REPORT_OK);
    assert(pqc_versions_deleted(v, o1, &R) == REPORT_INVALID);
    while (pqc_versions_due(v, 0)) {
        char d[PQC_OBJECT_MAX];
        snprintf(d, sizeof d, "%s", pqc_versions_due(v, 0));
        assert(pqc_versions_deleted(v, d, &R) == REPORT_OK);
        n_due--;
    }
    assert(n_due == 1);
    /* bytes that are not a checkpoint are refused, and nothing changes */
    assert(pqc_versions_stored(v, "x.parquet.000009.ckpt",
                               (const uint8_t *)"garbage", 7, &R)
           == REPORT_REFUSED && R.error[0]);
    assert(pqc_versions_stored(v, NULL, c1, l1, &R) == REPORT_INVALID);
    assert(!pqc_versions_due(v, 0));
    pqc_versions_free(v);
    pqc_versions_free(NULL);

    /* keep 0: a replaced version goes at once */
    assert(pqc_versions_new(0, &v, &R) == REPORT_OK);
    assert(pqc_versions_stored(v, o1, c1, l1, &R) == REPORT_OK);
    assert(pqc_versions_stored(v, o2, c2, l2, &R) == REPORT_OK);
    assert(strcmp(pqc_versions_due(v, 0), o1) == 0);
    pqc_versions_free(v);                       /* with deletes still listed */

    pqc_test_set_clock(0, 0, 0);
    pqc_free(c1); pqc_free(c2); pqc_free(c3);
    pqc_writer_free(src.w);
}

static void upload_tests(void) {
    char err[512];
    Sink local = {0}, remote = {0};
    Source src = {t_pqc_writer_new("f.parquet", 4096, err, 512), &local, 0};
    char o1[PQC_OBJECT_MAX], o2[PQC_OBJECT_MAX], o3[PQC_OBJECT_MAX];
    uint64_t l1, l2, l3, r1, r2, r3;
    uint8_t *c1 = next_version(&src, 2, o1, &l1, &r1);
    PqcUpload *m = NULL;
    PqcUploadStep st;

    assert(pqc_upload_new("f.txt", 600, &m, &R) == REPORT_INVALID && !m);
    assert(pqc_upload_new(NULL, 600, &m, &R) == REPORT_INVALID);
    assert(pqc_upload_new("f.parquet", -1, &m, &R) == REPORT_INVALID && !m);
    assert(pqc_upload_new("f.parquet", 600, &m, NULL) == REPORT_INVALID);
    assert(pqc_upload_new("f.parquet", 600, &m, &R) == REPORT_OK && m);
    assert(pqc_upload_next(m, &st, &R) == REPORT_INVALID);    /* no pass yet */
    assert(pqc_upload_did(m, &R) == REPORT_INVALID);
    assert(pqc_upload_remote_has(m, 1, 5, &R) == REPORT_INVALID);
    assert(!pqc_upload_version(m, NULL, NULL, NULL));
    pqc_upload_failed(m);                                     /* nothing to end */
    pqc_test_set_clock(1, 1000, 100);

    /* nothing local yet, the writer still on the file: nothing to do */
    Pass p = run_pass(m, &local, &remote, NULL, NULL, 0, 0, 1, 0);
    assert(p.last == PQC_UPLOAD_NOTHING && remote.n == 0);

    /* the first version: every object, then the version, then the ticket */
    PqcReader *rd = t_pqc_reader_open(c1, l1, err, 512);
    int n1 = (int)pqc_reader_n_chunks(rd);
    pqc_reader_free(rd);
    assert(n1 >= 1);
    p = run_pass(m, &local, &remote, o1, c1, l1, 0, 1, 0);
    assert(p.stats == n1 && p.uploads == n1 && p.versions == 1);
    assert(p.stored == 1 && p.stored_rows == r1 && !strcmp(p.stored_object, o1));
    assert(p.deletes == 0 && p.local == 0 && p.remote == 0);
    assert(p.last == PQC_UPLOAD_NOTHING && remote.n == n1 + 1);
    const uint8_t *sb; size_t sl; const char *so;
    assert(pqc_upload_version(m, &sb, &sl, &so) && sl == l1 &&
           memcmp(sb, c1, l1) == 0 && strcmp(so, o1) == 0);

    /* the same version again: nothing is asked of the remote */
    p = run_pass(m, &local, &remote, o1, c1, l1, 0, 1, 0);
    assert(p.stats == 0 && p.uploads == 0 && p.versions == 0 && p.stored == 0);

    /* a later version: only what the remote lacks, and an outage in
     * the middle loses nothing of what was sent */
    uint8_t *c2 = next_version(&src, 6, o2, &l2, &r2);
    p = run_pass(m, &local, &remote, o2, c2, l2, 0, 1, 2);    /* the 2nd step fails */
    assert(p.last == -1 && p.versions == 0 && p.stored == 0);
    int had = remote.n;
    p = run_pass(m, &local, &remote, o2, c2, l2, 0, 1, 0);
    assert(p.versions == 1 && p.stored_rows == r2);
    assert(remote.n > had && sink_find(&remote, o2) >= 0);
    /* an object the remote holds with the wrong size is sent again */
    {
        PqcUpload *m2 = NULL;
        Sink half = remote;
        assert(pqc_upload_new("f.parquet", 600, &m2, &R) == REPORT_OK);
        int cut = -1;
        for (int i = 0; i < half.n; i++)
            if (strstr(half.o[i].name, ".chunks/") && names_it(c2, l2, half.o[i].name))
                cut = i;
        assert(cut >= 0);
        half.o[cut].size -= 1;
        p = run_pass(m2, &local, &half, o2, c2, l2, 0, 1, 0);
        assert(p.uploads == 1 && p.versions == 1);       /* an upload that knows nothing asks */
        assert(half.o[sink_find(&half, half.o[cut].name)].size ==
               local.o[sink_find(&local, half.o[cut].name)].size);
        pqc_upload_free(m2);
    }

    /* 600 s after the first version was replaced, the next version
     * sent takes it away, and the objects only it named */
    pqc_test_set_clock(1, 1700, 800);
    uint8_t *c3 = next_version(&src, 5, o3, &l3, &r3);
    p = run_pass(m, &local, &remote, o3, c3, l3, 0, 1, 0);
    assert(p.versions == 1 && p.deletes >= 1);
    {
        /* an object sent or seen is not asked about again: the remote
         * was asked only about what the two earlier versions did not name */
        PqcReader *r = t_pqc_reader_open(c3, l3, err, 512);
        int fresh = 0;
        for (uint32_t k = 0; k < pqc_reader_n_chunks(r); k++) {
            const char *name; uint64_t length; uint32_t crc;
            pqc_reader_chunk(r, k, &name, &length, &crc);
            fresh += !names_it(c1, l1, name) && !names_it(c2, l2, name);
        }
        pqc_reader_free(r);
        assert(fresh > 0 && p.stats == fresh && p.uploads == fresh);
    }
    assert(sink_find(&remote, o1) < 0 && sink_find(&remote, o2) >= 0);
    for (int i = 0; i < remote.n; i++)                   /* nothing kept is gone */
        if (strstr(remote.o[i].name, ".chunks/"))
            assert(names_it(c2, l2, remote.o[i].name) ||
                   names_it(c3, l3, remote.o[i].name));
    {
        PqcReader *r = t_pqc_reader_open(c3, l3, err, 512);
        for (uint32_t k = 0; k < pqc_reader_n_chunks(r); k++) {
            const char *name; uint64_t length; uint32_t crc;
            pqc_reader_chunk(r, k, &name, &length, &crc);
            assert(sink_find(&remote, name) >= 0);
        }
        pqc_reader_free(r);
    }

    /* THE END OF A FILE. The writer ended it: uploaded, then the local
     * assembly, then the remote one from the version in hand. The
     * remote fails at its assembly: the file waits there. */
    p = run_pass(m, &local, &remote, o3, c3, l3, 0, 0, 2);   /* local ok, remote fails */
    assert(p.last == -1 && p.local == 1 && p.remote == 0);
    /* now the local copy has the finished file and no version */
    p = run_pass(m, &local, &remote, NULL, NULL, 0, 1, 0, 0);
    assert(p.local == 0 && p.remote == 1 && p.flag == 1);
    assert(p.last == PQC_UPLOAD_DONE);
    /* done stays done */
    p = run_pass(m, &local, &remote, NULL, NULL, 0, 1, 0, 0);
    assert(p.last == PQC_UPLOAD_DONE && p.remote == 0);
    pqc_upload_failed(m);
    assert(pqc_upload_next(m, &st, &R) == REPORT_OK && st.action == PQC_UPLOAD_DONE);
    pqc_upload_free(m);

    /* A NEW PROCESS, for a file an earlier one left: local finished,
     * remote not. It has nothing in hand: from the remote's own. */
    assert(pqc_upload_new("f.parquet", 600, &m, &R) == REPORT_OK);
    p = run_pass(m, &local, &remote, NULL, NULL, 0, 1, 0, 0);
    assert(p.remote == 1 && p.flag == 0 && p.local == 0 &&
           p.last == PQC_UPLOAD_DONE);
    pqc_upload_free(m);
    /* nothing local at all (the remote sweep found it) */
    assert(pqc_upload_new("f.parquet", 600, &m, &R) == REPORT_OK);
    p = run_pass(m, &local, &remote, NULL, NULL, 0, 0, 0, 0);
    assert(p.remote == 1 && p.flag == 0 && p.last == PQC_UPLOAD_DONE);
    pqc_upload_free(m);
    /* a new process with the last version still local: it asks the
     * remote about every object, sends none, sends the version, and
     * ends the file */
    assert(pqc_upload_new("f.parquet", 600, &m, &R) == REPORT_OK);
    p = run_pass(m, &local, &remote, o3, c3, l3, 0, 0, 0);
    assert(p.stats > 0 && p.uploads == 0 && p.versions == 1);
    assert(p.local == 1 && p.remote == 1 && p.flag == 1 &&
           p.last == PQC_UPLOAD_DONE);
    pqc_upload_free(m);

    /* refusals: bytes that are no checkpoint, a checkpoint of another
     * file; the pass is over and nothing is sent */
    assert(pqc_upload_new("f.parquet", 600, &m, &R) == REPORT_OK);
    assert(pqc_upload_local(m, o1, (const uint8_t *)"garbage", 7, 0, 1, &R)
           == REPORT_REFUSED && strstr(R.error, o1));
    assert(pqc_upload_next(m, &st, &R) == REPORT_OK && st.action == PQC_UPLOAD_NOTHING);
    assert(pqc_upload_local(m, o1, NULL, 0, 0, 1, &R) == REPORT_INVALID);
    assert(pqc_upload_local(m, NULL, c1, l1, 0, 1, &R) == REPORT_INVALID);
    assert(pqc_upload_local(NULL, o1, c1, l1, 0, 1, &R) == REPORT_INVALID);
    assert(pqc_upload_local(m, o1, c1, l1, 0, 1, NULL) == REPORT_INVALID);
    pqc_upload_free(m);
    assert(pqc_upload_new("g.parquet", 600, &m, &R) == REPORT_OK);
    assert(pqc_upload_local(m, "g.parquet.000001.ckpt", c1, l1, 0, 1, &R)
           == REPORT_REFUSED && strstr(R.error, "f.parquet"));
    pqc_upload_free(m);
    pqc_upload_free(NULL);
    assert(pqc_upload_doing(NULL)[0] == 0);

    pqc_test_set_clock(0, 0, 0);
    pqc_free(c1); pqc_free(c2); pqc_free(c3);
    pqc_writer_free(src.w);
}

/* Many files, each uploaded through random outages while the clock
 * runs: whatever fails, a version on the remote has every object it
 * names (run_pass checks THE ORDER), the newest version sent is never
 * deleted, and the file ends in both copies. */
static void upload_soak(void) {
    char err[512];
    double mono = 5000;
    for (int iter = 0; iter < 150; iter++) {
        Sink *local = calloc(1, sizeof *local), *remote = calloc(1, sizeof *remote);
        Source src = {t_pqc_writer_new("f.parquet", 1024 + rnd() % 4096, err, 512),
                      local, 0};
        PqcUpload *m = NULL;
        assert(pqc_upload_new("f.parquet", 600, &m, &R) == REPORT_OK);
        char object[PQC_OBJECT_MAX] = "";
        uint8_t *ck = NULL;
        uint64_t len = 0, rows = 0, uploaded_rows = 0;
        int versions = 2 + (int)(rnd() % 6);
        for (int k = 0; k < versions; k++) {
            pqc_free(ck);
            ck = next_version(&src, 1 + (int)(rnd() % 5), object, &len, &rows);
            mono += (double)(rnd() % 500);
            pqc_test_set_clock(1, mono + 1e9, mono);
            int passes = (int)(rnd() % 3);      /* 0: this version is never sent */
            for (int t = 0; t < passes; t++) {
                Pass p = run_pass(m, local, remote, object, ck, len, 0, 1,
                                  rnd() % 3 ? 0 : 1 + (int)(rnd() % 6));
                if (p.stored) uploaded_rows = p.stored_rows;
                const char *so;
                if (pqc_upload_version(m, NULL, NULL, &so))
                    assert(sink_find(remote, so) >= 0);
            }
        }
        /* the writer ends the file; passes until it is done */
        int final_local = 0, done = 0;
        for (int t = 0; t < 200 && !done; t++) {
            Pass p = run_pass(m, local, remote, final_local ? NULL : object,
                              final_local ? NULL : ck, final_local ? 0 : len,
                              final_local, 0,
                              rnd() % 2 ? 0 : 1 + (int)(rnd() % 8));
            if (p.stored) uploaded_rows = p.stored_rows;
            if (p.local) final_local = 1;       /* its versions are gone locally */
            done = p.last == PQC_UPLOAD_DONE;
        }
        assert(done && uploaded_rows == rows);
        assert(sink_find(remote, object) >= 0); /* the last version was sent */
        pqc_upload_free(m);
        pqc_free(ck);
        pqc_writer_free(src.w);
        free(local);
        free(remote);
    }
    pqc_test_set_clock(0, 0, 0);
}

static void sweep_tests(void) {
    /* local: (final there, uploaded, readers are told) */
    assert(pqc_sweep_local(1, 0, 1) == PQC_SWEEP_AWAIT_CATALOG);
    assert(pqc_sweep_local(1, 0, 0) == PQC_SWEEP_DISCARD);
    assert(pqc_sweep_local(1, 1, 1) == PQC_SWEEP_DISCARD);    /* a spool's copy */
    assert(pqc_sweep_local(1, 1, 0) == PQC_SWEEP_DISCARD);
    assert(pqc_sweep_local(0, 1, 1) == PQC_SWEEP_UPLOAD);
    assert(pqc_sweep_local(0, 1, 0) == PQC_SWEEP_UPLOAD);
    assert(pqc_sweep_local(0, 0, 1) == PQC_SWEEP_FINISH);
    assert(pqc_sweep_local(0, 0, 0) == PQC_SWEEP_FINISH);
    /* remote: (final there, readers are told, it has an upload) */
    assert(pqc_sweep_remote(1, 1, 0) == PQC_SWEEP_AWAIT_CATALOG);
    assert(pqc_sweep_remote(1, 1, 1) == PQC_SWEEP_AWAIT_CATALOG);
    assert(pqc_sweep_remote(1, 0, 0) == PQC_SWEEP_DISCARD);
    assert(pqc_sweep_remote(0, 1, 1) == PQC_SWEEP_NOTHING);
    assert(pqc_sweep_remote(0, 1, 0) == PQC_SWEEP_UPLOAD);
    assert(pqc_sweep_remote(0, 0, 0) == PQC_SWEEP_UPLOAD);
}

/* ---- the assembly of a file ---------------------------------------------- */

typedef struct { char object[PQC_OBJECT_MAX]; const uint8_t *ck; uint64_t len; } Held;

/* One assembly in a pretend copy. held: the bytes of the versions
 * there. given: the version the caller has in hand, or NULL. ops: what
 * the host did, in order ("S name ", "R ", "A ", "L ", "D name ").
 * fail_delete: the number (from 1) of the delete that fails, 0 for
 * none. Answers the status that ended it, the flag of DONE in *flag. */
static int run_finish(Sink *s, const char *file, int mode, int keep,
                      const Held *held, int n_held, const Held *given,
                      int fail_delete, char *ops, size_t cap, int *flag) {
    PqcFinish *f = NULL;
    PqcFinishStep st, again;
    char of[PQC_OBJECT_MAX];
    int64_t seq;
    int rc, deletes = 0;
    ops[0] = 0;
    *flag = -1;
    assert(pqc_finish_new(file, mode, keep, &f, &R) == REPORT_OK && f);
    for (int i = s->n - 1; i >= 0; i--)         /* any order */
        if (pqc_ckpt_object_parse(s->o[i].name, of, &seq) && !strcmp(of, file))
            assert(pqc_finish_version(f, s->o[i].name, &R) == REPORT_OK);
    if (given &&
        (rc = pqc_finish_given(f, given->object, given->ck, given->len, &R))) {
        pqc_finish_free(f);
        return rc;
    }
#define OP(...) snprintf(ops + strlen(ops), cap - strlen(ops), __VA_ARGS__)
    for (;;) {
        if ((rc = pqc_finish_next(f, &st, &R)) != REPORT_OK) break;
        assert(pqc_finish_next(f, &again, &R) == REPORT_OK);  /* the same again */
        assert(again.action == st.action && !strcmp(again.name, st.name));
        if (st.action == PQC_FINISH_DONE) { *flag = st.flag; break; }
        if (st.action == PQC_FINISH_STAT) {
            int i = sink_find(s, st.name);
            OP("S %s ", st.name);
            assert(pqc_finish_did(f, &R) == REPORT_INVALID);
            assert(pqc_finish_next(f, &again, &R) == REPORT_OK);
            if ((rc = pqc_finish_has(f, i >= 0, i >= 0 ? s->o[i].size : 0, &R)))
                break;
            continue;
        }
        if (st.action == PQC_FINISH_READ) {
            int k = -1;
            for (int i = 0; i < n_held; i++)
                if (!strcmp(held[i].object, st.name)) k = i;
            assert(k >= 0 && sink_find(s, st.name) >= 0);
            OP("R %s ", st.name);
            if ((rc = pqc_finish_read(f, held[k].ck, held[k].len, &R))) break;
            continue;
        }
        if (st.action == PQC_FINISH_ASSEMBLE) {
            const char *name;
            int64_t off, len;
            uint64_t total = 0;
            for (uint32_t i = 0; i < st.size; i++) {
                assert(pqc_finish_source(f, i, &name, &off, &len));
                int at = sink_find(s, name);
                assert(at >= 0);
                if (i + 1 < st.size) assert(off == -1 && len == -1);
                else assert(off > 0 && len > 0 &&
                            (uint64_t)(off + len) <= s->o[at].size);
                total += off < 0 ? s->o[at].size : (uint64_t)len;
            }
            assert(!pqc_finish_source(f, (uint32_t)st.size, &name, &off, &len));
            OP("A ");
            sink_put(s, st.name, total);
        } else if (st.action == PQC_FINISH_LIST) {
            OP("L ");
            size_t n = strlen(st.name);
            for (int i = 0; i < s->n; i++)
                if (!strncmp(s->o[i].name, st.name, n))
                    assert(pqc_finish_listed(f, s->o[i].name, &R) == REPORT_OK);
        } else {
            assert(st.action == PQC_FINISH_DELETE);
            if (++deletes == fail_delete) { rc = -1; break; }
            OP("D %s ", st.name);
            sink_delete(s, st.name);
        }
        assert(pqc_finish_did(f, &R) == REPORT_OK);
    }
#undef OP
    pqc_finish_free(f);
    return rc;
}

static void finish_tests(void) {
    char err[512], ops[1 << 16], want[1 << 16];
    Sink s = {0};
    Source src = {t_pqc_writer_new("f.parquet", 4096, err, 512), &s, 0};
    Held h[3];
    uint64_t rows;
    int flag;
    PqcFinish *f = NULL;
    PqcFinishStep st;

    /* nothing there */
    assert(run_finish(&s, "f.parquet", 0, 0, NULL, 0, NULL, 0, ops, sizeof ops,
                      &flag) == REPORT_REFUSED);
    assert(strstr(R.error, "nothing there") && !strcmp(ops, "S f.parquet "));

    h[0].ck = next_version(&src, 3, h[0].object, &h[0].len, &rows);
    h[1].ck = next_version(&src, 3, h[1].object, &h[1].len, &rows);
    PqcReader *rd = t_pqc_reader_open(h[1].ck, h[1].len, err, 512);
    uint32_t n = pqc_reader_n_chunks(rd);
    assert(n >= 2);
    Sink start = s;

    /* an object of the newest version is not there, or not whole: refused,
     * and nothing was changed */
    const char *cname;
    uint64_t clen;
    uint32_t crc;
    pqc_reader_chunk(rd, n - 1, &cname, &clen, &crc);
    sink_delete(&s, cname);
    assert(run_finish(&s, "f.parquet", 0, 0, h, 2, NULL, 0, ops, sizeof ops,
                      &flag) == REPORT_REFUSED);
    assert(strstr(R.error, "missing or wrong size") && strstr(R.error, cname));
    assert(!strstr(ops, "A ") && !strstr(ops, "D "));
    s = start;
    sink_put(&s, cname, clen + 1);
    assert(run_finish(&s, "f.parquet", 0, 0, h, 2, NULL, 0, ops, sizeof ops,
                      &flag) == REPORT_REFUSED);
    assert(strstr(R.error, "missing or wrong size") && !strstr(ops, "A "));
    s = start;

    /* an object that no version names: a crash left it */
    sink_put(&s, "f.parquet.chunks/999999", 77);
    Sink before = s;

    /* the assembly: the finished file asked about, the newest version
     * read, each object asked about, the file made, and the deletes in
     * their order, the version in use last */
    assert(run_finish(&s, "f.parquet", 0, 0, h, 2, NULL, 0, ops, sizeof ops,
                      &flag) == REPORT_OK && flag == 1);
    snprintf(want, sizeof want, "S f.parquet R %s ", h[1].object);
    for (uint32_t i = 0; i < n; i++) {
        pqc_reader_chunk(rd, i, &cname, &clen, &crc);
        snprintf(want + strlen(want), sizeof want - strlen(want), "S %s ", cname);
    }
    strcat(want, "A L ");
    assert(!strncmp(ops, want, strlen(want)));
    char tail[2 * PQC_OBJECT_MAX + 16];
    snprintf(tail, sizeof tail, "D %s D %s ", h[0].object, h[1].object);
    assert(strlen(ops) > strlen(tail) &&
           !strcmp(ops + strlen(ops) - strlen(tail), tail));
    assert(strstr(ops, "D f.parquet.chunks/999999 "));
    assert(s.n == 1 && !strcmp(s.o[0].name, "f.parquet"));

    /* again: it was finished before */
    assert(run_finish(&s, "f.parquet", 0, 0, h, 2, NULL, 0, ops, sizeof ops,
                      &flag) == REPORT_OK && flag == 0);
    assert(!strcmp(ops, "S f.parquet "));

    /* a delete fails: the file is there, and so is the version in use.
     * The next assembly refuses (both are there); a discard ends it. */
    s = before;
    assert(run_finish(&s, "f.parquet", 0, 0, h, 2, NULL, 2, ops, sizeof ops,
                      &flag) == -1);
    assert(sink_find(&s, "f.parquet") >= 0 && sink_find(&s, h[1].object) >= 0);
    assert(run_finish(&s, "f.parquet", 0, 0, h, 2, NULL, 0, ops, sizeof ops,
                      &flag) == REPORT_REFUSED);
    assert(strstr(R.error, "BOTH final and checkpoint exist"));
    assert(run_finish(&s, "f.parquet", PQC_FINISH_DISCARD, 0, h, 2, NULL, 0,
                      ops, sizeof ops, &flag) == REPORT_OK && flag == 0);
    assert(s.n == 1 && !strstr(ops, "A ") && !strstr(ops, "R "));

    /* keep_checkpoints: nothing is deleted; the discard deletes later,
     * in the same order; with no finished file it refuses */
    s = before;
    assert(run_finish(&s, "f.parquet", PQC_FINISH_DISCARD, 0, h, 2, NULL, 0,
                      ops, sizeof ops, &flag) == REPORT_REFUSED);
    assert(strstr(R.error, "only copy") && s.n == before.n);
    assert(run_finish(&s, "f.parquet", 0, 1, h, 2, NULL, 0, ops, sizeof ops,
                      &flag) == REPORT_OK && flag == 1);
    assert(s.n == before.n + 1 && !strstr(ops, "L ") && !strstr(ops, "D "));
    assert(run_finish(&s, "f.parquet", PQC_FINISH_DISCARD, 0, h, 2, NULL, 0,
                      ops, sizeof ops, &flag) == REPORT_OK && flag == 0);
    assert(!strncmp(ops, "S f.parquet L D ", 16) &&
           !strcmp(ops + strlen(ops) - strlen(tail), tail));
    assert(s.n == 1);

    /* the newest version is not a checkpoint: the one before it is used,
     * with a line for the log, and the bad one is deleted before it */
    s = before;
    Held bad[2] = {h[0], h[1]};
    bad[1].ck = (const uint8_t *)"garbage";
    bad[1].len = 7;
    assert(run_finish(&s, "f.parquet", 0, 0, bad, 2, NULL, 0, ops, sizeof ops,
                      &flag) == REPORT_OK && flag == 1);
    snprintf(want, sizeof want, "S f.parquet R %s R %s ", h[1].object,
             h[0].object);
    assert(!strncmp(ops, want, strlen(want)));
    snprintf(tail, sizeof tail, "D %s D %s ", h[1].object, h[0].object);
    assert(!strcmp(ops + strlen(ops) - strlen(tail), tail));
    /* no version is one: refused, nothing changed */
    s = before;
    bad[0] = bad[1];
    snprintf(bad[0].object, sizeof bad[0].object, "%s", h[0].object);
    assert(run_finish(&s, "f.parquet", 0, 0, bad, 2, NULL, 0, ops, sizeof ops,
                      &flag) == REPORT_REFUSED);
    assert(strstr(R.error, "no checkpoint version holds") &&
           strstr(R.error, h[0].object) && strstr(R.error, h[1].object));
    assert(s.n == before.n);

    /* the line for the log */
    assert(pqc_finish_new("f.parquet", 0, 0, &f, &R) == REPORT_OK);
    assert(pqc_finish_version(f, h[0].object, &R) == REPORT_OK);
    assert(pqc_finish_version(f, h[1].object, &R) == REPORT_OK);
    assert(pqc_finish_next(f, &st, &R) == REPORT_OK && st.action == PQC_FINISH_STAT
           && st.flag == 1);
    assert(pqc_finish_read(f, h[1].ck, h[1].len, &R) == REPORT_INVALID);
    assert(pqc_finish_next(f, &st, &R) == REPORT_OK);
    assert(pqc_finish_has(f, 0, 0, &R) == REPORT_OK);
    assert(pqc_finish_version(f, h[0].object, &R) == REPORT_INVALID);  /* begun */
    assert(pqc_finish_next(f, &st, &R) == REPORT_OK && st.action == PQC_FINISH_READ);
    assert(pqc_finish_has(f, 1, 1, &R) == REPORT_INVALID);
    assert(pqc_finish_next(f, &st, &R) == REPORT_OK);
    assert(pqc_finish_read(f, NULL, 0, &R) == REPORT_OK && R.lines == 0);  /* empty */
    assert(pqc_finish_next(f, &st, &R) == REPORT_OK && !strcmp(st.name, h[0].object));
    assert(pqc_finish_read(f, h[0].ck, h[0].len, &R) == REPORT_OK);
    assert(R.lines == 1 && R.line[0].level == REPORT_WARNING &&
           strstr(R.line[0].text, "refused, assembling from") &&
           strstr(R.line[0].text, h[1].object));
    pqc_finish_free(f);

    /* the caller has the newest version in hand: no question about the
     * finished file, no read; the last source is that version's own bytes */
    s = before;
    assert(run_finish(&s, "f.parquet", 0, 0, h, 2, &h[1], 0, ops, sizeof ops,
                      &flag) == REPORT_OK && flag == 1);
    assert(strncmp(ops, "S f.parquet ", 12) && !strstr(ops, "R ") && s.n == 1);
    s = before;
    assert(run_finish(&s, "f.parquet", 0, 0, h, 2, &bad[1], 0, ops, sizeof ops,
                      &flag) == REPORT_REFUSED && s.n == before.n);

    /* a version that names another file */
    Sink g = {0};
    Held hg = h[0];
    snprintf(hg.object, sizeof hg.object, "g.parquet.000001.ckpt");
    sink_put(&g, hg.object, hg.len);
    assert(run_finish(&g, "g.parquet", 0, 0, &hg, 1, NULL, 0, ops, sizeof ops,
                      &flag) == REPORT_REFUSED);
    assert(strstr(R.error, "mispaired") && g.n == 1);
    assert(run_finish(&g, "g.parquet", 0, 0, &hg, 1, &hg, 0, ops, sizeof ops,
                      &flag) == REPORT_REFUSED && strstr(R.error, "mispaired"));
    /* a file in a directory of the place: the checkpoint names the base */
    Sink d = {0};
    Held hd = h[0];
    snprintf(hd.object, sizeof hd.object, "a/b/f.parquet.000001.ckpt");
    sink_put(&d, hd.object, hd.len);
    assert(run_finish(&d, "a/b/f.parquet", 0, 0, &hd, 1, NULL, 0, ops,
                      sizeof ops, &flag) == REPORT_REFUSED);
    assert(strstr(R.error, "missing or wrong size"));    /* paired; no objects */

    /* arguments */
    assert(pqc_finish_new(NULL, 0, 0, &f, &R) == REPORT_INVALID);
    assert(pqc_finish_new("f.parquet", 7, 0, &f, &R) == REPORT_INVALID && !f);
    assert(pqc_finish_new("f.parquet", 0, 0, &f, NULL) == REPORT_INVALID);
    assert(pqc_finish_new("f.parquet", 0, 0, &f, &R) == REPORT_OK);
    assert(pqc_finish_version(f, "g.parquet.000001.ckpt", &R) == REPORT_INVALID);
    assert(pqc_finish_version(f, "f.parquet", &R) == REPORT_INVALID);
    assert(pqc_finish_did(f, &R) == REPORT_INVALID);
    assert(pqc_finish_listed(f, "x", &R) == REPORT_INVALID);
    assert(pqc_finish_next(f, NULL, &R) == REPORT_INVALID);
    assert(!pqc_finish_source(f, 0, &cname, (int64_t *)&clen, (int64_t *)&clen));
    pqc_finish_free(f);
    pqc_finish_free(NULL);
    assert(pqc_finish_new("f.parquet", PQC_FINISH_DISCARD, 0, &f, &R) == REPORT_OK);
    assert(pqc_finish_given(f, h[0].object, h[0].ck, h[0].len, &R) == REPORT_INVALID);
    pqc_finish_free(f);

    pqc_reader_free(rd);
    pqc_free((void *)h[0].ck);
    pqc_free((void *)h[1].ck);
    pqc_writer_free(src.w);
}

/* ---- the parts of an assembly -------------------------------------------- */

static void parts_are(const uint64_t *sizes, uint32_t n, uint64_t part_min,
                      const char *want) {
    PqcPart *p = NULL;
    uint32_t np = 0;
    char got[256] = "";
    assert(pqc_plan_parts(sizes, n, part_min, &p, &np, &R) == REPORT_OK);
    uint32_t next = 0;
    for (uint32_t i = 0; i < np; i++) {
        assert(p[i].first == next && p[i].count >= 1);     /* each source once */
        assert(!p[i].copy || p[i].count == 1);
        uint64_t bytes = 0;
        for (uint32_t k = 0; k < p[i].count; k++) bytes += sizes[p[i].first + k];
        assert(i + 1 == np || bytes >= part_min);          /* S3's rule */
        snprintf(got + strlen(got), sizeof got - strlen(got), "%c%u ",
                 p[i].copy ? 'c' : 'u', p[i].count);
        next += p[i].count;
    }
    assert(next == n);
    assert(!strcmp(got, want));
    pqc_free(p);
}

static void parts_tests(void) {
    const uint64_t big = 5000, small = 100;
    parts_are((uint64_t[]){big, big, small, small}, 4, 4096, "c1 c1 u2 ");
    parts_are((uint64_t[]){big}, 1, 4096, "c1 ");
    parts_are((uint64_t[]){small, small}, 2, 4096, "u2 ");
    /* a small object before a large one: the run takes the large one
     * in, so that no part before the last is under the minimum */
    parts_are((uint64_t[]){small, big, small}, 3, 4096, "u2 u1 ");
    parts_are(NULL, 0, 4096, "");
    parts_are((uint64_t[]){small, small, small}, 3, 0, "c1 c1 c1 ");
    /* any sizes: each source once, in order, and S3's rule holds */
    for (int round = 0; round < 2000; round++) {
        uint64_t sizes[40];
        uint32_t n = rnd() % 40;
        for (uint32_t i = 0; i < n; i++)
            sizes[i] = rnd() % 3 ? rnd() % 300 : 1000 + rnd() % 9000;
        PqcPart *p = NULL;
        uint32_t np = 0, next = 0;
        assert(pqc_plan_parts(sizes, n, 1000, &p, &np, &R) == REPORT_OK);
        for (uint32_t i = 0; i < np; i++) {
            uint64_t bytes = 0;
            assert(p[i].first == next);
            for (uint32_t k = 0; k < p[i].count; k++) bytes += sizes[next++];
            assert(i + 1 == np || bytes >= 1000);
            assert(!p[i].copy || (p[i].count == 1 && bytes >= 1000));
        }
        assert(next == n);
        pqc_free(p);
    }
    /* more parts than S3 takes */
    uint32_t many = PQC_MAX_PARTS + 1, np;
    uint64_t *sizes = malloc(many * sizeof *sizes);
    PqcPart *p;
    for (uint32_t i = 0; i < many; i++) sizes[i] = 10;
    assert(pqc_plan_parts(sizes, many, 10, &p, &np, &R) == REPORT_REFUSED && !p);
    assert(strstr(R.error, "exceeds"));
    assert(pqc_plan_parts(sizes, many - 1, 10, &p, &np, &R) == REPORT_OK &&
           np == PQC_MAX_PARTS);
    pqc_free(p);
    assert(pqc_plan_parts(sizes, 3, 10, NULL, &np, &R) == REPORT_INVALID);
    assert(pqc_plan_parts(NULL, 3, 10, &p, &np, &R) == REPORT_INVALID);
    assert(pqc_plan_parts(sizes, 3, 10, &p, &np, NULL) == REPORT_INVALID);
    free(sizes);
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
        uint8_t *s0 = t_pqc_shift_footer(th, flen, 0, &o0, err, 512);
        assert(s0);
        uint8_t *s1 = t_pqc_shift_footer(th, flen, 5000, &o1, err, 512);
        assert(s1);
        uint8_t *s2 = t_pqc_shift_footer(s1, o1, -5000, &o2, err, 512);
        assert(s2);
        assert(o2 == o0 && memcmp(s2, s0, o0) == 0);
        pqc_free(s0);
        pqc_free(s1);
        pqc_free(s2);
    }

    /* writer soak: many lifecycles, random sizes, chunk drains */
    for (int iter = 0; iter < 300; iter++) {
        PqcWriter *w = t_pqc_writer_new("soak.parquet",
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
            int64_t rows = t_pqc_add_group(w, mini, n, err, 512);
            assert(rows >= 0);
            /* a checkpoint may not claim what is not stored */
            if (rnd() % 5 == 0) {
                uint64_t cl0, wm0;
                assert(t_pqc_build_checkpoint(w, NULL, 0, &cl0, &wm0,
                                            err, 512) == NULL);
            }
            /* drain */
            while ((rc = t_pqc_next_object(w, name, &data, &len, &crc)) == 1) {
                assert(pqc_crc32(data, len) == crc);
                if (rnd() % 4 == 0) {          /* failed store: retry   */
                    const uint8_t *d2;
                    uint64_t l2;
                    uint32_t c2;
                    assert(t_pqc_next_object(w, name, &d2, &l2, &c2) == 1);
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
                    assert(t_pqc_add_unfinished(w, mini, bad, err, 512) < 0);
                    tn = mini_file(mini, 100 + rnd() % 500, 1, 2,
                                   (int64_t)span);
                }
                assert(t_pqc_add_unfinished(w, mini, tn, err, 512)
                       == (int64_t)span);
                uint64_t cl, wm;
                assert(t_pqc_build_checkpoint(w, NULL, 0, &cl, &wm, err, 512)
                       == NULL);               /* its object is not stored */
                int stored = 0;
                while ((rc = t_pqc_next_object(w, name, &data, &len, &crc))
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
                uint8_t *ck = t_pqc_build_checkpoint(w, NULL, 0, &cl, &wm,
                                                   err, 512);
                assert(ck);
                assert(wm == pqc_rows_final(w) + unfinished);
                PqcReader *r = t_pqc_reader_open(ck, cl, err, 512);
                assert(r);
                assert(strcmp(pqc_reader_name(r), "soak.parquet") == 0);
                assert(pqc_reader_n_chunks(r) >= pqc_chunks_cut(w) + 1);
                uint64_t off, dl;
                pqc_reader_data(r, &off, &dl);
                assert(off + dl == cl);
                /* corrupt copy must refuse */
                ck[cl / 2] ^= 0xFF;
                assert(t_pqc_reader_open(ck, cl, err, 512) == NULL);
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
        uint8_t *ck = t_pqc_build_checkpoint(w, NULL, 0, &cl, &wm, err, 512);
        assert(ck && wm == pqc_rows_final(w));
        pqc_free(ck);
        pqc_writer_free(w);
        while (n_store) free(store[--n_store]);
    }

    /* refusals */
    assert(t_pqc_writer_new("nope.txt", 1024, err, 512) == NULL);
    {
        PqcWriter *w = t_pqc_writer_new("e.parquet", 1024, err, 512);
        uint64_t cl, wm;
        assert(t_pqc_build_checkpoint(w, NULL, 0, &cl, &wm, err, 512)
               == NULL);                        /* no rows: needs a mini */
        pqc_writer_free(w);
    }
    version_names_tests();
    versions_tests();
    upload_tests();
    upload_soak();
    sweep_tests();
    finish_tests();
    parts_tests();
    printf("san_test: OK\n");
    return 0;
}
