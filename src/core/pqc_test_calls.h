/* pqc_test_calls.h -- for the checkpoint core's C tests: each call that
 * reports (report.h), in the shape the tests state their checks in: the
 * result as the return value (NULL or negative for a failure), the
 * reason copied to the test's buffer. Every helper also checks what
 * report.h promises: the status returned is the report's, a failure has
 * a reason, and this core makes no lines. */
#ifndef PQC_TEST_CALLS_H
#define PQC_TEST_CALLS_H

#include <assert.h>
#include <string.h>

#include "pqc.h"

static Report T_report;

static int t_took(int rc, char *err, size_t errlen) {
    assert(rc == T_report.status);
    assert(T_report.lines == 0 && T_report.dropped == 0);
    assert((rc == REPORT_OK) == (T_report.error[0] == 0));
    if (err && errlen) {
        strncpy(err, T_report.error, errlen - 1);
        err[errlen - 1] = 0;
    }
    return rc;
}

static inline PqcWriter *t_pqc_writer_new(const char *final_name,
                                          uint64_t chunk_bytes, char *err,
                                          size_t errlen) {
    PqcWriter *w = NULL;
    return t_took(pqc_writer_new(final_name, chunk_bytes, &w, &T_report),
                  err, errlen) ? NULL : w;
}

static inline int64_t t_pqc_add_group(PqcWriter *w, const uint8_t *mini,
                                      size_t len, char *err, size_t errlen) {
    int64_t rows = -1;
    return t_took(pqc_add_group(w, mini, len, &rows, &T_report), err, errlen)
           ? -1 : rows;
}

static inline int64_t t_pqc_add_unfinished(PqcWriter *w, const uint8_t *mini,
                                           size_t len, char *err,
                                           size_t errlen) {
    int64_t rows = -1;
    return t_took(pqc_add_unfinished(w, mini, len, &rows, &T_report), err,
                  errlen) ? -1 : rows;
}

static inline int t_pqc_next_object(PqcWriter *w, char name[PQC_NAME_MAX],
                                    const uint8_t **data, uint64_t *len,
                                    uint32_t *crc) {
    int have = 0;
    if (t_took(pqc_next_object(w, name, data, len, crc, &have, &T_report),
               NULL, 0))
        return -1;
    return have;
}

static inline uint8_t *t_pqc_build_checkpoint(PqcWriter *w,
                                              const uint8_t *empty_mini,
                                              size_t empty_len,
                                              uint64_t *out_len,
                                              uint64_t *out_watermark,
                                              char *err, size_t errlen) {
    uint8_t *out = NULL;
    return t_took(pqc_build_checkpoint(w, empty_mini, empty_len, &out,
                                       out_len, out_watermark, &T_report),
                  err, errlen) ? NULL : out;
}

static inline PqcReader *t_pqc_reader_open(const uint8_t *ckpt, size_t len,
                                           char *err, size_t errlen) {
    PqcReader *r = NULL;
    return t_took(pqc_reader_open(ckpt, len, &r, &T_report), err, errlen)
           ? NULL : r;
}

static inline uint8_t *t_pqc_shift_footer(const uint8_t *thrift, size_t len,
                                          int64_t delta, size_t *out_len,
                                          char *err, size_t errlen) {
    uint8_t *out = NULL;
    return t_took(pqc_shift_footer(thrift, len, delta, &out, out_len,
                                   &T_report), err, errlen) ? NULL : out;
}

#endif
