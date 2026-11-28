/* pqc_upload.c -- the checkpoint core's rules for checkpoint versions,
 * for the upload of a file from a local copy to a remote one, for the
 * assembly of a file from its checkpoint, and for the parts of a
 * multipart assembly. pqc.h is the spec (CHECKPOINT VERSIONS, THE
 * UPLOAD, THE SWEEP AT START, THE ASSEMBLY OF A FILE, THE PARTS OF AN
 * ASSEMBLY). No input, no output, no waiting: the host stores, copies
 * and deletes; this says what, and in which order.
 */
#include "pqc.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static ReportClock g_clock;

void pqc_test_set_clock(int fixed, double wall, double mono) {
    g_clock.fixed = fixed;
    g_clock.wall = wall;
    g_clock.mono = mono;
}

static int no_memory(Report *report) {
    return report_fail(report, REPORT_NO_MEMORY, "out of memory");
}

static char *copy_text(const char *s) {
    size_t n = strlen(s) + 1;
    char *p = malloc(n);
    if (p) memcpy(p, s, n);
    return p;
}

/* ---- a set of object names ---------------------------------------------- */

typedef struct {
    char **s;
    int32_t n, cap;
} Names;

static int names_find(const Names *a, const char *name) {
    for (int32_t i = 0; i < a->n; i++)
        if (strcmp(a->s[i], name) == 0) return i;
    return -1;
}

static int names_push(Names *a, const char *name);

/* 0, or -1 when memory is out. A name already there is not added again. */
static int names_add(Names *a, const char *name) {
    if (names_find(a, name) >= 0) return 0;
    return names_push(a, name);
}

/* 0, or -1 when memory is out. The name is added at the end. */
static int names_push(Names *a, const char *name) {
    if (a->n == a->cap) {
        int32_t cap = a->cap ? a->cap * 2 : 8;
        char **s = realloc(a->s, (size_t)cap * sizeof *s);
        if (!s) return -1;
        a->s = s;
        a->cap = cap;
    }
    char *c = copy_text(name);
    if (!c) return -1;
    a->s[a->n++] = c;
    return 0;
}

static void names_remove_at(Names *a, int32_t i) {
    free(a->s[i]);
    memmove(&a->s[i], &a->s[i + 1], (size_t)(a->n - i - 1) * sizeof *a->s);
    a->n--;
}

static void names_free(Names *a) {
    for (int32_t i = 0; i < a->n; i++) free(a->s[i]);
    free(a->s);
    a->s = NULL;
    a->n = a->cap = 0;
}

static int by_text(const void *a, const void *b) {
    return strcmp(*(char *const *)a, *(char *const *)b);
}

/* The object names the checkpoint bytes list, into `out` (empty
 * before). The report has the reason when the bytes are refused. */
static int names_of(const uint8_t *ckpt, size_t len, Names *out,
                    Report *report) {
    PqcReader *r = NULL;
    Report inner;
    if (pqc_reader_open(ckpt, len, &r, &inner) != REPORT_OK)
        return report_fail(report, inner.status, "%s", inner.error);
    uint32_t n = pqc_reader_n_chunks(r);
    for (uint32_t i = 0; i < n; i++) {
        const char *name;
        uint64_t length;
        uint32_t crc;
        pqc_reader_chunk(r, i, &name, &length, &crc);
        if (names_add(out, name)) {
            pqc_reader_free(r);
            names_free(out);
            return no_memory(report);
        }
    }
    pqc_reader_free(r);
    return REPORT_OK;
}

/* ---- the names of checkpoint versions ----------------------------------- */

#define PARQUET ".parquet"
#define CKPT ".ckpt"

int pqc_ckpt_object(const char *name, int64_t seq, char out[PQC_OBJECT_MAX],
                    Report *report) {
    if (!report_begin(report)) return REPORT_INVALID;
    if (!name || !out)
        return report_fail(report, REPORT_INVALID,
                           "pqc_ckpt_object: a NULL argument");
    if (seq < 0)
        return report_fail(report, REPORT_INVALID,
                           "pqc_ckpt_object: version %lld of %s: a version "
                           "is 0 or more", (long long)seq, name);
    int n = snprintf(out, PQC_OBJECT_MAX, "%s.%06lld%s", name,
                     (long long)seq, CKPT);
    if (n < 0 || n >= PQC_OBJECT_MAX)
        return report_fail(report, REPORT_INVALID,
                           "pqc_ckpt_object: the name of version %lld of "
                           "%.60s... is longer than %d bytes",
                           (long long)seq, name, PQC_OBJECT_MAX - 1);
    return REPORT_OK;
}

static int ends_with(const char *s, size_t len, const char *tail) {
    size_t t = strlen(tail);
    return len >= t && memcmp(s + len - t, tail, t) == 0;
}

/* <name>.ckpt or <name>.NNNNNN.ckpt (six digits or more), where <name>
 * ends in ".parquet" and has something before it. */
int pqc_ckpt_object_parse(const char *object, char name[PQC_OBJECT_MAX],
                          int64_t *seq) {
    if (!object) return 0;
    size_t len = strlen(object);
    if (!ends_with(object, len, CKPT)) return 0;
    len -= strlen(CKPT);                 /* the part before ".ckpt" */
    int64_t version = 0;
    if (!ends_with(object, len, PARQUET)) {
        size_t digits = 0;
        while (digits < len && object[len - 1 - digits] >= '0' &&
               object[len - 1 - digits] <= '9')
            digits++;
        if (digits < 6 || digits > 18 || digits == len ||
            object[len - 1 - digits] != '.')
            return 0;
        for (size_t i = len - digits; i < len; i++)
            version = version * 10 + (object[i] - '0');
        len -= digits + 1;
        if (!ends_with(object, len, PARQUET)) return 0;
    }
    if (len <= strlen(PARQUET) || len >= PQC_OBJECT_MAX) return 0;
    if (name) {
        memcpy(name, object, len);
        name[len] = 0;
    }
    if (seq) *seq = version;
    return 1;
}

/* ---- checkpoint versions ------------------------------------------------- */

typedef struct {
    char *object;
    int replaced;                 /* a later version was stored */
    double at;                    /* when, on the monotonic clock */
    Names names;                  /* the objects it names */
} Version;

struct PqcVersions {
    double keep;
    Version *v;
    int32_t n, cap;
    Names due;                    /* to delete now, in order */
};

static void version_free(Version *x) {
    free(x->object);
    names_free(&x->names);
}

int pqc_versions_new(double keep_seconds, PqcVersions **out, Report *report) {
    if (!report_begin(report)) return REPORT_INVALID;
    if (!out)
        return report_fail(report, REPORT_INVALID,
                           "pqc_versions_new: a NULL argument");
    *out = NULL;
    if (!(keep_seconds >= 0) || !isfinite(keep_seconds))
        return report_fail(report, REPORT_INVALID,
                           "pqc_versions_new: keep_seconds is %g; it is 0 "
                           "or more", keep_seconds);
    PqcVersions *v = calloc(1, sizeof *v);
    if (!v) return no_memory(report);
    v->keep = keep_seconds;
    *out = v;
    return REPORT_OK;
}

void pqc_versions_free(PqcVersions *v) {
    if (!v) return;
    for (int32_t i = 0; i < v->n; i++) version_free(&v->v[i]);
    free(v->v);
    names_free(&v->due);
    free(v);
}

int pqc_versions_names(const PqcVersions *v, const char *object) {
    if (!v || !object) return 0;
    for (int32_t i = 0; i < v->n; i++)
        if (names_find(&v->v[i].names, object) >= 0) return 1;
    return 0;
}

/* What becomes of a version this holds when a new one is stored. */
enum { STAYS = 0, SAME_NAME, GOES };

int pqc_versions_stored(PqcVersions *v, const char *object,
                        const uint8_t *ckpt, size_t len, Report *report) {
    if (!report_begin(report)) return REPORT_INVALID;
    if (!v || !object || (!ckpt && len))
        return report_fail(report, REPORT_INVALID,
                           "pqc_versions_stored: a NULL argument");
    /* Everything that can fail is made first: v changes only when the
     * whole step is known to work. */
    Version fresh = {0};
    if (names_of(ckpt, len, &fresh.names, report) != REPORT_OK)
        return report->status;
    Names named = {0};            /* what a kept version names */
    int32_t due_before = v->due.n;
    int *fate = calloc((size_t)v->n + 1, sizeof *fate);
    fresh.object = copy_text(object);
    int oom = !fate || !fresh.object;
    if (!oom && v->n == v->cap) {
        int32_t cap = v->cap ? v->cap * 2 : 4;
        Version *p = realloc(v->v, (size_t)cap * sizeof *p);
        if (p) {
            v->v = p;
            v->cap = cap;
        } else {
            oom = 1;
        }
    }
    double now = report_mono(&g_clock);
    /* A version of the same name (a store done again) is this one, not
     * an older one. The newest of the others is replaced from now. */
    int32_t newest = -1;
    for (int32_t i = 0; i < v->n && !oom; i++) {
        if (strcmp(v->v[i].object, object) == 0)
            fate[i] = SAME_NAME;
        else
            newest = i;
    }
    for (int32_t i = 0; i < v->n && !oom; i++) {
        if (fate[i] == SAME_NAME) continue;
        int replaced = v->v[i].replaced || i == newest;
        double at = v->v[i].replaced ? v->v[i].at : now;
        if (replaced && now - at >= v->keep) fate[i] = GOES;
    }
    for (int32_t i = 0; i < v->n && !oom; i++) {
        if (fate[i] != STAYS) continue;
        for (int32_t k = 0; k < v->v[i].names.n && !oom; k++)
            oom = names_add(&named, v->v[i].names.s[k]) != 0;
    }
    for (int32_t k = 0; k < fresh.names.n && !oom; k++)
        oom = names_add(&named, fresh.names.s[k]) != 0;
    /* the deletes, in order: a version first, then the objects that
     * only it named */
    for (int32_t i = 0; i < v->n && !oom; i++) {
        if (fate[i] != GOES) continue;
        oom = names_add(&v->due, v->v[i].object) != 0;
        qsort(v->v[i].names.s, (size_t)v->v[i].names.n, sizeof(char *),
              by_text);
        for (int32_t k = 0; k < v->v[i].names.n && !oom; k++) {
            const char *name = v->v[i].names.s[k];
            if (names_find(&named, name) >= 0) continue;
            oom = names_add(&v->due, name) != 0 ||
                  names_add(&named, name) != 0;
        }
    }
    names_free(&named);
    if (oom) {
        while (v->due.n > due_before) names_remove_at(&v->due, v->due.n - 1);
        free(fate);
        version_free(&fresh);
        return no_memory(report);
    }
    /* now the change: nothing below fails */
    int32_t kept = 0;
    for (int32_t i = 0; i < v->n; i++) {
        if (fate[i] != STAYS) {
            version_free(&v->v[i]);
            continue;
        }
        if (i == newest && !v->v[i].replaced) {
            v->v[i].replaced = 1;
            v->v[i].at = now;
        }
        v->v[kept++] = v->v[i];
    }
    v->n = kept;
    v->v[v->n++] = fresh;
    free(fate);
    /* an object the new version is, or names, is not deleted, whatever
     * an earlier step listed */
    int at = names_find(&v->due, object);
    if (at >= 0) names_remove_at(&v->due, at);
    for (int32_t k = 0; k < fresh.names.n; k++) {
        at = names_find(&v->due, fresh.names.s[k]);
        if (at >= 0) names_remove_at(&v->due, at);
    }
    return REPORT_OK;
}

const char *pqc_versions_due(const PqcVersions *v, int32_t index) {
    if (!v || index < 0 || index >= v->due.n) return NULL;
    return v->due.s[index];
}

int pqc_versions_deleted(PqcVersions *v, const char *object, Report *report) {
    if (!report_begin(report)) return REPORT_INVALID;
    if (!v || !object)
        return report_fail(report, REPORT_INVALID,
                           "pqc_versions_deleted: a NULL argument");
    int at = names_find(&v->due, object);
    if (at < 0)
        return report_fail(report, REPORT_INVALID,
                           "pqc_versions_deleted: %s is not in the list of "
                           "objects to delete", object);
    names_remove_at(&v->due, at);
    return REPORT_OK;
}

/* ---- the upload ---------------------------------------------------------- */

enum {
    PASS_NONE = 0,      /* no pass yet */
    PASS_OBJECTS,       /* the objects the newest local version names */
    PASS_VERSION,       /* that version itself */
    PASS_STORED,        /* tell the host what the remote holds now */
    PASS_DELETES,       /* the replaced versions that are due */
    PASS_AFTER,         /* uploaded: is the file ended? */
    PASS_LOCAL,         /* assemble it locally */
    PASS_REMOTE,        /* assemble it on the remote */
    PASS_OVER,          /* nothing more in this pass */
    PASS_DONE           /* the file is finished in both copies */
};

enum { LOCAL_NOTHING, LOCAL_FINISHED, LOCAL_SAME, LOCAL_NEWER };

struct PqcUpload {
    char name[PQC_NAME_MAX];
    PqcVersions *versions;        /* the remote versions this stored */
    Names there;                  /* objects known to be on the remote */
    uint8_t *uploaded;             /* the newest version stored there */
    size_t uploaded_len;
    uint64_t uploaded_rows;
    char uploaded_object[PQC_OBJECT_MAX];

    int phase;
    int local;                    /* LOCAL_* of this pass */
    int live;
    int upload;                   /* the object at idx must be copied */
    uint8_t *raw;                 /* this pass's local version */
    size_t raw_len;
    uint64_t raw_rows;
    char raw_object[PQC_OBJECT_MAX];
    PqcReader *reader;            /* over raw */
    uint32_t idx;                 /* the next object of reader to look at */
    char doing[PQC_OBJECT_MAX + 64];
};

static void pass_free(PqcUpload *m) {
    pqc_reader_free(m->reader);
    m->reader = NULL;
    free(m->raw);
    m->raw = NULL;
    m->raw_len = 0;
    m->upload = 0;
    m->idx = 0;
    m->doing[0] = 0;
}

int pqc_upload_new(const char *name, double keep_seconds, PqcUpload **out,
                   Report *report) {
    if (!report_begin(report)) return REPORT_INVALID;
    if (!name || !out)
        return report_fail(report, REPORT_INVALID,
                           "pqc_upload_new: a NULL argument");
    *out = NULL;
    size_t len = strlen(name);
    if (!ends_with(name, len, PARQUET) || len <= strlen(PARQUET))
        return report_fail(report, REPORT_INVALID,
                           "pqc_upload_new: the file's name %.80s does not "
                           "end in .parquet", name);
    if (len >= PQC_NAME_MAX)
        return report_fail(report, REPORT_INVALID,
                           "pqc_upload_new: the file's name is longer than "
                           "%d bytes", PQC_NAME_MAX - 1);
    PqcUpload *m = calloc(1, sizeof *m);
    if (!m) return no_memory(report);
    memcpy(m->name, name, len + 1);
    if (pqc_versions_new(keep_seconds, &m->versions, report) != REPORT_OK) {
        free(m);
        return report->status;
    }
    *out = m;
    return REPORT_OK;
}

void pqc_upload_free(PqcUpload *m) {
    if (!m) return;
    pass_free(m);
    pqc_versions_free(m->versions);
    names_free(&m->there);
    free(m->uploaded);
    free(m);
}

/* The last part of a path: a checkpoint records the file's own name. */
static const char *base_name(const char *path) {
    const char *slash = strrchr(path, '/');
    return slash ? slash + 1 : path;
}

int pqc_upload_local(PqcUpload *m, const char *object, const uint8_t *ckpt,
                     size_t len, int final_exists, int live, Report *report) {
    if (!report_begin(report)) return REPORT_INVALID;
    if (!m || (object && !ckpt) || (!object && len))
        return report_fail(report, REPORT_INVALID,
                           "pqc_upload_local: a NULL argument");
    if (m->phase == PASS_DONE) return REPORT_OK;
    pass_free(m);
    m->live = live != 0;
    if (!object) {
        m->local = final_exists ? LOCAL_FINISHED : LOCAL_NOTHING;
        m->phase = PASS_AFTER;
        return REPORT_OK;
    }
    if (strlen(object) >= PQC_OBJECT_MAX) {
        m->phase = PASS_OVER;
        return report_fail(report, REPORT_INVALID,
                           "pqc_upload_local: the version's name is longer "
                           "than %d bytes", PQC_OBJECT_MAX - 1);
    }
    if (m->uploaded && len == m->uploaded_len &&
        memcmp(ckpt, m->uploaded, len) == 0) {
        m->local = LOCAL_SAME;    /* the remote has it: nothing to send */
        m->phase = PASS_DELETES;
        return REPORT_OK;
    }
    m->phase = PASS_OVER;         /* until the version is known to hold */
    Report inner;
    PqcReader *r = NULL;
    if (pqc_reader_open(ckpt, len, &r, &inner) != REPORT_OK)
        return report_fail(report, inner.status, "%s: %s", object,
                           inner.error);
    if (strcmp(pqc_reader_name(r), base_name(m->name)) != 0) {
        report_fail(report, REPORT_REFUSED,
                    "%s: the checkpoint is of the file %s, and this upload "
                    "is of %s: not sent", object, pqc_reader_name(r),
                    m->name);
        pqc_reader_free(r);
        return report->status;
    }
    m->raw = malloc(len ? len : 1);
    if (!m->raw) {
        pqc_reader_free(r);
        return no_memory(report);
    }
    memcpy(m->raw, ckpt, len);
    m->raw_len = len;
    m->raw_rows = pqc_reader_watermark(r);
    snprintf(m->raw_object, sizeof m->raw_object, "%s", object);
    m->reader = r;
    m->local = LOCAL_NEWER;
    m->phase = PASS_OBJECTS;
    return REPORT_OK;
}

static void step_set(PqcUploadStep *step, int action, const char *name,
                     uint64_t size, int flag) {
    step->action = action;
    step->flag = flag;
    step->size = size;
    snprintf(step->name, sizeof step->name, "%s", name ? name : "");
}

int pqc_upload_next(PqcUpload *m, PqcUploadStep *step, Report *report) {
    if (!report_begin(report)) return REPORT_INVALID;
    if (!m || !step)
        return report_fail(report, REPORT_INVALID,
                           "pqc_upload_next: a NULL argument");
    step_set(step, PQC_UPLOAD_NOTHING, NULL, 0, 0);
    for (;;) {
        switch (m->phase) {
        case PASS_NONE:
            return report_fail(report, REPORT_INVALID,
                               "pqc_upload_next before pqc_upload_local: "
                               "no pass has begun");
        case PASS_OBJECTS: {
            uint32_t n = pqc_reader_n_chunks(m->reader);
            const char *name = NULL;
            uint64_t length = 0;
            uint32_t crc;
            while (m->idx < n) {
                pqc_reader_chunk(m->reader, m->idx, &name, &length, &crc);
                if (m->upload || names_find(&m->there, name) < 0) break;
                m->idx++;         /* sent or seen: it never changes */
            }
            if (m->idx >= n) {
                m->phase = PASS_VERSION;
                continue;
            }
            if (m->upload) {
                snprintf(m->doing, sizeof m->doing,
                         "uploading object %s (%.1f MB)", name,
                         (double)length / 1e6);
                step_set(step, PQC_UPLOAD_OBJECT, name, length, 0);
            } else {
                step_set(step, PQC_UPLOAD_STAT, name, length, 0);
            }
            return REPORT_OK;
        }
        case PASS_VERSION:
            snprintf(m->doing, sizeof m->doing,
                     "uploading checkpoint %s (%.1f MB)", m->raw_object,
                     (double)m->raw_len / 1e6);
            step_set(step, PQC_UPLOAD_VERSION, m->raw_object,
                     m->raw_len, 0);
            return REPORT_OK;
        case PASS_STORED:
            step_set(step, PQC_UPLOAD_STORED, m->uploaded_object,
                     m->uploaded_rows, 0);
            return REPORT_OK;
        case PASS_DELETES: {
            const char *due = pqc_versions_due(m->versions, 0);
            if (!due) {
                m->phase = PASS_AFTER;
                continue;
            }
            step_set(step, PQC_UPLOAD_DELETE, due, 0, 0);
            return REPORT_OK;
        }
        case PASS_AFTER:
            if (m->live)
                m->phase = PASS_OVER;
            else if (m->local == LOCAL_NEWER || m->local == LOCAL_SAME)
                m->phase = PASS_LOCAL;
            else
                m->phase = PASS_REMOTE;
            continue;
        case PASS_LOCAL:
            step_set(step, PQC_UPLOAD_ASSEMBLE_LOCAL, m->name, 0, 0);
            return REPORT_OK;
        case PASS_REMOTE:
            snprintf(m->doing, sizeof m->doing, "assembling %s on the store",
                     m->name);
            step_set(step, PQC_UPLOAD_ASSEMBLE_REMOTE, m->name, 0,
                     m->uploaded != NULL);
            return REPORT_OK;
        case PASS_DONE:
            step_set(step, PQC_UPLOAD_DONE, m->name, 0, 0);
            return REPORT_OK;
        default:                  /* PASS_OVER */
            return REPORT_OK;
        }
    }
}

int pqc_upload_remote_has(PqcUpload *m, int exists, uint64_t size,
                          Report *report) {
    if (!report_begin(report)) return REPORT_INVALID;
    if (!m)
        return report_fail(report, REPORT_INVALID,
                           "pqc_upload_remote_has: a NULL argument");
    uint32_t n = m->phase == PASS_OBJECTS ? pqc_reader_n_chunks(m->reader) : 0;
    if (m->phase != PASS_OBJECTS || m->upload || m->idx >= n)
        return report_fail(report, REPORT_INVALID,
                           "pqc_upload_remote_has: the upload did not ask "
                           "about an object");
    const char *name;
    uint64_t length;
    uint32_t crc;
    pqc_reader_chunk(m->reader, m->idx, &name, &length, &crc);
    if (exists && size == length) {
        if (names_add(&m->there, name)) return no_memory(report);
        m->idx++;
    } else {
        m->upload = 1;            /* not there, or not whole: copy it */
    }
    return REPORT_OK;
}

int pqc_upload_did(PqcUpload *m, Report *report) {
    if (!report_begin(report)) return REPORT_INVALID;
    if (!m)
        return report_fail(report, REPORT_INVALID,
                           "pqc_upload_did: a NULL argument");
    m->doing[0] = 0;
    switch (m->phase) {
    case PASS_OBJECTS: {
        uint32_t n = pqc_reader_n_chunks(m->reader);
        if (!m->upload || m->idx >= n) break;
        const char *name;
        uint64_t length;
        uint32_t crc;
        pqc_reader_chunk(m->reader, m->idx, &name, &length, &crc);
        if (names_add(&m->there, name)) return no_memory(report);
        m->upload = 0;
        m->idx++;
        return REPORT_OK;
    }
    case PASS_VERSION: {
        /* the remote holds it: that is so whatever follows */
        free(m->uploaded);
        m->uploaded = m->raw;
        m->uploaded_len = m->raw_len;
        m->uploaded_rows = m->raw_rows;
        memcpy(m->uploaded_object, m->raw_object, sizeof m->uploaded_object);
        m->raw = NULL;
        m->raw_len = 0;
        m->phase = PASS_STORED;
        if (pqc_versions_stored(m->versions, m->uploaded_object, m->uploaded,
                                m->uploaded_len, report) != REPORT_OK)
            return report->status;
        /* what the remote may lack again: an object that goes with
         * its last version */
        for (int32_t i = m->there.n - 1; i >= 0; i--)
            if (!pqc_versions_names(m->versions, m->there.s[i]))
                names_remove_at(&m->there, i);
        return REPORT_OK;
    }
    case PASS_STORED:
        m->phase = PASS_DELETES;
        return REPORT_OK;
    case PASS_DELETES: {
        const char *due = pqc_versions_due(m->versions, 0);
        if (!due) break;
        return pqc_versions_deleted(m->versions, due, report);
    }
    case PASS_LOCAL:
        m->phase = PASS_REMOTE;
        return REPORT_OK;
    case PASS_REMOTE:
        pass_free(m);
        m->phase = PASS_DONE;
        return REPORT_OK;
    default:
        break;
    }
    return report_fail(report, REPORT_INVALID,
                       "pqc_upload_did: the upload gave no step to do");
}

void pqc_upload_failed(PqcUpload *m) {
    if (!m || m->phase == PASS_NONE || m->phase == PASS_DONE) return;
    pass_free(m);
    m->phase = PASS_OVER;
}

int pqc_upload_version(const PqcUpload *m, const uint8_t **ckpt, size_t *len,
                       const char **object) {
    if (!m || !m->uploaded) return 0;
    if (ckpt) *ckpt = m->uploaded;
    if (len) *len = m->uploaded_len;
    if (object) *object = m->uploaded_object;
    return 1;
}

const char *pqc_upload_doing(const PqcUpload *m) {
    return m ? m->doing : "";
}

/* ---- the sweep at start --------------------------------------------------- */

int pqc_sweep_local(int final_exists, int has_remote, int keeps_checkpoints) {
    if (final_exists) {
        /* it finished before the crash. Its versions were kept for
         * readers only when this copy is the store they read: of a
         * spool's copy no reader was told. */
        return (!has_remote && keeps_checkpoints) ? PQC_SWEEP_AWAIT_CATALOG
                                                  : PQC_SWEEP_DISCARD;
    }
    return has_remote ? PQC_SWEEP_UPLOAD : PQC_SWEEP_FINISH;
}

int pqc_sweep_remote(int final_exists, int keeps_checkpoints, int in_flight) {
    if (final_exists)
        return keeps_checkpoints ? PQC_SWEEP_AWAIT_CATALOG : PQC_SWEEP_DISCARD;
    /* its local file was assembled and the crash came before the
     * remote one: an upload finishes it from the remote's own objects */
    return in_flight ? PQC_SWEEP_NOTHING : PQC_SWEEP_UPLOAD;
}

/* ---- the assembly of a file ---------------------------------------------- */

enum {
    FIN_START = 0,
    FIN_FINAL,          /* is the finished file there? */
    FIN_READ,           /* the versions, the newest first */
    FIN_OBJECTS,        /* each object the version names: there, whole? */
    FIN_ASSEMBLE,
    FIN_LIST,           /* the objects under the file's prefix */
    FIN_DELETE,
    FIN_DONE
};

typedef struct {
    int64_t seq;
    char object[PQC_OBJECT_MAX];
} FinVersion;

struct PqcFinish {
    char name[PQC_NAME_MAX];
    int mode, keep;
    FinVersion *version;          /* the versions the place holds, oldest first */
    int32_t n_versions, cap_versions;
    int sorted;
    uint8_t *raw;                 /* the bytes of the version in use */
    PqcReader *reader;            /* over raw */
    char used[PQC_OBJECT_MAX];    /* its object name */
    int given;                    /* the caller gave it: no read */
    int32_t at;                   /* the version to read */
    char refused[REPORT_LINE_TEXT - 192];
    uint32_t idx;                 /* the object to ask about */
    Names todo;                   /* the deletes, in order */
    int32_t del;
    int phase;
    int asked;                    /* the action of the step not yet answered */
    int assembled;
};

static int version_order(const void *a, const void *b) {
    const FinVersion *x = a, *y = b;
    if (x->seq != y->seq) return x->seq < y->seq ? -1 : 1;
    return strcmp(x->object, y->object);
}

int pqc_finish_new(const char *name, int mode, int keep_checkpoints,
                   PqcFinish **out, Report *report) {
    if (!report_begin(report)) return REPORT_INVALID;
    if (out) *out = NULL;
    if (!name || !out)
        return report_fail(report, REPORT_INVALID,
                           "pqc_finish_new: a NULL argument");
    if (mode != PQC_FINISH_ASSEMBLE_FILE && mode != PQC_FINISH_DISCARD)
        return report_fail(report, REPORT_INVALID,
                           "pqc_finish_new: mode %d is not one", mode);
    if (strlen(name) >= PQC_NAME_MAX)
        return report_fail(report, REPORT_INVALID,
                           "pqc_finish_new: the file's name is longer than "
                           "%d bytes", PQC_NAME_MAX - 1);
    PqcFinish *f = calloc(1, sizeof *f);
    if (!f) return no_memory(report);
    snprintf(f->name, sizeof f->name, "%s", name);
    f->mode = mode;
    f->keep = keep_checkpoints != 0;
    *out = f;
    return REPORT_OK;
}

void pqc_finish_free(PqcFinish *f) {
    if (!f) return;
    free(f->version);
    free(f->raw);
    pqc_reader_free(f->reader);
    names_free(&f->todo);
    free(f);
}

int pqc_finish_version(PqcFinish *f, const char *object, Report *report) {
    if (!report_begin(report)) return REPORT_INVALID;
    if (!f || !object)
        return report_fail(report, REPORT_INVALID,
                           "pqc_finish_version: a NULL argument");
    if (f->phase != FIN_START)
        return report_fail(report, REPORT_INVALID,
                           "pqc_finish_version: the steps have begun");
    char of[PQC_OBJECT_MAX];
    int64_t seq;
    if (!pqc_ckpt_object_parse(object, of, &seq) || strcmp(of, f->name) != 0)
        return report_fail(report, REPORT_INVALID,
                           "pqc_finish_version: %.200s is not a checkpoint "
                           "version of %.200s", object, f->name);
    if (f->n_versions == f->cap_versions) {
        int32_t cap = f->cap_versions ? f->cap_versions * 2 : 4;
        FinVersion *v = realloc(f->version, (size_t)cap * sizeof *v);
        if (!v) return no_memory(report);
        f->version = v;
        f->cap_versions = cap;
    }
    FinVersion *v = &f->version[f->n_versions++];
    v->seq = seq;
    snprintf(v->object, sizeof v->object, "%s", object);
    return REPORT_OK;
}

/* The bytes are the version in use from now: they must be a checkpoint
 * of this file. */
static int fin_use(PqcFinish *f, const char *object, const uint8_t *ckpt,
                   size_t len, Report *report) {
    PqcReader *r = NULL;
    Report inner;
    if (pqc_reader_open(ckpt, len, &r, &inner) != REPORT_OK)
        return report_fail(report, inner.status, "%s", inner.error);
    uint8_t *raw = malloc(len ? len : 1);
    if (!raw) {
        pqc_reader_free(r);
        return no_memory(report);
    }
    memcpy(raw, ckpt, len);
    pqc_reader_free(f->reader);
    free(f->raw);
    f->reader = r;
    f->raw = raw;
    snprintf(f->used, sizeof f->used, "%s", object);
    return REPORT_OK;
}

int pqc_finish_given(PqcFinish *f, const char *object, const uint8_t *ckpt,
                     size_t len, Report *report) {
    if (!report_begin(report)) return REPORT_INVALID;
    if (!f || !object || !ckpt)
        return report_fail(report, REPORT_INVALID,
                           "pqc_finish_given: a NULL argument");
    if (f->phase != FIN_START || f->mode != PQC_FINISH_ASSEMBLE_FILE)
        return report_fail(report, REPORT_INVALID,
                           "pqc_finish_given: not before the first step of "
                           "an assembly");
    if (strlen(object) >= PQC_OBJECT_MAX)
        return report_fail(report, REPORT_INVALID,
                           "pqc_finish_given: the version's name is longer "
                           "than %d bytes", PQC_OBJECT_MAX - 1);
    Report inner;
    if (fin_use(f, object, ckpt, len, &inner) != REPORT_OK)
        return report_fail(report, inner.status, "%.200s: %s", object,
                           inner.error);
    f->given = 1;
    return REPORT_OK;
}

/* The version in use names this file, or the report says it does not. */
static int fin_paired(const PqcFinish *f, Report *report) {
    const char *named = pqc_reader_name(f->reader);
    if (strcmp(named, base_name(f->name)) == 0) return REPORT_OK;
    return report_fail(report, REPORT_REFUSED,
                       "%.200s: checkpoint names '%.100s', paired with "
                       "'%.100s' -- mispaired, refusing",
                       f->used, named, f->name);
}

/* The deletes, in their order: the objects the host listed are in todo
 * already; then every version but the one in use; then that one. */
static int fin_plan_deletes(PqcFinish *f) {
    for (int32_t i = 0; i < f->n_versions; i++)
        if (strcmp(f->version[i].object, f->used) != 0 &&
            names_push(&f->todo, f->version[i].object))
            return -1;
    if (f->used[0] && names_push(&f->todo, f->used)) return -1;
    return 0;
}

int pqc_finish_next(PqcFinish *f, PqcFinishStep *step, Report *report) {
    if (!report_begin(report)) return REPORT_INVALID;
    if (!f || !step)
        return report_fail(report, REPORT_INVALID,
                           "pqc_finish_next: a NULL argument");
    step_set(step, PQC_FINISH_DONE, NULL, 0, 0);
    f->asked = 0;
    for (;;) {
        switch (f->phase) {
        case FIN_START:
            if (!f->sorted) {
                if (f->n_versions)
                    qsort(f->version, (size_t)f->n_versions,
                          sizeof *f->version, version_order);
                f->sorted = 1;
            }
            if (f->given) {
                if (fin_paired(f, report) != REPORT_OK) return report->status;
                f->phase = FIN_OBJECTS;
                continue;
            }
            f->phase = FIN_FINAL;
            continue;
        case FIN_FINAL:
            step_set(step, PQC_FINISH_STAT, f->name, 0, 1);
            break;
        case FIN_READ:
            step_set(step, PQC_FINISH_READ, f->version[f->at].object, 0, 0);
            break;
        case FIN_OBJECTS: {
            if (f->idx >= pqc_reader_n_chunks(f->reader)) {
                f->phase = FIN_ASSEMBLE;
                continue;
            }
            const char *name;
            uint64_t length;
            uint32_t crc;
            pqc_reader_chunk(f->reader, f->idx, &name, &length, &crc);
            step_set(step, PQC_FINISH_STAT, name, length, 0);
            break;
        }
        case FIN_ASSEMBLE:
            step_set(step, PQC_FINISH_ASSEMBLE, f->name,
                     (uint64_t)pqc_reader_n_chunks(f->reader) + 1, 0);
            break;
        case FIN_LIST: {
            char prefix[PQC_OBJECT_MAX];
            snprintf(prefix, sizeof prefix, "%s.chunks/", f->name);
            step_set(step, PQC_FINISH_LIST, prefix, 0, 0);
            break;
        }
        case FIN_DELETE:
            if (f->del >= f->todo.n) {
                f->phase = FIN_DONE;
                continue;
            }
            step_set(step, PQC_FINISH_DELETE, f->todo.s[f->del], 0, 0);
            break;
        default:
            step_set(step, PQC_FINISH_DONE, f->name, 0, f->assembled);
            return REPORT_OK;
        }
        f->asked = step->action;
        return REPORT_OK;
    }
}

int pqc_finish_has(PqcFinish *f, int exists, uint64_t size, Report *report) {
    if (!report_begin(report)) return REPORT_INVALID;
    if (!f)
        return report_fail(report, REPORT_INVALID,
                           "pqc_finish_has: a NULL argument");
    if (f->asked != PQC_FINISH_STAT)
        return report_fail(report, REPORT_INVALID,
                           "pqc_finish_has: no object was asked about");
    f->asked = 0;
    if (f->phase == FIN_OBJECTS) {
        const char *name;
        uint64_t length;
        uint32_t crc;
        pqc_reader_chunk(f->reader, f->idx, &name, &length, &crc);
        if (!exists || size != length)
            return report_fail(report, REPORT_REFUSED,
                               "%.300s: missing or wrong size -- cannot "
                               "finalize", name);
        f->idx++;
        return REPORT_OK;
    }
    /* the finished file */
    if (f->mode == PQC_FINISH_DISCARD) {
        if (!exists)
            return report_fail(report, REPORT_REFUSED,
                               "discard_checkpoints('%.200s'): the final "
                               "file does not exist -- the checkpoints are "
                               "its only copy", f->name);
        if (f->n_versions)
            snprintf(f->used, sizeof f->used, "%s",
                     f->version[f->n_versions - 1].object);
        f->phase = FIN_LIST;
        return REPORT_OK;
    }
    if (!f->n_versions) {
        if (!exists)
            return report_fail(report, REPORT_REFUSED,
                               "finalize('%.200s'): nothing there", f->name);
        f->phase = FIN_DONE;            /* it was finished before */
        return REPORT_OK;
    }
    if (exists)
        return report_fail(report, REPORT_REFUSED,
                           "%.200s: BOTH final and checkpoint exist -- "
                           "refusing to guess", f->name);
    f->at = f->n_versions - 1;
    f->phase = FIN_READ;
    return REPORT_OK;
}

int pqc_finish_read(PqcFinish *f, const uint8_t *ckpt, size_t len,
                    Report *report) {
    if (!report_begin(report)) return REPORT_INVALID;
    if (!f || (!ckpt && len))
        return report_fail(report, REPORT_INVALID,
                           "pqc_finish_read: a NULL argument");
    if (!ckpt) ckpt = (const uint8_t *)"";      /* an empty object */
    if (f->asked != PQC_FINISH_READ)
        return report_fail(report, REPORT_INVALID,
                           "pqc_finish_read: no version was asked for");
    f->asked = 0;
    const char *object = f->version[f->at].object;
    Report inner;
    int rc = fin_use(f, object, ckpt, len, &inner);
    if (rc == REPORT_NO_MEMORY) return no_memory(report);
    if (rc != REPORT_OK) {
        /* not a checkpoint: say so, and go to the version before it */
        size_t n = strlen(f->refused);
        snprintf(f->refused + n, sizeof f->refused - n, "%s%.200s: %s",
                 n ? "; " : "", object, inner.error);
        if (f->at == 0)
            return report_fail(report, REPORT_REFUSED,
                               "finalize('%.100s'): no checkpoint version "
                               "holds -- %.300s", f->name, f->refused);
        f->at--;
        return REPORT_OK;
    }
    if (fin_paired(f, report) != REPORT_OK) return report->status;
    if (f->refused[0])
        report_line(report, &g_clock, REPORT_WARNING,
                    "finalize %.200s: newer version(s) refused, assembling "
                    "from %.200s: %s", f->name, f->used, f->refused);
    f->phase = FIN_OBJECTS;
    return REPORT_OK;
}

int pqc_finish_source(const PqcFinish *f, uint32_t index, const char **name,
                      int64_t *offset, int64_t *length) {
    if (!f || !f->reader || !name || !offset || !length) return 0;
    uint32_t n = pqc_reader_n_chunks(f->reader);
    if (index < n) {
        uint64_t len;
        uint32_t crc;
        pqc_reader_chunk(f->reader, index, name, &len, &crc);
        *offset = -1;                   /* the object, whole */
        *length = -1;
        return 1;
    }
    if (index > n) return 0;
    uint64_t off, len;
    pqc_reader_data(f->reader, &off, &len);
    *name = f->used;
    *offset = (int64_t)off;
    *length = (int64_t)len;
    return 1;
}

int pqc_finish_listed(PqcFinish *f, const char *object, Report *report) {
    if (!report_begin(report)) return REPORT_INVALID;
    if (!f || !object)
        return report_fail(report, REPORT_INVALID,
                           "pqc_finish_listed: a NULL argument");
    if (f->asked != PQC_FINISH_LIST)
        return report_fail(report, REPORT_INVALID,
                           "pqc_finish_listed: no list was asked for");
    if (names_push(&f->todo, object)) return no_memory(report);
    return REPORT_OK;
}

int pqc_finish_did(PqcFinish *f, Report *report) {
    if (!report_begin(report)) return REPORT_INVALID;
    if (!f)
        return report_fail(report, REPORT_INVALID,
                           "pqc_finish_did: a NULL argument");
    switch (f->asked) {
    case PQC_FINISH_ASSEMBLE:
        f->assembled = 1;
        f->phase = f->keep ? FIN_DONE : FIN_LIST;
        break;
    case PQC_FINISH_LIST:
        if (fin_plan_deletes(f)) return no_memory(report);
        f->phase = FIN_DELETE;
        break;
    case PQC_FINISH_DELETE:
        f->del++;
        break;
    default:
        return report_fail(report, REPORT_INVALID,
                           "pqc_finish_did: no step to do was given");
    }
    f->asked = 0;
    return REPORT_OK;
}

/* ---- the parts of an assembly -------------------------------------------- */

int pqc_plan_parts(const uint64_t *sizes, uint32_t n, uint64_t part_min,
                   PqcPart **out, uint32_t *n_out, Report *report) {
    if (!report_begin(report)) return REPORT_INVALID;
    if (out) *out = NULL;
    if (n_out) *n_out = 0;
    if (!out || !n_out || (n && !sizes))
        return report_fail(report, REPORT_INVALID,
                           "pqc_plan_parts: a NULL argument");
    PqcPart *part = malloc((n ? n : 1) * sizeof *part);
    if (!part) return no_memory(report);
    uint32_t parts = 0, run = 0;        /* run: the sources of the open part */
    uint64_t run_bytes = 0;
    for (uint32_t i = 0; i < n; i++) {
        if (!run && sizes[i] >= part_min) {
            part[parts++] = (PqcPart){.first = i, .count = 1, .copy = 1};
            continue;
        }
        run++;
        run_bytes += sizes[i];
        if (run_bytes >= part_min) {
            part[parts++] = (PqcPart){.first = i + 1 - run, .count = run};
            run = 0;
            run_bytes = 0;
        }
    }
    if (run) part[parts++] = (PqcPart){.first = n - run, .count = run};
    if (parts > PQC_MAX_PARTS) {
        free(part);
        return report_fail(report, REPORT_REFUSED,
                           "assemble: %u parts exceeds %d", parts,
                           PQC_MAX_PARTS);
    }
    *out = part;
    *n_out = parts;
    return REPORT_OK;
}
