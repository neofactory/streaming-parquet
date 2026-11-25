"""CFFI binding of the parquet_checkpoint C core: src/core/pqc.c, whose
header pqc.h is the format's spec.

The core does the format work and no I/O. A host encodes rows as a
complete parquet file (a "mini file", offsets from 0) and hands it the
bytes; the core rebases the footer's offsets to the rows' place in the
final file, decides which rows each object holds (the new rows of a
checkpoint, or a merge), cuts chunks, builds the combined footer and
the checkpoint object, and hands bytes back for the host to store. The
Go host (src/go/pqc) and the DuckDB extension use the same core; this
module is what the Python writer calls, so the format's rules exist
once.

The library is built with `make` and the system C compiler the first
time this module is imported in a checkout, from its sources in
src/core into this package, like arrow_ring's; `pip install` builds it
at install.
"""

import os
import re
import subprocess

import cffi

_PKG_DIR = os.path.dirname(os.path.abspath(__file__))
# the library is beside this file; in a checkout its sources are in src/core
_CORE_DIR = os.path.join(os.path.dirname(os.path.dirname(_PKG_DIR)), 'core')
_LIB_PATH = os.path.join(_PKG_DIR, 'libpqc.dylib')

NAME_MAX = 256                  # PQC_NAME_MAX

ffi = cffi.FFI()
_CDEF = """
typedef struct {
    int32_t level;
    double wall;
    double mono;
    char text[1024];
} ReportLine;
typedef struct {
    int32_t status;
    int32_t lines;
    int32_t dropped;
    char error[512];
    ReportLine line[4];
} Report;
typedef struct PqcWriter PqcWriter;
int pqc_writer_new(const char *final_name, uint64_t chunk_bytes,
                   PqcWriter **out, Report *report);
void pqc_writer_free(PqcWriter *w);
int pqc_add_group(PqcWriter *w, const uint8_t *mini, size_t len,
                  int64_t *rows, Report *report);
void pqc_next_span(PqcWriter *w, uint64_t new_rows,
                   uint64_t *first_row, uint64_t *rows);
int pqc_add_unfinished(PqcWriter *w, const uint8_t *mini, size_t len,
                       int64_t *rows, Report *report);
void pqc_drop_unfinished(PqcWriter *w);
int pqc_next_object(PqcWriter *w, char name[256], const uint8_t **data,
                    uint64_t *len, uint32_t *crc, int *have, Report *report);
void pqc_object_stored(PqcWriter *w);
int pqc_build_checkpoint(PqcWriter *w, const uint8_t *empty_mini,
                         size_t empty_len, uint8_t **out, uint64_t *out_len,
                         uint64_t *out_watermark, Report *report);
uint64_t pqc_rows_final(const PqcWriter *w);
uint64_t pqc_rows_unfinished(const PqcWriter *w);
uint32_t pqc_chunks_cut(const PqcWriter *w);
uint64_t pqc_logical_end(const PqcWriter *w);
double pqc_dead_after(double beat_seconds);
void pqc_free(void *p);

typedef struct PqcVersions PqcVersions;
int pqc_ckpt_object(const char *name, int64_t seq, char out[320],
                    Report *report);
int pqc_ckpt_object_parse(const char *object, char name[320], int64_t *seq);
int pqc_versions_new(double keep_seconds, PqcVersions **out, Report *report);
void pqc_versions_free(PqcVersions *v);
int pqc_versions_stored(PqcVersions *v, const char *object,
                        const uint8_t *ckpt, size_t len, Report *report);
const char *pqc_versions_due(const PqcVersions *v, int32_t index);
int pqc_versions_deleted(PqcVersions *v, const char *object, Report *report);
int pqc_versions_names(const PqcVersions *v, const char *object);

typedef struct PqcMirror PqcMirror;
typedef struct {
    int32_t action;
    int32_t flag;
    uint64_t size;
    char name[320];
} PqcMirrorStep;
int pqc_mirror_new(const char *name, double keep_seconds, PqcMirror **out,
                   Report *report);
void pqc_mirror_free(PqcMirror *m);
int pqc_mirror_local(PqcMirror *m, const char *object, const uint8_t *ckpt,
                     size_t len, int final_exists, int live, Report *report);
int pqc_mirror_next(PqcMirror *m, PqcMirrorStep *step, Report *report);
int pqc_mirror_remote_has(PqcMirror *m, int exists, uint64_t size,
                          Report *report);
int pqc_mirror_did(PqcMirror *m, Report *report);
void pqc_mirror_failed(PqcMirror *m);
int pqc_mirror_shipped(const PqcMirror *m, const uint8_t **ckpt, size_t *len,
                       const char **object);
const char *pqc_mirror_doing(const PqcMirror *m);
int pqc_sweep_local(int final_exists, int has_remote, int keeps_checkpoints);
int pqc_sweep_remote(int final_exists, int keeps_checkpoints, int in_flight);
void pqc_test_set_clock(int fixed, double wall, double mono);

typedef struct PqcFinish PqcFinish;
int pqc_finish_new(const char *name, int mode, int keep_checkpoints,
                   PqcFinish **out, Report *report);
void pqc_finish_free(PqcFinish *f);
int pqc_finish_version(PqcFinish *f, const char *object, Report *report);
int pqc_finish_given(PqcFinish *f, const char *object, const uint8_t *ckpt,
                     size_t len, Report *report);
int pqc_finish_next(PqcFinish *f, PqcMirrorStep *step, Report *report);
int pqc_finish_has(PqcFinish *f, int exists, uint64_t size, Report *report);
int pqc_finish_read(PqcFinish *f, const uint8_t *ckpt, size_t len,
                    Report *report);
int pqc_finish_listed(PqcFinish *f, const char *object, Report *report);
int pqc_finish_did(PqcFinish *f, Report *report);
int pqc_finish_source(const PqcFinish *f, uint32_t index, const char **name,
                      int64_t *offset, int64_t *length);

typedef struct {
    uint32_t first, count;
    int32_t copy;
} PqcPart;
int pqc_plan_parts(const uint64_t *sizes, uint32_t n, uint64_t part_min,
                   PqcPart **out, uint32_t *n_out, Report *report);
"""
ffi.cdef(_CDEF)


def _build_if_stale():
    """In a checkout, build the library from src/core into this package
    when a source is newer than it. An installed package has the
    library and no sources: nothing to do."""
    sources = [os.path.join(_CORE_DIR, n) for n in ('pqc.c', 'pqc_mirror.c', 'pqc.h', 'report.h')]
    if not all(os.path.exists(s) for s in sources):
        if os.path.exists(_LIB_PATH):
            return
        raise ImportError(
            f"{__package__}: no {_LIB_PATH} and no sources in {_CORE_DIR} "
            f"to build it from; install the package again")
    if (os.path.exists(_LIB_PATH) and os.path.getmtime(_LIB_PATH)
            >= max(os.path.getmtime(s) for s in sources)):
        return
    subprocess.run(['make', '-C', _CORE_DIR, f'OUT={_PKG_DIR}', 'libpqc'],
                   check=True, capture_output=True)


_build_if_stale()
lib = ffi.dlopen(_LIB_PATH)
# Look up every function and parse every type now, as arrow_ring's
# binding does. cffi does both on first use under a lock of its own
# that is not reentrant; the garbage collector can run inside it, and a
# destructor there (Writer.__del__ -> pqc_writer_free) finding its
# function for the first time waited on the lock its own thread held.
for _name in sorted(set(re.findall(r"\b(pqc_\w+)\s*\(", _CDEF))):
    getattr(lib, _name)
for _type in ('Report *', 'PqcWriter **', f'char[{NAME_MAX}]',
              'const uint8_t **', 'uint8_t **', 'uint64_t *', 'int64_t *',
              'uint32_t *', 'int *', 'PqcVersions **', 'PqcMirror **',
              'PqcMirrorStep *', 'char[320]', 'size_t *', 'const char **',
              'PqcFinish **', 'PqcPart **', 'uint64_t[]'):
    ffi.new(_type, 1) if _type.endswith('[]') else ffi.new(_type)


def dead_after(beat_seconds=0.0):
    """Seconds of silence after which a writer with this beat is dead
    (pqc_dead_after: three beats, at least 30 seconds)."""
    return float(lib.pqc_dead_after(float(beat_seconds)))


class CoreError(Exception):
    """The core refused: its message says what."""


class Writer:
    """One PqcWriter. One owner at a time, like a file handle: the core
    takes no lock. How a call reports is src/core/report.h: the writer
    has one Report, and a status that is not 0 becomes a CoreError
    (MemoryError for no memory) with the report's text."""

    _NO_MEMORY = 4

    def __init__(self, final_name, chunk_bytes):
        self._report = ffi.new('Report *')
        self._w = ffi.NULL
        out = ffi.new('PqcWriter **')
        self._check(lib.pqc_writer_new(final_name.encode('utf-8'),
                                       chunk_bytes, out, self._report))
        self._w = out[0]

    def _check(self, rc):
        if rc == 0:
            return
        text = ffi.string(self._report.error).decode('utf-8', 'replace')
        if rc == self._NO_MEMORY:
            raise MemoryError(text)
        raise CoreError(text)

    def add_group(self, mini):
        """Append the final row group(s) of a complete parquet file.
        They replace every unfinished object. Returns the rows added."""
        rows = ffi.new('int64_t *')
        self._check(lib.pqc_add_group(self._w, mini, len(mini), rows,
                                      self._report))
        return rows[0]

    def next_span(self, new_rows):
        """A checkpoint is due and new_rows rows are in no object yet:
        (first_row, rows), the rows of the file to encode as one row
        group. The new rows alone, or a merge that ends with them."""
        first = ffi.new('uint64_t *')
        rows = ffi.new('uint64_t *')
        lib.pqc_next_span(self._w, new_rows, first, rows)
        return int(first[0]), int(rows[0])

    def add_unfinished(self, mini):
        """The span of the last next_span, as a complete parquet file.
        Returns its rows."""
        rows = ffi.new('int64_t *')
        self._check(lib.pqc_add_unfinished(self._w, mini, len(mini), rows,
                                           self._report))
        return rows[0]

    def drop_unfinished(self):
        """Forget the unfinished objects: the next checkpoint claims
        the final rows only."""
        lib.pqc_drop_unfinished(self._w)

    def next_object(self):
        """The next object to store as (name, bytes), or None when none
        is owed. The same object comes again until object_stored()."""
        name = ffi.new(f'char[{NAME_MAX}]')
        data = ffi.new('const uint8_t **')
        length = ffi.new('uint64_t *')
        crc = ffi.new('uint32_t *')
        have = ffi.new('int *')
        self._check(lib.pqc_next_object(self._w, name, data, length, crc,
                                        have, self._report))
        if not have[0]:
            return None
        return (ffi.string(name).decode('utf-8'),
                bytes(ffi.buffer(data[0], length[0])))

    def object_stored(self):
        """The object next_object() gave is in the store: the core may
        now name it in a checkpoint."""
        lib.pqc_object_stored(self._w)

    def build_checkpoint(self, empty_mini=None):
        """(checkpoint object bytes, rows covered): the list of the
        stored objects and the footer. empty_mini: for a writer with no
        rows, a complete empty parquet file; None otherwise."""
        n = ffi.new('uint64_t *')
        watermark = ffi.new('uint64_t *')
        out = ffi.new('uint8_t **')
        if empty_mini is None:
            rc = lib.pqc_build_checkpoint(self._w, ffi.NULL, 0, out, n,
                                          watermark, self._report)
        else:
            rc = lib.pqc_build_checkpoint(self._w, empty_mini,
                                          len(empty_mini), out, n, watermark,
                                          self._report)
        self._check(rc)
        try:
            return bytes(ffi.buffer(out[0], n[0])), int(watermark[0])
        finally:
            lib.pqc_free(out[0])

    @property
    def rows_final(self):
        return int(lib.pqc_rows_final(self._w))

    @property
    def rows_unfinished(self):
        return int(lib.pqc_rows_unfinished(self._w))

    @property
    def logical_end(self):
        """Where the next final row group begins: the bytes of the
        file so far, without its footer."""
        return int(lib.pqc_logical_end(self._w))

    @property
    def chunks_cut(self):
        return int(lib.pqc_chunks_cut(self._w))

    def free(self):
        if self._w != ffi.NULL:
            lib.pqc_writer_free(self._w)
            self._w = ffi.NULL

    def __del__(self):
        try:
            self.free()
        except Exception:
            pass


# ---- checkpoint versions, the mirror, the sweep (src/core/pqc.h) -----------

OBJECT_MAX = 320                # PQC_OBJECT_MAX
_NO_MEMORY = 4

_thread = __import__('threading').local()


def _report():
    """This thread's Report (src/core/report.h)."""
    r = getattr(_thread, 'report', None)
    if r is None:
        r = _thread.report = ffi.new('Report *')
    return r


def _check(rc, r):
    if rc == 0:
        return
    text = ffi.string(r.error).decode('utf-8', 'replace')
    if rc == _NO_MEMORY:
        raise MemoryError(text)
    raise CoreError(text)


def ckpt_object(name, seq):
    """The object name of version `seq` of file `name`'s checkpoint."""
    out = ffi.new(f'char[{OBJECT_MAX}]')
    r = _report()
    _check(lib.pqc_ckpt_object(name.encode('utf-8'), int(seq), out, r), r)
    return ffi.string(out).decode('utf-8')


def ckpt_object_parse(obj):
    """(name, seq) for a checkpoint version's object name, else None."""
    name = ffi.new(f'char[{OBJECT_MAX}]')
    seq = ffi.new('int64_t *')
    if not lib.pqc_ckpt_object_parse(obj.encode('utf-8'), name, seq):
        return None
    return ffi.string(name).decode('utf-8'), int(seq[0])


class Versions:
    """The checkpoint versions of one file in one place (PqcVersions):
    which replaced versions, and which objects, are due for deletion.
    The host stores and deletes."""

    def __init__(self, keep_seconds):
        self._v = ffi.NULL
        out = ffi.new('PqcVersions **')
        r = _report()
        _check(lib.pqc_versions_new(float(keep_seconds), out, r), r)
        self._v = out[0]

    def stored(self, obj, ckpt):
        """The version `obj` with the bytes `ckpt` is stored."""
        r = _report()
        _check(lib.pqc_versions_stored(self._v, obj.encode('utf-8'), ckpt,
                                       len(ckpt), r), r)

    def due(self):
        """The objects to delete now, in order."""
        out, i = [], 0
        while True:
            p = lib.pqc_versions_due(self._v, i)
            if p == ffi.NULL:
                return out
            out.append(ffi.string(p).decode('utf-8'))
            i += 1

    def deleted(self, obj):
        r = _report()
        _check(lib.pqc_versions_deleted(self._v, obj.encode('utf-8'), r), r)

    def free(self):
        if self._v != ffi.NULL:
            lib.pqc_versions_free(self._v)
            self._v = ffi.NULL

    def __del__(self):
        self.free()


# PqcMirrorAction
MIRROR_NOTHING, MIRROR_STAT, MIRROR_UPLOAD, MIRROR_UPLOAD_VERSION = 0, 1, 2, 3
MIRROR_STORED, MIRROR_DELETE, MIRROR_ASSEMBLE_LOCAL = 4, 5, 6
MIRROR_ASSEMBLE_REMOTE, MIRROR_DONE = 7, 8


class Mirror:
    """The mirror of one file from a local copy to a remote one
    (PqcMirror): the rules of what is sent, in which order, what is
    deleted, and how a file ends. The host does each step."""

    def __init__(self, name, keep_seconds):
        self._m = ffi.NULL
        out = ffi.new('PqcMirror **')
        r = _report()
        _check(lib.pqc_mirror_new(name.encode('utf-8'), float(keep_seconds),
                                  out, r), r)
        self._m = out[0]
        self._step = ffi.new('PqcMirrorStep *')

    def local(self, obj, ckpt, final_exists, live):
        """A pass begins: the newest local version and its bytes (None,
        None for none), whether the finished file is there, whether the
        writer still has the file open."""
        r = _report()
        _check(lib.pqc_mirror_local(
            self._m, obj.encode('utf-8') if obj is not None else ffi.NULL,
            ckpt if ckpt is not None else ffi.NULL,
            len(ckpt) if ckpt is not None else 0,
            1 if final_exists else 0, 1 if live else 0, r), r)

    def next(self):
        """(action, name, size, flag) of the next step."""
        r = _report()
        _check(lib.pqc_mirror_next(self._m, self._step, r), r)
        st = self._step
        return (st.action, ffi.string(st.name).decode('utf-8'), int(st.size),
                int(st.flag))

    def remote_has(self, exists, size):
        r = _report()
        _check(lib.pqc_mirror_remote_has(self._m, 1 if exists else 0,
                                         int(size), r), r)

    def did(self):
        r = _report()
        _check(lib.pqc_mirror_did(self._m, r), r)

    def failed(self):
        lib.pqc_mirror_failed(self._m)

    def shipped(self):
        """(bytes, object) of the newest version stored on the remote,
        or (None, None)."""
        ck = ffi.new('const uint8_t **')
        n = ffi.new('size_t *')
        obj = ffi.new('const char **')
        if not lib.pqc_mirror_shipped(self._m, ck, n, obj):
            return None, None
        return bytes(ffi.buffer(ck[0], n[0])), ffi.string(obj[0]).decode('utf-8')

    @property
    def doing(self):
        """What the mirror is doing on the remote now, or None."""
        text = ffi.string(lib.pqc_mirror_doing(self._m)).decode('utf-8')
        return text or None

    def free(self):
        if self._m != ffi.NULL:
            lib.pqc_mirror_free(self._m)
            self._m = ffi.NULL

    def __del__(self):
        self.free()


# PqcSweep
SWEEP_NOTHING, SWEEP_FINISH, SWEEP_MIRROR = 0, 1, 2
SWEEP_AWAIT_CATALOG, SWEEP_DISCARD = 3, 4


def sweep_local(final_exists, has_remote, keeps_checkpoints):
    return lib.pqc_sweep_local(int(bool(final_exists)), int(bool(has_remote)),
                               int(bool(keeps_checkpoints)))


def sweep_remote(final_exists, keeps_checkpoints, in_flight):
    return lib.pqc_sweep_remote(int(bool(final_exists)),
                                int(bool(keeps_checkpoints)),
                                int(bool(in_flight)))


# pqc_finish_new's mode, and PqcFinishAction
FINISH_ASSEMBLE_FILE, FINISH_DISCARD = 0, 1
FINISH_READ, FINISH_STAT, FINISH_ASSEMBLE = 1, 2, 3
FINISH_LIST, FINISH_DELETE, FINISH_DONE = 4, 5, 6


class Finish:
    """The assembly of one file from its checkpoint in one place
    (PqcFinish): which version, the checks, the sources, and the
    deletes in their order. The host does each step."""

    def __init__(self, name, mode, keep_checkpoints):
        self._f = ffi.NULL
        out = ffi.new('PqcFinish **')
        r = _report()
        _check(lib.pqc_finish_new(name.encode('utf-8'), int(mode),
                                  1 if keep_checkpoints else 0, out, r), r)
        self._f = out[0]
        self._step = ffi.new('PqcMirrorStep *')

    def version(self, obj):
        """The place holds the checkpoint version `obj`."""
        r = _report()
        _check(lib.pqc_finish_version(self._f, obj.encode('utf-8'), r), r)

    def given(self, obj, ckpt):
        """The caller has the newest version's bytes in hand."""
        r = _report()
        _check(lib.pqc_finish_given(self._f, obj.encode('utf-8'), ckpt,
                                    len(ckpt), r), r)

    def next(self):
        """(action, name, size, flag) of the next step."""
        r = _report()
        _check(lib.pqc_finish_next(self._f, self._step, r), r)
        st = self._step
        return (st.action, ffi.string(st.name).decode('utf-8'), int(st.size),
                int(st.flag))

    def has(self, exists, size):
        r = _report()
        _check(lib.pqc_finish_has(self._f, 1 if exists else 0, int(size), r), r)

    def read(self, ckpt):
        """The bytes of the version asked for. Returns the lines for
        the log, each (level, text)."""
        r = _report()
        _check(lib.pqc_finish_read(self._f, ckpt, len(ckpt), r), r)
        return [(int(r.line[i].level),
                 ffi.string(r.line[i].text).decode('utf-8', 'replace'))
                for i in range(r.lines)]

    def listed(self, obj):
        r = _report()
        _check(lib.pqc_finish_listed(self._f, obj.encode('utf-8'), r), r)

    def did(self):
        r = _report()
        _check(lib.pqc_finish_did(self._f, r), r)

    def sources(self, count):
        """The sources of the ASSEMBLE step: [(name, offset, length)],
        offset and length -1 for a whole object."""
        name = ffi.new('const char **')
        off = ffi.new('int64_t *')
        length = ffi.new('int64_t *')
        out = []
        for i in range(count):
            if not lib.pqc_finish_source(self._f, i, name, off, length):
                raise CoreError(f"the assembly has no source {i}")
            out.append((ffi.string(name[0]).decode('utf-8'), int(off[0]),
                        int(length[0])))
        return out

    def free(self):
        if self._f != ffi.NULL:
            lib.pqc_finish_free(self._f)
            self._f = ffi.NULL

    def __del__(self):
        self.free()


def plan_parts(sizes, part_min):
    """The parts of a multipart assembly of sources of these sizes
    (pqc_plan_parts): [(first, count, copy)]."""
    n = len(sizes)
    arr = ffi.new('uint64_t[]', [int(x) for x in sizes] or 1)
    out = ffi.new('PqcPart **')
    n_out = ffi.new('uint32_t *')
    r = _report()
    _check(lib.pqc_plan_parts(arr, n, int(part_min), out, n_out, r), r)
    try:
        return [(int(out[0][i].first), int(out[0][i].count),
                 bool(out[0][i].copy)) for i in range(n_out[0])]
    finally:
        lib.pqc_free(out[0])


def test_set_clock(wall, mono):
    """For tests: the time the core sees."""
    lib.pqc_test_set_clock(1, float(wall), float(mono))


def test_system_clock():
    lib.pqc_test_set_clock(0, 0.0, 0.0)
