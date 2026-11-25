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
              'uint32_t *', 'int *'):
    ffi.new(_type)


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
