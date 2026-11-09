"""CFFI binding of the parquet_checkpoint C core: core/pqc.c, whose
header pqc.h is the format's spec.

The core does the format work and no I/O. A host encodes each row group
as a complete parquet file (a "mini file", offsets from 0) and hands it
the bytes; the core rebases the footer's offsets to the group's place
in the final file, cuts chunks, builds the combined footer and the
checkpoint object, and hands bytes back for the host to store. The Go
host (go/pqc) and the DuckDB extension use the same core; this module
is what the Python writer calls, so the format's rules exist once.

The library is built with `make` and the system C compiler the first
time this module is imported, next to its sources (core/), like
arrow_ring's.
"""

import os
import re
import subprocess

import cffi

_CORE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'core')
_LIB_PATH = os.path.join(_CORE_DIR, 'libpqc.dylib')

NAME_MAX = 256                  # PQC_NAME_MAX
_ERRLEN = 512

ffi = cffi.FFI()
_CDEF = """
typedef struct PqcWriter PqcWriter;
PqcWriter *pqc_writer_new(const char *final_name, uint64_t chunk_bytes,
                          char *err, size_t errlen);
void pqc_writer_free(PqcWriter *w);
int64_t pqc_add_group(PqcWriter *w, const uint8_t *mini, size_t len,
                      char *err, size_t errlen);
int pqc_next_chunk(PqcWriter *w, char name[256], const uint8_t **data,
                   uint64_t *len, uint32_t *crc);
void pqc_chunk_stored(PqcWriter *w);
uint8_t *pqc_build_checkpoint(PqcWriter *w, const uint8_t *tail_mini,
                              size_t tail_len, uint64_t *out_len,
                              uint64_t *out_watermark,
                              char *err, size_t errlen);
uint64_t pqc_rows_final(const PqcWriter *w);
uint32_t pqc_chunks_cut(const PqcWriter *w);
uint64_t pqc_logical_end(const PqcWriter *w);
void pqc_free(void *p);
"""
ffi.cdef(_CDEF)


def _build_lib():
    src = os.path.join(_CORE_DIR, 'pqc.c')
    hdr = os.path.join(_CORE_DIR, 'pqc.h')
    if (not os.path.exists(_LIB_PATH)
            or os.path.getmtime(_LIB_PATH) < max(os.path.getmtime(src),
                                                 os.path.getmtime(hdr))):
        subprocess.run(['make', 'libpqc.dylib'], cwd=_CORE_DIR, check=True,
                       capture_output=True)


_build_lib()
lib = ffi.dlopen(_LIB_PATH)
# Look up every function and parse every type now, as arrow_ring's
# binding does. cffi does both on first use under a lock of its own
# that is not reentrant; the garbage collector can run inside it, and a
# destructor there (Writer.__del__ -> pqc_writer_free) finding its
# function for the first time waited on the lock its own thread held.
for _name in sorted(set(re.findall(r"\b(pqc_\w+)\s*\(", _CDEF))):
    getattr(lib, _name)
for _type in (f'char[{_ERRLEN}]', f'char[{NAME_MAX}]', 'const uint8_t **',
              'uint64_t *', 'uint32_t *'):
    ffi.new(_type)


class CoreError(Exception):
    """The core refused: its message says what."""


class Writer:
    """One PqcWriter. One owner at a time, like a file handle: the core
    takes no lock."""

    def __init__(self, final_name, chunk_bytes):
        self._err = ffi.new(f'char[{_ERRLEN}]')
        self._w = lib.pqc_writer_new(final_name.encode('utf-8'),
                                     chunk_bytes, self._err, _ERRLEN)
        if self._w == ffi.NULL:
            raise CoreError(self._message())

    def _message(self):
        return ffi.string(self._err).decode('utf-8', 'replace')

    def add_group(self, mini):
        """Append the row group(s) of a complete parquet file. Returns
        the rows added."""
        rows = lib.pqc_add_group(self._w, mini, len(mini), self._err, _ERRLEN)
        if rows < 0:
            raise CoreError(self._message())
        return rows

    def next_chunk(self):
        """The next cut chunk as (name, bytes), or None when none is
        ready. The same chunk comes again until chunk_stored()."""
        name = ffi.new(f'char[{NAME_MAX}]')
        data = ffi.new('const uint8_t **')
        length = ffi.new('uint64_t *')
        crc = ffi.new('uint32_t *')
        rc = lib.pqc_next_chunk(self._w, name, data, length, crc)
        if rc < 0:
            raise CoreError("out of memory cutting a chunk")
        if rc == 0:
            return None
        return (ffi.string(name).decode('utf-8'),
                bytes(ffi.buffer(data[0], length[0])))

    def chunk_stored(self):
        """The chunk next_chunk() gave is in the store: the core may
        now name it in a checkpoint."""
        lib.pqc_chunk_stored(self._w)

    def build_checkpoint(self, tail_mini=None):
        """(checkpoint object bytes, rows covered). tail_mini: a
        complete parquet file of rows not yet added as a row group, or
        None."""
        n = ffi.new('uint64_t *')
        watermark = ffi.new('uint64_t *')
        if tail_mini is None:
            p = lib.pqc_build_checkpoint(self._w, ffi.NULL, 0, n, watermark,
                                         self._err, _ERRLEN)
        else:
            p = lib.pqc_build_checkpoint(self._w, tail_mini, len(tail_mini),
                                         n, watermark, self._err, _ERRLEN)
        if p == ffi.NULL:
            raise CoreError(self._message())
        try:
            return bytes(ffi.buffer(p, n[0])), int(watermark[0])
        finally:
            lib.pqc_free(p)

    @property
    def rows_final(self):
        return int(lib.pqc_rows_final(self._w))

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
