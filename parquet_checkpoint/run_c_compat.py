"""C core suite for parquet_checkpoint, driven from Python.

Run:  python3 parquet_checkpoint/run_c_compat.py

The writer's rules live once, in the C core (core/pqc.c). This suite
builds core/libpqc.dylib (make) and proves the core against things
that are not the core:

  - CRC-32: equal to zlib's
  - thrift offset rebase: pyarrow reads the footer before and after;
    every offset moved by the delta and nothing else changed, on many
    random schemas and deltas
  - the core driven directly, the way a host would: row groups and
    growing tails; the Python CheckpointReader (its own parser) reads
    every checkpoint and gets exactly the rows written; the final file
    is exact
  - the package's CheckpointWriter, a host over the core: given the
    same writes it leaves byte for byte the objects the direct driver
    left, and the C reader opens its checkpoints
  - refusals: corrupt checkpoint, tampered CRC
"""

import io
import os
import random
import subprocess
import sys
import traceback

import cffi
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from parquet_checkpoint import (ckpt_object, ckpt_versions,
                                CheckpointReader, CheckpointWriter, FakeS3,
                                finalize)

CORE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'core')

r = subprocess.run(['make', '-C', CORE, 'libpqc.dylib'],
                   capture_output=True, text=True)
if r.returncode != 0:
    print(r.stdout + r.stderr)
    sys.exit(1)

ffi = cffi.FFI()
ffi.cdef("""
typedef struct PqcWriter PqcWriter;
typedef struct PqcReader PqcReader;
PqcWriter *pqc_writer_new(const char *final_name, uint64_t chunk_bytes,
                          char *err, size_t errlen);
void pqc_writer_free(PqcWriter *w);
int64_t pqc_add_group(PqcWriter *w, const uint8_t *mini, size_t len,
                      char *err, size_t errlen);
void pqc_next_span(PqcWriter *w, uint64_t new_rows,
                   uint64_t *first_row, uint64_t *rows);
int64_t pqc_add_unfinished(PqcWriter *w, const uint8_t *mini, size_t len,
                           char *err, size_t errlen);
void pqc_drop_unfinished(PqcWriter *w);
int pqc_next_object(PqcWriter *w, char name[256], const uint8_t **data,
                    uint64_t *len, uint32_t *crc);
void pqc_object_stored(PqcWriter *w);
uint8_t *pqc_build_checkpoint(PqcWriter *w, const uint8_t *empty_mini,
                              size_t empty_len, uint64_t *out_len,
                              uint64_t *out_watermark,
                              char *err, size_t errlen);
uint64_t pqc_rows_final(const PqcWriter *w);
uint64_t pqc_rows_unfinished(const PqcWriter *w);
uint32_t pqc_chunks_cut(const PqcWriter *w);
uint64_t pqc_logical_end(const PqcWriter *w);
PqcReader *pqc_reader_open(const uint8_t *ckpt, size_t len,
                           char *err, size_t errlen);
void pqc_reader_free(PqcReader *r);
const char *pqc_reader_name(const PqcReader *r);
uint64_t pqc_reader_watermark(const PqcReader *r);
uint32_t pqc_reader_n_chunks(const PqcReader *r);
int pqc_reader_chunk(const PqcReader *r, uint32_t idx, const char **name,
                     uint64_t *length, uint32_t *crc);
void pqc_reader_data(const PqcReader *r, uint64_t *offset,
                     uint64_t *length);
uint32_t pqc_crc32(const uint8_t *data, size_t len);
uint8_t *pqc_shift_footer(const uint8_t *thrift, size_t len,
                          int64_t delta, size_t *out_len,
                          char *err, size_t errlen);
void pqc_free(void *p);
""")
C = ffi.dlopen(os.path.join(CORE, 'libpqc.dylib'))

ERR = ffi.new('char[512]')


def c_shift(thrift, delta):
    n = ffi.new('size_t *')
    p = C.pqc_shift_footer(thrift, len(thrift), delta, n, ERR, 512)
    if p == ffi.NULL:
        raise RuntimeError(ffi.string(ERR).decode())
    out = bytes(ffi.buffer(p, n[0]))
    C.pqc_free(p)
    return out


def encode_mini(table, **opts):
    buf = io.BytesIO()
    w = pq.ParquetWriter(buf, table.schema, **opts)
    w.write_table(table)
    w.close()
    return buf.getvalue()


def footer_of(mini):
    import struct as _s
    flen = _s.unpack('<I', mini[-8:-4])[0]
    return mini[len(mini) - 8 - flen:len(mini) - 8]


class CWriter:
    """Drive the C core the way a host would; store into a FakeS3."""

    def __init__(self, name, sink, chunk_bytes):
        self.name = name
        self.sink = sink
        self._seq = 0
        self._w = C.pqc_writer_new(name.encode(), chunk_bytes, ERR, 512)
        if self._w == ffi.NULL:
            raise RuntimeError(ffi.string(ERR).decode())

    def add_group(self, mini):
        n = C.pqc_add_group(self._w, mini, len(mini), ERR, 512)
        if n < 0:
            raise RuntimeError(ffi.string(ERR).decode())
        self.store_objects()
        return n

    def next_span(self, new_rows):
        first, rows = ffi.new('uint64_t *'), ffi.new('uint64_t *')
        C.pqc_next_span(self._w, new_rows, first, rows)
        return first[0], rows[0]

    def add_unfinished(self, mini):
        n = C.pqc_add_unfinished(self._w, mini, len(mini), ERR, 512)
        if n < 0:
            raise RuntimeError(ffi.string(ERR).decode())
        self.store_objects()
        return n

    def drop_unfinished(self):
        C.pqc_drop_unfinished(self._w)

    def store_objects(self):
        name = ffi.new('char[256]')
        data = ffi.new('const uint8_t **')
        length = ffi.new('uint64_t *')
        crc = ffi.new('uint32_t *')
        while True:
            rc = C.pqc_next_object(self._w, name, data, length, crc)
            if rc < 0:
                raise RuntimeError("pqc_next_object failed")
            if rc == 0:
                return
            body = bytes(ffi.buffer(data[0], length[0]))
            assert C.pqc_crc32(body, len(body)) == crc[0]
            self.sink.put(ffi.string(name).decode(), body)   # durable FIRST
            C.pqc_object_stored(self._w)

    def checkpoint(self, empty_mini=None):
        n = ffi.new('uint64_t *')
        wm = ffi.new('uint64_t *')
        t = empty_mini if empty_mini is not None else ffi.NULL
        tl = len(empty_mini) if empty_mini is not None else 0
        p = C.pqc_build_checkpoint(self._w, t, tl, n, wm, ERR, 512)
        if p == ffi.NULL:
            raise RuntimeError(ffi.string(ERR).decode())
        ck = bytes(ffi.buffer(p, n[0]))
        C.pqc_free(p)
        self._seq += 1                               # the host names versions
        self.sink.put(ckpt_object(self.name, self._seq), ck)
        return ck, wm[0]

    def free(self):
        C.pqc_writer_free(self._w)


F = pa.field
SCHEMAS = [
    pa.schema([F('n', pa.int64(), nullable=False),
               F('x', pa.float64(), nullable=False),
               F('s', pa.string(), nullable=False)]),
    pa.schema([F('a', pa.int32()), F('b', pa.bool_()),
               F('c', pa.binary())]),
    pa.schema([F('t', pa.timestamp('us')), F('v', pa.float32()),
               F('tags', pa.list_(pa.utf8()))]),
]


def rand_table(rng, schema, n):
    cols = {}
    for f in schema:
        if pa.types.is_integer(f.type):
            cols[f.name] = [rng.randrange(-2**31, 2**31) for _ in range(n)]
        elif pa.types.is_floating(f.type):
            cols[f.name] = [rng.random() * 1e6 for _ in range(n)]
        elif pa.types.is_boolean(f.type):
            cols[f.name] = [rng.random() < 0.5 for _ in range(n)]
        elif pa.types.is_timestamp(f.type):
            cols[f.name] = [rng.randrange(0, 2**40) for _ in range(n)]
        elif pa.types.is_list(f.type):
            cols[f.name] = [[f'v{rng.randrange(100)}'
                             for _ in range(rng.randrange(3))]
                            for _ in range(n)]
        elif pa.types.is_binary(f.type):
            cols[f.name] = [rng.randbytes(rng.randrange(20))
                            for _ in range(n)]
        else:
            cols[f.name] = [f'row-{i}-{rng.randrange(1000)}'
                            for i in range(n)]
    return pa.table(cols, schema=schema)


passed = failed = 0
errors = []


def test(name):
    def deco(fn):
        global passed, failed
        try:
            fn()
            print(f"  PASS  {name}", flush=True)
            passed += 1
        except Exception as e:
            print(f"  FAIL  {name}: {e}", flush=True)
            traceback.print_exc()
            failed += 1
            errors.append(name)
    return deco


@test("crc32 matches zlib")
def _():
    import zlib
    rng = random.Random(1)
    for _ in range(50):
        b = rng.randbytes(rng.randrange(0, 5000))
        assert C.pqc_crc32(b, len(b)) == zlib.crc32(b)


def footer_facts(thrift):
    """What pyarrow reads from a bare footer: for each column chunk its
    offsets, and everything else about it."""
    import struct as _s
    md = pq.read_metadata(pa.BufferReader(
        b'PAR1' + thrift + _s.pack('<I', len(thrift)) + b'PAR1'))
    offsets, rest = [], []
    for g in range(md.num_row_groups):
        rg = md.row_group(g)
        for c in range(rg.num_columns):
            col = rg.column(c).to_dict()
            offsets.append((col.pop('data_page_offset'),
                            col.pop('dictionary_page_offset'),
                            col.pop('file_offset')))
            rest.append(col)
        rest.append((rg.num_rows, rg.total_byte_size))
    return offsets, rest, md.num_rows, md.schema.to_arrow_schema()


@test("thrift shift: pyarrow reads the footer before and after; every "
      "offset moved by the delta, nothing else changed, x120")
def _():
    rng = random.Random(42)
    moved = 0
    for k in range(120):
        schema = SCHEMAS[k % len(SCHEMAS)]
        t = rand_table(rng, schema, rng.randrange(1, 300))
        opts = {}
        if k % 3 == 0:
            opts['compression'] = 'zstd'
        if k % 4 == 0:
            opts['write_page_checksum'] = True
        if k % 5 == 0:
            opts['write_page_index'] = True   # exercises the drop path
        mini = encode_mini(t, **opts)
        delta = rng.choice([0, 1, 4, 1000, 2**31, 2**40])
        # the shift drops the page-index references; compare with the
        # same footer shifted by nothing
        was = footer_facts(c_shift(footer_of(mini), 0))
        now = footer_facts(c_shift(footer_of(mini), delta))
        assert now[1:] == was[1:], f"iteration {k}: more than offsets changed"
        for a, b in zip(was[0], now[0]):
            for before, after in zip(a, b):
                if before is None:      # no dictionary page
                    assert after is None, (k, before, after)
                else:
                    assert after == before + delta, (k, before, after, delta)
                    moved += 1
        # and back again: the bytes of before
        back = c_shift(c_shift(footer_of(mini), delta), -delta)
        assert back == c_shift(footer_of(mini), 0), f"iteration {k}: not undone"
    assert moved > 300, moved


@test("shift refuses trailing garbage")
def _():
    t = rand_table(random.Random(2), SCHEMAS[0], 10)
    thrift = footer_of(encode_mini(t))
    try:
        c_shift(thrift + b'\x00garbage', 4)
        raise AssertionError("must refuse trailing bytes")
    except RuntimeError as e:
        assert 'trailing' in str(e)


@test("the core driven directly, and the package's writer over it: the "
      "Python reader gets exactly the rows at every checkpoint; both "
      "hosts leave the same objects byte for byte; the C reader opens them")
def _():
    rng = random.Random(7)
    opts = {'compression': 'none', 'write_page_checksum': True}
    c_sink, p_sink = FakeS3(part_min=4096), FakeS3(part_min=4096)
    cw = CWriter('par.parquet', c_sink, 32 * 1024)
    pw = CheckpointWriter('par.parquet', SCHEMAS[0], sink=p_sink,
                          chunk_bytes=32 * 1024,
                          checkpoint_every_write=False, **opts)

    def rows(lo, hi):
        return pa.table({'n': list(range(lo, hi)),
                         'x': [j * 0.5 for j in range(lo, hi)],
                         's': [f'row-{j}' for j in range(lo, hi)]},
                        schema=SCHEMAS[0])
    final = 0                            # rows in final row groups
    checkpoints = merges = 0
    for k in range(14):
        tail = 0                         # rows after them, in the tail
        for c in range(rng.randrange(0, 30)):
            fresh = rng.randrange(1, 60)
            first, span = cw.next_span(fresh)
            tail += fresh
            assert first + span == final + tail, (first, span, final, tail)
            if span > fresh:
                merges += 1
            cw.add_unfinished(encode_mini(rows(first, first + span), **opts))
            ck_c, wm = cw.checkpoint()
            pw.checkpoint(rows(final, final + tail))
            assert wm == pw.rows_checkpointed == final + tail
            checkpoints += 1
            # the core's checkpoint through the PYTHON reader: exact rows
            got = CheckpointReader(ck_c, c_sink).table()
            assert got.column('n').to_pylist() == list(range(final + tail))
        n = tail + rng.randrange(50, 800)        # the row group holds the tail
        cw.add_group(encode_mini(rows(final, final + n), **opts))
        pw.write_table(rows(final, final + n))
        final += n
        ck_c, wm = cw.checkpoint()
        pw.checkpoint()
        assert wm == pw.rows_checkpointed == final
        got = CheckpointReader(ck_c, c_sink).table()
        assert got.column('n').to_pylist() == list(range(final))
    assert checkpoints > 100 and merges > 5, (checkpoints, merges)
    # both hosts: the same objects, the same bytes
    c_all, p_all = c_sink.list(''), p_sink.list('')
    assert c_all == p_all, (c_all, p_all)
    assert len(c_sink.list('par.parquet.chunks/')) > 100
    for name in c_all:
        assert c_sink.get(name) == p_sink.get(name), f"{name} differs"
    # the package writer's newest checkpoint through the C reader
    ck_p = p_sink.get(ckpt_versions(p_sink, 'par.parquet')[-1][1])
    rd = C.pqc_reader_open(ck_p, len(ck_p), ERR, 512)
    assert rd != ffi.NULL, ffi.string(ERR).decode()
    assert ffi.string(C.pqc_reader_name(rd)).decode() == 'par.parquet'
    assert C.pqc_reader_watermark(rd) == pw.rows_checkpointed == final
    assert C.pqc_reader_n_chunks(rd) == len(CheckpointReader(ck_p).manifest)
    off, ln = ffi.new('uint64_t *'), ffi.new('uint64_t *')
    C.pqc_reader_data(rd, off, ln)
    assert ck_p[off[0]:off[0] + ln[0]] == ck_p[off[0]:]     # data = suffix
    C.pqc_reader_free(rd)
    # finalize both sets of objects: the same final file, every row
    cw.free()
    del pw
    assert finalize('par.parquet', c_sink)[0] == 'recovered'
    assert finalize('par.parquet', p_sink)[0] == 'recovered'
    fc, fp = c_sink.get('par.parquet'), p_sink.get('par.parquet')
    assert fc == fp, "final files differ"
    tt = pq.read_table(pa.BufferReader(fc))
    assert tt.column('n').to_pylist() == list(range(final))
    print(f"        ({checkpoints} checkpoints, {merges} merges)")


@test("empty checkpoint: C accepts an empty mini as the data region")
def _():
    sink = FakeS3(part_min=4096)
    cw = CWriter('e.parquet', sink, 5 << 20)
    empty = encode_mini(pa.table({'n': []},
                                 schema=pa.schema([F('n', pa.int64())])))
    ck, wm = cw.checkpoint(empty)
    assert wm == 0
    rd = CheckpointReader(ck, sink)
    assert rd.num_rows == 0 and rd.table().num_rows == 0
    cw.free()


@test("C reader refuses corruption like the Python one")
def _():
    sink = FakeS3(part_min=4096)
    cw = CWriter('r.parquet', sink, 5 << 20)
    t = rand_table(random.Random(3), SCHEMAS[0], 100)
    cw.add_group(encode_mini(t, compression='none'))
    ck, _ = cw.checkpoint()
    cw.free()
    bad = bytearray(ck)
    bad[25] ^= 0xFF
    assert C.pqc_reader_open(bytes(bad), len(bad), ERR, 512) == ffi.NULL
    assert b'CRC' in ffi.string(ERR)
    assert C.pqc_reader_open(b'JUNK' + ck[4:], len(ck), ERR, 512) == ffi.NULL


print(f"\n{'=' * 50}")
print(f"Results: {passed} passed, {failed} failed")
if errors:
    print(f"Failed: {', '.join(errors)}")
    sys.exit(1)
print("All tests passed!")
