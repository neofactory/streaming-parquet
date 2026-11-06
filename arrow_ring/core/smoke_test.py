"""Smoke test for the C core, driven through pyarrow's C interfaces.

Builds a schema in pyarrow, exports it via the C Data Interface into
ar_ring_new, writes rows through the leaf setters, and imports record
sets back through ArrowArrayStream. Run after `make`:

    python3 packages/arrow_ring/core/smoke_test.py
"""

import os
import sys

import cffi
import pyarrow as pa

HERE = os.path.dirname(os.path.abspath(__file__))

ffi = cffi.FFI()
ffi.cdef("""
typedef int ArStatus;
typedef struct ArRing ArRing;
typedef struct ArRecordSet ArRecordSet;
typedef struct ArRingOptions ArRingOptions;

ArStatus ar_ring_new(const void *schema, int64_t capacity,
                     const ArRingOptions *opts, ArRing **out);
void ar_ring_free(ArRing *ring);
const char *ar_last_error(const ArRing *ring);
int32_t ar_leaf_index(const ArRing *ring, const char *path);
ArStatus ar_row_begin(ArRing *ring, int64_t block_ms, int64_t *out_seq);
ArStatus ar_set_i64(ArRing *ring, int32_t leaf, int64_t v);
ArStatus ar_set_f64(ArRing *ring, int32_t leaf, double v);
ArStatus ar_set_bool(ArRing *ring, int32_t leaf, _Bool v);
ArStatus ar_set_bytes(ArRing *ring, int32_t leaf, const void *d, int64_t n);
ArStatus ar_set_list(ArRing *ring, int32_t leaf, const void *e, int64_t n);
ArStatus ar_set_null(ArRing *ring, int32_t leaf);
ArStatus ar_commit(ArRing *ring, int64_t *out_count);
ArStatus ar_get_f64(const ArRing *ring, int64_t seq, int32_t leaf,
                    double *out, _Bool *is_null);
ArStatus ar_get_var(const ArRing *ring, int64_t seq, int32_t leaf,
                    const void **data, int64_t *n, _Bool *is_null);
int64_t ar_oldest_seq(const ArRing *ring);
int64_t ar_newest_seq(const ArRing *ring);
uint64_t ar_leaf_data_address(const ArRing *ring, int32_t leaf);
ArStatus ar_relation(ArRing *ring, double duration_s, ArRecordSet **out);
int64_t ar_recordset_lo(const ArRecordSet *rs);
int64_t ar_recordset_hi(const ArRecordSet *rs);
ArStatus ar_recordset_stream(ArRecordSet *rs, void *out_stream);
void ar_recordset_release(ArRecordSet *rs);
""")
lib = ffi.dlopen(os.path.join(HERE, "libarrow_ring.dylib"))

AR_OK, AR_PIN_STALLED = 0, 4
checks = []


def check(name, cond):
    checks.append((name, cond))
    print(f"  {'PASS' if cond else 'FAIL'} {name}")


def ok(st):
    if st != AR_OK:
        raise RuntimeError(ffi.string(lib.ar_last_error(ffi.NULL)).decode())


def new_ring(schema, capacity):
    cs = ffi.new("char[]", 128)   # ArrowSchema is ~72 bytes; padded
    schema._export_to_c(int(ffi.cast("uintptr_t", cs)))
    out = ffi.new("ArRing **")
    ok(lib.ar_ring_new(cs, capacity, ffi.NULL, out))
    return out[0]


def table_of(rs):
    stream = ffi.new("char[]", 80)   # ArrowArrayStream is 40 bytes; padded
    ok(lib.ar_recordset_stream(rs, stream))
    reader = pa.RecordBatchReader._import_from_c(
        int(ffi.cast("uintptr_t", stream)))
    return reader.read_all()


F = pa.field
schema = pa.schema([
    F('tv', pa.float64(), nullable=False),
    F('servo', pa.struct([F('pos', pa.int32(), nullable=False),
                          F('vel', pa.int32(), nullable=False)]),
      nullable=False),
    F('mark', pa.int32(), nullable=True),
    F('label', pa.utf8(), nullable=False),
    F('samples', pa.list_(pa.float64()), nullable=False),
])

ring = new_ring(schema, 16)
L = {p: lib.ar_leaf_index(ring, p.encode())
     for p in ('tv', 'servo.pos', 'servo.vel', 'mark', 'label', 'samples')}
check("leaf indices resolve", all(v >= 0 for v in L.values()))

import struct as _struct
for i in range(6):
    ok(lib.ar_row_begin(ring, 0, ffi.NULL))
    ok(lib.ar_set_f64(ring, L['tv'], float(i)))
    ok(lib.ar_set_i64(ring, L['servo.pos'], i * 100))
    ok(lib.ar_set_i64(ring, L['servo.vel'], i * 200))
    if i % 2 == 0:
        ok(lib.ar_set_i64(ring, L['mark'], i * 10))   # odd rows stay null
    enc = f"row-{i}".encode()
    ok(lib.ar_set_bytes(ring, L['label'], enc, len(enc)))
    vals = _struct.pack(f'<{i}d', *[float(j) for j in range(i)])
    ok(lib.ar_set_list(ring, L['samples'], vals, i))
n = ffi.new("int64_t *")
ok(lib.ar_commit(ring, n))
check("commit count", n[0] == 6)

out = ffi.new("double *"); nul = ffi.new("_Bool *")
ok(lib.ar_get_f64(ring, 3, L['tv'], out, nul))
check("get_f64", out[0] == 3.0 and not nul[0])
d = ffi.new("const void **"); dn = ffi.new("int64_t *")
ok(lib.ar_get_var(ring, 4, L['label'], d, dn, nul))
check("get_var utf8", ffi.buffer(d[0], dn[0])[:] == b'row-4')

rsp = ffi.new("ArRecordSet **")
ok(lib.ar_relation(ring, 0.0, rsp))
rs = rsp[0]
t = table_of(rs)
check("table rows", t.num_rows == 6)
check("fixed column", t.column('tv').to_pylist() == [float(i) for i in range(6)])
check("struct column", t.column('servo').to_pylist()[2] == {'pos': 200, 'vel': 400})
check("nullable column", t.column('mark').to_pylist() == [0, None, 20, None, 40, None])
check("utf8 column", t.column('label').to_pylist() == [f'row-{i}' for i in range(6)])
check("list column", t.column('samples').to_pylist() ==
      [[float(j) for j in range(i)] for i in range(6)])
addr = lib.ar_leaf_data_address(ring, L['tv'])
check("zero-copy fixed data",
      t.column('tv').chunk(0).buffers()[1].address == addr)

# pin blocks writer at wrap; release unblocks
for i in range(6, 16):
    ok(lib.ar_row_begin(ring, 0, ffi.NULL))
    ok(lib.ar_set_f64(ring, L['tv'], float(i)))
    ok(lib.ar_set_bytes(ring, L['label'], b'x', 1))
    ok(lib.ar_set_list(ring, L['samples'], b'', 0))
ok(lib.ar_commit(ring, ffi.NULL))
st = lib.ar_row_begin(ring, 20, ffi.NULL)     # seq 16 recycles seq 0: pinned
check("pin stalls writer", st == AR_PIN_STALLED)
lib.ar_recordset_release(rs)
check("release unblocks writer",
      lib.ar_row_begin(ring, 0, ffi.NULL) == AR_OK)
check("frozen table content survived pin window",
      t.column('label').to_pylist()[0] == 'row-0')

lib.ar_ring_free(ring)

bad = sum(1 for _, c in checks if not c)
print(f"\n{len(checks) - bad}/{len(checks)} passed")
sys.exit(1 if bad else 0)
