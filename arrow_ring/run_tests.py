"""Standalone test runner for arrow_ring (no pytest dependency).

Run:  python3 packages/arrow_ring/run_tests.py

Covers the allocation model (mutable -> frozen -> recycled), read-only
handles, pins blocking the writer, lease exclusion math, zero-copy
verification, wrapped-region record sets, the Arrow type surface
(fixed-width tier, structs, nulls), and the variable-width segment pool
(utf8/binary/list, chunk cuts, oversized values, segment reuse).
"""

import os
import subprocess
import sys
import threading
import time
import traceback

import pyarrow as pa

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from arrow_ring import Ring, StaleReadError, PinStalledError

F = pa.field
SCHEMA = pa.schema([F('tv', pa.float64(), nullable=False),
                    F('v0', pa.float32(), nullable=False),
                    F('n', pa.int32(), nullable=False)])


class Raises:
    """Context manager asserting the block raises the given exception."""
    def __init__(self, exc_type):
        self.exc_type = exc_type

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            raise AssertionError(f"expected {self.exc_type.__name__}, "
                                 f"no exception raised")
        return issubclass(exc_type, self.exc_type)


def fill(ring, count, start=0):
    """Write `count` rows (tv=i, v0=i*0.5, n=i) and commit."""
    for i in range(start, start + count):
        row = ring.get_mutable_row()
        row.tv = float(i)
        row.v0 = i * 0.5
        row.n = i
    ring.commit()


# ---- write / commit / read ------------------------------------------------

def test_write_commit_read_roundtrip():
    ring = Ring(SCHEMA, capacity=16)
    fill(ring, 5)
    assert ring.oldest_seq == 0 and ring.newest_seq == 4
    r = ring.read_row(3)
    assert r.tv == 3.0 and r.v0 == 1.5 and r.n == 3


def test_uncommitted_rows_not_readable():
    ring = Ring(SCHEMA, capacity=16)
    row = ring.get_mutable_row()
    row.tv = 1.0
    with Raises(StaleReadError):
        ring.read_row(0)
    ring.commit()
    assert ring.read_row(0).tv == 1.0


def test_read_row_is_read_only():
    ring = Ring(SCHEMA, capacity=16)
    fill(ring, 1)
    r = ring.read_row(0)
    with Raises(AttributeError):
        r.tv = 99.0
    assert ring.read_row(0).tv == 0.0


def test_commit_returns_count():
    ring = Ring(SCHEMA, capacity=16)
    for i in range(3):
        row = ring.get_mutable_row()
        row.n = i
    assert ring.commit() == 3
    assert ring.commit() == 0


def test_wrap_recycles_oldest():
    ring = Ring(SCHEMA, capacity=8)
    fill(ring, 8)
    fill(ring, 2, start=8)          # recycles seqs 0 and 1
    assert ring.oldest_seq == 2
    with Raises(StaleReadError):
        ring.read_row(1)
    assert ring.read_row(8).n == 8
    assert ring.read_row(2).n == 2  # survivor intact


def test_allocation_destroys_before_commit():
    ring = Ring(SCHEMA, capacity=4)
    fill(ring, 4)
    ring.get_mutable_row()          # allocates seq 4 -> seq 0 unreadable
    with Raises(StaleReadError):
        ring.read_row(0)


# ---- record sets: zero-copy, pins, leases ---------------------------------

def test_relation_content_and_range():
    ring = Ring(SCHEMA, capacity=64)
    fill(ring, 10)
    with ring.get_static_relation(duration_s=0.0) as rs:
        assert (rs.lo, rs.hi) == (0, 10) and len(rs) == 10
        t = rs.table
        assert t.num_rows == 10
        assert t.column('n').to_pylist() == list(range(10))


def test_relation_is_zero_copy():
    ring = Ring(SCHEMA, capacity=64)
    fill(ring, 10)
    with ring.get_static_relation(duration_s=0.0) as rs:
        col = rs.table.column('tv').chunk(0)
        # Arrow data buffer must alias the ring's own column memory
        # (leaf 0 = tv in schema order).
        assert col.buffers()[1].address == ring.leaf_data_address(0)


def test_relation_wrapped_region_is_two_chunks():
    ring = Ring(SCHEMA, capacity=8)
    ring.LEASE_MARGIN_S = 0.0       # two commits observe a real rate;
                                    # zero the margin for exact bounds
    fill(ring, 8)
    fill(ring, 4, start=8)          # physical window now wraps: seqs 4..11
    with ring.get_static_relation(duration_s=0.0) as rs:
        assert (rs.lo, rs.hi) == (4, 12)
        t = rs.table
        assert t.column('n').num_chunks == 2
        assert t.column('n').to_pylist() == list(range(4, 12))


def test_empty_relation():
    ring = Ring(SCHEMA, capacity=16)
    with ring.get_static_relation(duration_s=1.0) as rs:
        assert len(rs) == 0 and rs.table.num_rows == 0


def test_lease_excludes_only_rows_facing_recycle():
    # Exclusion is against the RECYCLE FRONTIER, not the oldest row: a
    # ring that is not near full excludes nothing (an honest hold
    # recycles nothing), and a full ring excludes rate x (hold+margin).
    ring = Ring(SCHEMA, capacity=1000, rate_hint=100.0)
    fill(ring, 500)
    with ring.get_static_relation(duration_s=2.0) as rs:
        assert rs.lo == 0 and rs.hi == 500          # half-full: no exclusion
    # Fresh ring, single commit (a second commit would raise the observed
    # peak above the hint), filled to capacity:
    ring = Ring(SCHEMA, capacity=1000, rate_hint=100.0)
    fill(ring, 1000)
    # frontier after a 2.5s hold at 100 rows/s: 1000 + 250 - 1000 = 250
    with ring.get_static_relation(duration_s=2.0) as rs:
        assert rs.lo == 250 and rs.hi == 1000


def test_pin_blocks_writer_then_raises():
    ring = Ring(SCHEMA, capacity=8)
    fill(ring, 8)
    rs = ring.get_static_relation(duration_s=0.0)   # pins [0, 8)
    with Raises(PinStalledError):
        ring.get_mutable_row(block_s=0.05)   # seq 8 must recycle seq 0
    rs.release()
    row = ring.get_mutable_row()             # now fine
    row.n = 8
    ring.commit()
    assert ring.read_row(8).n == 8


def test_pin_range_holds_only_its_rows():
    # pin(lo, hi) pins the rows named and no others, with no lease: the
    # writer recycles everything below lo freely and meets the pin only
    # when it reaches lo, a whole ring after the rows were written.
    ring = Ring(SCHEMA, capacity=8)
    fill(ring, 8)
    rs = ring.pin(6, 8)                      # the two newest rows
    assert (rs.lo, rs.hi) == (6, 8) and rs.table.num_rows == 2
    assert rs.table.column('n').to_pylist() == [6, 7]
    fill(ring, 6, start=8)                   # seq 8..13 recycle 0..5: fine
    with Raises(PinStalledError):
        ring.get_mutable_row(block_s=0.05)   # seq 14 would recycle 6
    rs.release()
    fill(ring, 1, start=14)
    # Below the oldest row, lo is raised (rows recycled): the set says so.
    rs = ring.pin(0, 15)
    assert rs.lo == ring.oldest_seq == 7 and rs.hi == 15
    rs.release()
    # Above committed, hi is lowered; an empty range pins nothing.
    rs = ring.pin(15, 100)
    assert (rs.lo, rs.hi) == (15, 15) and rs.table.num_rows == 0
    rs.release()
    fill(ring, 8, start=15)                  # a whole ring: no pin in the way
    # A detached range pin rides the table and still holds only its rows.
    rs = ring.pin(21, 23)
    t = rs.detach()
    fill(ring, 6, start=23)                  # recycles 15..20
    with Raises(PinStalledError):
        ring.get_mutable_row(block_s=0.05)   # seq 29 would recycle 21
    del t, rs                                # the last references: pin drops
    fill(ring, 1, start=29)
    assert ring.read_row(29).n == 29


def test_chunked_read_pins_only_the_chunk_in_hand():
    # iter_chunks: nothing pinned at open; the range is fixed at the
    # first chunk; each batch pins its own rows until it is dropped; a
    # read the writer laps ends with StaleReadError naming the rows.
    ring = Ring(SCHEMA, capacity=1000)
    fill(ring, 1000)                         # full: seq 0..999
    it = ring.iter_chunks(chunk_rows=100)
    fill(ring, 10, start=1000)               # before the first next: no pin yet
    first, b = next(it)
    assert first == 10 and b.num_rows == 100 # the range is fixed now: [10, 1010)
    assert b.column('n').to_pylist()[:2] == [10, 11]
    with Raises(PinStalledError):
        ring.get_mutable_row(block_s=0.05)   # seq 1010 would recycle 10, pinned
    del b                                    # the batch's pin drops
    fill(ring, 100, start=1010)              # recycles 10..109 freely
    rows, end = 100, first + 100
    for first, b in it:                      # the rest, holding nothing
        assert first == end                  # contiguous; the ring wrap cuts one chunk short
        rows += b.num_rows
        end = first + b.num_rows
        del b
    assert rows == 1000 and end == 1010      # [10, 1010): rows added after the first next are not in it
    # lapped: a chunk the writer recycled before the read reached it
    it = ring.iter_chunks(chunk_rows=100)
    first, b = next(it)
    del b
    fill(ring, 1000, start=1110)             # a whole ring: everything behind the read is gone
    with Raises(StaleReadError):
        next(it)
    # hi bounds the read; lo below the oldest row starts at the oldest
    got = [f for f, _ in ring.iter_chunks(lo=0, hi=ring.oldest_seq + 250, chunk_rows=100)]
    assert got == [ring.oldest_seq, ring.oldest_seq + 100, ring.oldest_seq + 200]
    # var-width columns: the segment boundaries cut chunks too, and the
    # data is zero copy and right
    ring = Ring(MIXED, capacity=400, segment_bytes=4096)
    fill_mixed(ring, 400)
    seen = []
    for first, b in ring.iter_chunks(chunk_rows=150):
        seen.extend(zip(range(first, first + b.num_rows), b.column('label').to_pylist()))
    assert len(seen) == 400 and all(l == f'r{s}' for s, l in seen)


def test_writer_unblocks_when_pin_released_concurrently():
    ring = Ring(SCHEMA, capacity=8)
    fill(ring, 8)
    rs = ring.get_static_relation(duration_s=0.0)
    t = threading.Timer(0.05, rs.release)
    t.start()
    row = ring.get_mutable_row(block_s=2.0)  # blocks ~50ms, then proceeds
    row.n = 8
    ring.commit()
    t.join()
    assert ring.read_row(8).n == 8


def test_frozen_data_stable_under_continued_writes():
    ring = Ring(SCHEMA, capacity=64)
    fill(ring, 10)
    with ring.get_static_relation(duration_s=0.0) as rs:
        before = rs.table.column('n').to_pylist()
        fill(ring, 20, start=10)             # writer advances, no recycle yet
        assert rs.table.column('n').to_pylist() == before


def test_rate_estimate_observes_commits():
    ring = Ring(SCHEMA, capacity=100_000)
    fill(ring, 1)                    # seed the commit clock
    time.sleep(0.01)
    fill(ring, 1000)
    assert ring._lease_rate() > 0


# ---- type surface: fixed-width tier ----------------------------------------

def test_fixed_width_kinds_roundtrip():
    schema = pa.schema([F('i8', pa.int8(), nullable=False),
                        F('u64', pa.uint64(), nullable=False),
                        F('f16', pa.float16(), nullable=False),
                        F('flag', pa.bool_(), nullable=False),
                        F('ts', pa.timestamp('us'), nullable=False),
                        F('dur', pa.duration('ms'), nullable=False),
                        F('day', pa.date32(), nullable=False)])
    ring = Ring(schema, capacity=16)
    row = ring.get_mutable_row()
    row.i8 = -5
    row.u64 = 2**63
    row.f16 = 1.5
    row.flag = True
    import datetime as _dt
    row.ts = _dt.datetime(2026, 8, 19, 12, 0, 0, tzinfo=_dt.timezone.utc)
    row.dur = 1500
    row.day = _dt.date(2026, 8, 19)
    ring.commit()
    with ring.get_static_relation(duration_s=0.0) as rs:
        d = rs.table.to_pylist()[0]
        assert d['i8'] == -5 and d['u64'] == 2**63
        assert d['f16'] == 1.5 and d['flag'] is True
        assert d['ts'].isoformat().startswith('2026-08-19T12:00:00')
        assert d['dur'].total_seconds() == 1.5
        assert d['day'].isoformat() == '2026-08-19'
    assert ring.read_row(0).flag is True


def test_bool_bitpacked_across_bytes():
    schema = pa.schema([F('b', pa.bool_(), nullable=False)])
    ring = Ring(schema, capacity=64)
    for i in range(20):
        row = ring.get_mutable_row()
        row.b = (i % 3 == 0)
    ring.commit()
    with ring.get_static_relation(duration_s=0.0) as rs:
        assert rs.table.column('b').to_pylist() == \
            [(i % 3 == 0) for i in range(20)]


def test_decimal_and_fixed_size_binary():
    import decimal as _dec
    schema = pa.schema([F('price', pa.decimal128(18, 4), nullable=False),
                        F('tag', pa.binary(4), nullable=False)])
    ring = Ring(schema, capacity=8)
    row = ring.get_mutable_row()
    row.price = _dec.Decimal('123.4567')
    row.tag = b'ABCD'
    ring.commit()
    with ring.get_static_relation(duration_s=0.0) as rs:
        d = rs.table.to_pylist()[0]
        assert d['price'] == _dec.Decimal('123.4567')
        assert d['tag'] == b'ABCD'
    with Raises(ValueError):
        r2 = ring.get_mutable_row()
        r2.tag = b'TOOLONG'


def test_fixed_size_list():
    schema = pa.schema([F('xyz', pa.list_(pa.float64(), 3), nullable=False)])
    ring = Ring(schema, capacity=8)
    for i in range(4):
        row = ring.get_mutable_row()
        row.xyz = [i, i + 0.5, i + 0.25]
    ring.commit()
    assert ring.read_row(2).xyz == (2.0, 2.5, 2.25)
    with ring.get_static_relation(duration_s=0.0) as rs:
        assert rs.table.column('xyz').to_pylist()[3] == [3.0, 3.5, 3.25]
    with Raises(ValueError):
        ring.get_mutable_row().xyz = [1.0, 2.0]


def test_nested_struct():
    servo = pa.struct([F('pos', pa.int32(), nullable=False),
                       F('vel', pa.int32(), nullable=False)])
    schema = pa.schema([F('ts', pa.float64(), nullable=False),
                        F('servo', servo, nullable=False)])
    ring = Ring(schema, capacity=8)
    row = ring.get_mutable_row()
    row.ts = 1.0
    row.servo.pos = 100
    row.servo.vel = 200
    ring.commit()
    r = ring.read_row(0)
    assert r.servo.pos == 100 and r.servo.vel == 200
    with ring.get_static_relation(duration_s=0.0) as rs:
        d = rs.table.to_pylist()[0]
        assert d['servo'] == {'pos': 100, 'vel': 200}


def test_nulls_fixed_width():
    schema = pa.schema([F('x', pa.float64(), nullable=False),
                        F('mark', pa.int32(), nullable=True)])
    ring = Ring(schema, capacity=8)
    ring.LEASE_MARGIN_S = 0.0        # two commits observe a rate; keep
                                     # the relation bounds exact
    for i in range(4):
        row = ring.get_mutable_row()
        row.x = float(i)
        if i % 2 == 0:
            row.mark = i * 10
        # odd rows: mark never set -> null (nullable leaves reset on alloc)
    ring.commit()
    assert ring.read_row(1).mark is None
    assert ring.read_row(2).mark == 20
    row = ring.get_mutable_row()
    row.x = 4.0
    row.mark = 7
    row.mark = None                  # explicit null overwrites
    ring.commit()
    assert ring.read_row(4).mark is None
    with ring.get_static_relation(duration_s=0.0) as rs:
        assert rs.table.column('mark').to_pylist() == [0, None, 20, None, None]
    with Raises(TypeError):
        ring.get_mutable_row().x = None   # not nullable


# ---- variable-width: utf8 / binary / list ----------------------------------

def test_utf8_roundtrip():
    schema = pa.schema([F('n', pa.int32(), nullable=False),
                        F('label', pa.utf8(), nullable=False)])
    ring = Ring(schema, capacity=16)
    words = ['', 'coarse', 'settled', 'µm-grid', 'x' * 500]
    for i, w in enumerate(words):
        row = ring.get_mutable_row()
        row.n = i
        row.label = w
    ring.commit()
    assert ring.read_row(3).label == 'µm-grid'
    assert ring.read_row(0).label == ''
    with ring.get_static_relation(duration_s=0.0) as rs:
        assert rs.table.column('label').to_pylist() == words


def test_utf8_null_vs_empty():
    schema = pa.schema([F('label', pa.utf8(), nullable=True)])
    ring = Ring(schema, capacity=8)
    row = ring.get_mutable_row()
    row.label = ''
    row = ring.get_mutable_row()
    row.label = None
    row = ring.get_mutable_row()     # never written -> null
    ring.commit()
    assert ring.read_row(0).label == ''
    assert ring.read_row(1).label is None
    assert ring.read_row(2).label is None
    with ring.get_static_relation(duration_s=0.0) as rs:
        assert rs.table.column('label').to_pylist() == ['', None, None]


def test_utf8_string_data_is_zero_copy():
    schema = pa.schema([F('label', pa.utf8(), nullable=False)])
    ring = Ring(schema, capacity=8)
    row = ring.get_mutable_row()
    row.label = 'hello'
    ring.commit()
    with ring.get_static_relation(duration_s=0.0) as rs:
        col = rs.table.column('label').chunk(0)
        assert col.buffers()[2].address == ring.var_data_address(0, 0)


def test_segment_boundary_causes_chunk_cut():
    schema = pa.schema([F('label', pa.utf8(), nullable=False)])
    ring = Ring(schema, capacity=64, segment_bytes=32)  # tiny segments
                                     # to force boundaries
    words = [f'word{i:02d}' for i in range(10)]   # 6 bytes each
    for w in words:
        row = ring.get_mutable_row()
        row.label = w
    ring.commit()
    with ring.get_static_relation(duration_s=0.0) as rs:
        col = rs.table.column('label')
        assert col.num_chunks > 1                  # cuts happened
        assert col.to_pylist() == words            # content survives them


def test_oversized_string_gets_dedicated_segment():
    schema = pa.schema([F('label', pa.utf8(), nullable=False)])
    ring = Ring(schema, capacity=8, segment_bytes=16)
    big = 'B' * 100                                # > segment size
    for w in ['small', big, 'after']:
        row = ring.get_mutable_row()
        row.label = w
    ring.commit()
    assert ring.pool_oversized_count(0) == 1
    assert ring.read_row(1).label == big
    with ring.get_static_relation(duration_s=0.0) as rs:
        assert rs.table.column('label').to_pylist() == ['small', big, 'after']


def test_ring_freed_by_gc_inside_a_type_parse_does_not_deadlock():
    # cffi parses a C type the first time under a lock that is not
    # reentrant, and the garbage collector can run inside that parse; a
    # ring freed there used to look up ar_ring_free for the first time
    # under the same lock and wait on its own thread forever (the
    # library's suite hung in wire_map, 2026-10-27). A fresh process
    # (nothing freed yet), a ring only the collector can free, a
    # collection on every allocation, and a C type never parsed before.
    code = """
import gc, os, sys
sys.path.insert(0, %r)
import pyarrow as pa
from arrow_ring import Ring
from arrow_ring._binding import ffi
ring = Ring(pa.schema([pa.field('n', pa.int64(), nullable=False)]), capacity=8)
ring._cycle = ring                       # only the collector frees it
del ring
gc.set_threshold(1, 1, 1)
ffi.new("struct { int64_t a; double b; int32_t c[3]; } *")
print("ok")
""" % os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    r = subprocess.run([sys.executable, '-c', code], capture_output=True,
                       text=True, timeout=60)
    assert r.returncode == 0 and r.stdout.strip() == 'ok', (r.stdout, r.stderr)


def test_batch_crossing_ring_end_keeps_live_strings():
    # A batch that wraps past the ring's end is ingested in two spans.
    # The second span's reclaim once took its floor from a slot the
    # first span had just overwritten, freeing segments that live rows
    # still used: wrong strings or "segment recycled under pin" from
    # the second wrap on (found 2026-10-27 at row ~490k of a 245,760-
    # row ring fed 5,000-row batches).
    schema = pa.schema([F('n', pa.int64(), nullable=False),
                        F('label', pa.utf8(), nullable=False)])
    ring = Ring(schema, capacity=10, segment_bytes=24)  # 4 values a segment
    n = 0
    for _ in range(12):                     # 48 rows: crosses the end often
        rows = list(range(n, n + 4))
        ring.append_batch(pa.record_batch(
            [pa.array(rows, pa.int64()), pa.array([f'w{i:04d}' for i in rows])],
            schema=schema))
        n += 4
        live = list(range(max(0, n - 10), n))
        assert [ring.read_row(i).label for i in live] == \
            [f'w{i:04d}' for i in live], n
        with ring.get_static_relation(duration_s=0.0) as rs:
            t = rs.table
            assert t.column('label').to_pylist() == \
                [f'w{i:04d}' for i in t.column('n').to_pylist()], n


def test_binary_column():
    schema = pa.schema([F('payload', pa.binary(), nullable=False)])
    ring = Ring(schema, capacity=8)
    row = ring.get_mutable_row()
    row.payload = b'\x00\x01\xffdata'
    ring.commit()
    assert ring.read_row(0).payload == b'\x00\x01\xffdata'
    with ring.get_static_relation(duration_s=0.0) as rs:
        assert rs.table.column('payload').to_pylist() == [b'\x00\x01\xffdata']


def test_list_of_primitive():
    schema = pa.schema([F('samples', pa.list_(pa.float64()), nullable=False)])
    ring = Ring(schema, capacity=16)
    lists = [[1.0, 2.0, 3.0], [], [4.5], [float(i) for i in range(20)]]
    for v in lists:
        row = ring.get_mutable_row()
        row.samples = v
    ring.commit()
    assert ring.read_row(0).samples == (1.0, 2.0, 3.0)
    assert len(ring.read_row(1).samples) == 0
    with ring.get_static_relation(duration_s=0.0) as rs:
        assert rs.table.column('samples').to_pylist() == lists


def test_var_with_ring_wrap():
    schema = pa.schema([F('n', pa.int32(), nullable=False),
                        F('label', pa.utf8(), nullable=False)])
    ring = Ring(schema, capacity=8)
    ring.LEASE_MARGIN_S = 0.0
    for i in range(12):                            # wraps: readable 4..11
        row = ring.get_mutable_row()
        row.n = i
        row.label = f'row-{i}'
        ring.commit()
    with ring.get_static_relation(duration_s=0.0) as rs:
        t = rs.table
        assert t.column('n').to_pylist() == list(range(4, 12))
        assert t.column('label').to_pylist() == [f'row-{i}'
                                                 for i in range(4, 12)]


def test_segment_reuse_after_recycle():
    schema = pa.schema([F('label', pa.utf8(), nullable=False)])
    ring = Ring(schema, capacity=4, segment_bytes=32)
    for i in range(40):                # many wraps; old segments retire
        row = ring.get_mutable_row()
        row.label = f'value-{i:04d}'   # 10 bytes
        ring.commit()
    live = ring.pool_segment_count(0)
    assert live <= 4, f"{live} live segments; reclaim not keeping up"
    assert ring.read_row(39).label == 'value-0039'


MIXED = pa.schema([F('tv', pa.float64(), nullable=False),
                   F('servo', pa.struct([F('pos', pa.int32(), nullable=False),
                                         F('vel', pa.int32(), nullable=False)]),
                     nullable=False),
                   F('mark', pa.int32(), nullable=True),
                   F('flag', pa.bool_(), nullable=False),
                   F('label', pa.utf8(), nullable=False),
                   F('samples', pa.list_(pa.float64()), nullable=False)])


def fill_mixed(ring, count, start=0):
    for i in range(start, start + count):
        row = ring.get_mutable_row()
        row.tv = float(i)
        row.servo.pos = i * 10
        row.servo.vel = i * 20
        row.mark = i if i % 2 == 0 else None
        row.flag = (i % 3 == 0)
        row.label = f'r{i}'
        row.samples = [float(j) for j in range(i % 4)]
    ring.commit()


def test_ring_to_ring_roundtrip():
    # The streaming loop minus the socket: A exports record batches,
    # B ingests them via the memcpy path; content must be identical.
    a = Ring(MIXED, capacity=64)
    b = Ring(MIXED, capacity=64)
    fill_mixed(a, 20)
    with a.get_static_relation(duration_s=0.0) as rs:
        b.append_table(rs.table)
    with b.get_static_relation(duration_s=0.0) as rs_b:
        with a.get_static_relation(duration_s=0.0) as rs_a:
            assert rs_b.table.to_pylist() == rs_a.table.to_pylist()
    assert b.read_row(7).label == 'r7'
    assert b.read_row(7).mark is None


def test_ring_to_ring_across_wrap():
    a = Ring(MIXED, capacity=16)
    a.LEASE_MARGIN_S = 0.0
    b = Ring(MIXED, capacity=64)
    b.LEASE_MARGIN_S = 0.0          # appends observe a rate; exact bounds
    fill_mixed(a, 24)                       # A wraps: readable 8..23
    with a.get_static_relation(duration_s=0.0) as rs:
        expect = rs.table.to_pylist()
        b.append_table(rs.table)
    with b.get_static_relation(duration_s=0.0) as rs_b:
        assert rs_b.table.to_pylist() == expect
    # B itself wraps while ingesting more
    with a.get_static_relation(duration_s=0.0) as rs:
        for _ in range(4):
            b.append_table(rs.table)        # 16 + 64 rows through cap 64
    assert b.newest_seq == 79


def test_append_pyarrow_built_batch():
    # Batches built by pyarrow directly (not from a ring), including a
    # slice with a nonzero offset -- exercises the offset arithmetic.
    ring = Ring(MIXED, capacity=32)
    ring.LEASE_MARGIN_S = 0.0       # two appends observe a rate; exact bounds
    batch = pa.record_batch({
        'tv': [1.0, 2.0, 3.0, 4.0],
        'servo': [{'pos': 1, 'vel': 2}, {'pos': 3, 'vel': 4},
                  {'pos': 5, 'vel': 6}, {'pos': 7, 'vel': 8}],
        'mark': [None, 5, None, 7],
        'flag': [True, False, True, False],
        'label': ['a', 'bb', '', 'dddd'],
        'samples': [[1.0], [], [2.0, 3.0], [4.0, 5.0, 6.0]],
    }, schema=MIXED)
    ring.append_batch(batch)
    ring.append_batch(batch.slice(1, 2))    # nonzero-offset ingest
    with ring.get_static_relation(duration_s=0.0) as rs:
        got = rs.table.to_pylist()
    assert got == batch.to_pylist() + batch.slice(1, 2).to_pylist()


def test_append_rejects_nulls_into_nonnullable():
    schema = pa.schema([F('x', pa.float64(), nullable=False)])
    ring = Ring(schema, capacity=8)
    batch = pa.record_batch(
        {'x': [1.0, None]},
        schema=pa.schema([F('x', pa.float64(), nullable=True)]))
    with Raises(Exception):
        ring.append_batch(batch)


def test_append_batch_larger_than_capacity_rejected():
    schema = pa.schema([F('x', pa.float64(), nullable=False)])
    ring = Ring(schema, capacity=4)
    batch = pa.record_batch({'x': [float(i) for i in range(5)]},
                            schema=schema)
    with Raises(Exception):
        ring.append_batch(batch)


def test_wire_map_scatter_gather():
    # A packed PLC-style frame: u16 counter, f64 position, i32 pair
    # inside a struct, 4-byte tag -- scattered in one C call.
    schema = pa.schema([F('cnt', pa.uint16(), nullable=False),
                        F('pos', pa.float64(), nullable=False),
                        F('servo', pa.struct([F('p', pa.int32(), nullable=False),
                                              F('v', pa.int32(), nullable=False)]),
                          nullable=False),
                        F('tag', pa.binary(4), nullable=False)])
    ring = Ring(schema, capacity=16)
    wmap = ring.wire_map([('cnt', 0, 2), ('pos', 2, 8),
                          ('servo.p', 10, 4), ('servo.v', 14, 4),
                          ('tag', 18, 4)])
    import struct as _s
    for i in range(5):
        frame = _s.pack('<Hd i i 4s', i, i * 1.5, i * 10, -i, b'ABCD')
        ring.get_mutable_row()
        ring.from_wire(wmap, frame)
    ring.commit()
    r = ring.read_row(3)
    assert r.cnt == 3 and r.pos == 4.5
    assert r.servo.p == 30 and r.servo.v == -3
    assert r.tag == b'ABCD'
    # gather back: identical bytes
    back = ring.to_wire(wmap, 3, len(frame))
    assert back == _s.pack('<Hd i i 4s', 3, 4.5, 30, -3, b'ABCD')
    with ring.get_static_relation(duration_s=0.0) as rs:
        assert rs.table.column('pos').to_pylist() == \
            [i * 1.5 for i in range(5)]


def test_wire_frame_is_any_buffer():
    # from_wire reads bytes or any buffer in place: a bytearray, a
    # memoryview, a ctypes struct over a send packet.
    import ctypes

    class Frame(ctypes.LittleEndianStructure):
        _pack_ = 1
        _layout_ = 'ms'
        _fields_ = [('cnt', ctypes.c_uint16), ('pos', ctypes.c_double)]

    schema = pa.schema([F('cnt', pa.uint16(), nullable=False),
                        F('pos', pa.float64(), nullable=False)])
    ring = Ring(schema, capacity=8)
    wmap = ring.wire_map([('cnt', 0, 2), ('pos', 2, 8)])
    import struct as _s
    raw = _s.pack('<Hd', 1, 1.5)
    for frame in (raw, bytearray(_s.pack('<Hd', 2, 2.5)),
                  memoryview(_s.pack('<Hd', 3, 3.5)), Frame(4, 4.5)):
        ring.get_mutable_row()
        ring.from_wire(wmap, frame)
    ring.commit()
    assert [(ring.read_row(s).cnt, ring.read_row(s).pos)
            for s in range(4)] == [(1, 1.5), (2, 2.5), (3, 3.5), (4, 4.5)]
    ring.get_mutable_row()
    with Raises(ValueError):
        ring.from_wire(wmap, bytearray(9))   # buffer too small


def test_wire_map_rejects_bad_maps():
    schema = pa.schema([F('x', pa.float64(), nullable=False),
                        F('s', pa.utf8(), nullable=False)])
    ring = Ring(schema, capacity=8)
    with Raises(ValueError):
        ring.wire_map([('x', 0, 4)])         # wrong width
    with Raises(Exception):
        ring.wire_map([('s', 0, 8)])         # var leaf not mappable
    with Raises(KeyError):
        ring.wire_map([('nope', 0, 8)])
    wmap = ring.wire_map([('x', 0, 8)])
    ring.get_mutable_row()
    with Raises(ValueError):
        ring.from_wire(wmap, b'short')       # frame too small


def test_rejected_types():
    with Raises(TypeError):
        Ring(pa.schema([F('m', pa.map_(pa.utf8(), pa.int32()))]), capacity=8)
    with Raises(TypeError):
        Ring(pa.schema([F('l', pa.list_(pa.utf8()))]), capacity=8)  # nested var
    with Raises(TypeError):
        servo = pa.struct([F('p', pa.int32())])
        Ring(pa.schema([F('s', servo, nullable=True)]), capacity=8)
    with Raises(TypeError):
        Ring("not a schema", capacity=8)



def test_hostile_data_ring_roundtrip():
    """Nulls, NaN/Inf, unicode, empty+huge values through append_batch
    -> static relation, exact; then across a wrap (hostile testkit)."""
    from hostile import hostile_batch, assert_hostile_equal, HOSTILE_SCHEMA
    ring = Ring(HOSTILE_SCHEMA, capacity=4096, lease_margin_s=0.0)
    for k in range(4):
        ring.append_batch(hostile_batch(k * 700, 700))
    rel = ring.get_static_relation(duration_s=1.0)
    try:
        assert_hostile_equal(rel.table, 0, 2800)
    finally:
        rel.release()
    for k in range(4, 8):
        ring.append_batch(hostile_batch(k * 700, 700))
    rel = ring.get_static_relation(duration_s=1.0)
    try:
        t = rel.table
        first = t.column('n').to_pylist()[0]
        assert_hostile_equal(t, first, t.num_rows)
    finally:
        rel.release()

def main():
    tests = [(name, fn) for name, fn in sorted(globals().items())
             if name.startswith('test_') and callable(fn)]
    failed = []
    for name, fn in tests:
        try:
            fn()
            print(f"  PASS {name}")
        except Exception:
            failed.append(name)
            print(f"  FAIL {name}")
            traceback.print_exc()
    print(f"\n{len(tests) - len(failed)}/{len(tests)} passed")
    return 1 if failed else 0




if __name__ == '__main__':
    sys.exit(main())
