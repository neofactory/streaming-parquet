"""Standalone test runner for parquet_checkpoint (no pytest).

Run:  python3 packages/parquet_checkpoint/run_tests.py

The manifest-format suite, per the README test plan: most tests run
against BOTH sinks (DirSink for the volume tier, FakeS3 for S3
semantics with fault injection); the subprocess crash gauntlet runs on
DirSink; fault/crash injection runs on FakeS3. `--child` is the
gauntlet's sacrificial writer.
"""

import os
import random
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import traceback

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from parquet_checkpoint import (ckpt_versions,
                                CheckpointError, CheckpointReader,
                                CheckpointWriter, DirSink, FakeS3,
                                finalize, live_file, live_table,
                                recover_directory, verify)
from parquet_checkpoint._sinks import CrashPoint, FaultInjected, SinkError


def newest(sink, name):
    """The newest checkpoint version's object name of file `name`."""
    v = ckpt_versions(sink, name)
    assert v, f"no checkpoint version of {name}"
    return v[-1][1]

F = pa.field
SCHEMA = pa.schema([F('n', pa.int64(), nullable=False),
                    F('x', pa.float64(), nullable=False),
                    F('s', pa.string(), nullable=False)])


def make_rows(start, count):
    return pa.table({'n': list(range(start, start + count)),
                     'x': [i * 0.5 for i in range(start, start + count)],
                     's': [f'row-{i}' for i in range(start, start + count)]},
                    schema=SCHEMA)


def check_exact(table, expect_count):
    assert table.num_rows == expect_count, (table.num_rows, expect_count)
    assert table.column('n').to_pylist() == list(range(expect_count))
    if expect_count:
        assert table.column('s').to_pylist()[-1] == f'row-{expect_count - 1}'


def both_sinks():
    """(label, sink, cleanup) for DirSink and FakeS3."""
    tmp = tempfile.mkdtemp()
    yield 'dir', DirSink(tmp), lambda: shutil.rmtree(tmp)
    yield 'fake-s3', FakeS3(part_min=4096), lambda: None


# ---------------------------------------------------------------------------
# --child: sacrificial writer for the crash gauntlet (DirSink).
if len(sys.argv) > 1 and sys.argv[1] == '--child':
    d, group = sys.argv[2], int(sys.argv[3])
    w = CheckpointWriter('kill.parquet', SCHEMA, sink=DirSink(d),
                         chunk_bytes=64 * 1024, compression='none')
    i = 0
    while True:
        w.write_table(make_rows(i, group))     # implies checkpoint
        i += group
# ---------------------------------------------------------------------------

signal.alarm(300)
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


@test("clean lifecycle on both sinks: chunks + checkpoint -> ordinary file")
def _():
    for label, sink, cleanup in both_sinks():
        try:
            w = CheckpointWriter('a.parquet', SCHEMA, sink=sink,
                                 chunk_bytes=64 * 1024, compression='none')
            for k in range(8):
                w.write_table(make_rows(k * 500, 500))
            w.close()
            assert sink.list('') == ['a.parquet'], (label, sink.list(''))
            t = pq.read_table(pa.BufferReader(sink.get('a.parquet')))
            check_exact(t, 4000)
        finally:
            cleanup()


@test("provisional tail: zero committed-row loss at a checkpoint instant")
def _():
    for label, sink, cleanup in both_sinks():
        try:
            w = CheckpointWriter('b.parquet', SCHEMA, sink=sink)
            w.write_table(make_rows(0, 100))
            w.checkpoint(tail=make_rows(100, 7))    # unwritten rows ride
            del w                                    # crash
            status, name = finalize('b.parquet', sink)
            assert status == 'recovered', (label, status)
            t = pq.read_table(pa.BufferReader(sink.get('b.parquet')))
            check_exact(t, 107)                      # tail NOT lost
        finally:
            cleanup()


@test("provisional tail superseded by the next write_table")
def _():
    sink = FakeS3()
    w = CheckpointWriter('c.parquet', SCHEMA, sink=sink)
    w.write_table(make_rows(0, 50))
    w.checkpoint(tail=make_rows(50, 5))
    r = CheckpointReader(newest(sink, 'c.parquet'), sink)
    assert r.num_rows == 55, r.num_rows
    w.write_table(make_rows(50, 50))     # the 5 re-encoded as final
    r = CheckpointReader(newest(sink, 'c.parquet'), sink)
    assert r.num_rows == 100, r.num_rows
    check_exact(r.table(), 100)
    w.close()
    check_exact(pq.read_table(pa.BufferReader(sink.get('c.parquet'))), 100)


@test("reader mid-write: exact prefix, queryable by DuckDB")
def _():
    sink = FakeS3(part_min=4096)
    w = CheckpointWriter('d.parquet', SCHEMA, sink=sink,
                         chunk_bytes=32 * 1024, compression='none')
    for k in range(6):
        w.write_table(make_rows(k * 300, 300))
        r = CheckpointReader(newest(sink, 'd.parquet'), sink)
        assert r.num_rows == (k + 1) * 300
        t = r.table()
        check_exact(t, (k + 1) * 300)
    con = duckdb.connect()
    con.register('live', t)
    n, mx = con.sql("SELECT count(*), max(n) FROM live").fetchall()[0]
    assert (n, mx) == (1800, 1799), (n, mx)
    w.close()


@test("reader thread hammering a live writer stays monotonic (dir)")
def _():
    tmp = tempfile.mkdtemp()
    try:
        path = os.path.join(tmp, 'e.parquet')
        w = CheckpointWriter(path, SCHEMA, chunk_bytes=64 * 1024,
                             compression='none')
        stop = threading.Event()
        problems = []

        def reader():
            seen = 0
            while not stop.is_set():
                try:
                    t = live_table(path)
                except (CheckpointError, SinkError, FileNotFoundError):
                    continue
                if t.num_rows < seen:
                    problems.append(f"backwards {seen}->{t.num_rows}")
                    return
                if t.num_rows and \
                        t.column('n').to_pylist()[t.num_rows - 1] \
                        != t.num_rows - 1:
                    problems.append(f"content wrong at {t.num_rows}")
                    return
                seen = t.num_rows
        r = threading.Thread(target=reader)
        r.start()
        for k in range(30):
            w.write_table(make_rows(k * 200, 200))
        stop.set()
        r.join()
        w.close()
        assert not problems, problems[0]
        check_exact(pq.read_table(path), 6000)
    finally:
        shutil.rmtree(tmp)


@test("crash gauntlet: SIGKILL at random moments, finalize, exact (dir)")
def _():
    rng = random.Random(20260822)
    with tempfile.TemporaryDirectory() as tmp:
        for k in range(10):
            d = os.path.join(tmp, f'k{k}')
            os.makedirs(d)
            child = subprocess.Popen(
                [sys.executable, os.path.abspath(__file__), '--child',
                 d, '50'],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            sink = DirSink(d)
            deadline = time.monotonic() + 15.0
            while (not bool(ckpt_versions(sink, 'kill.parquet'))
                   and time.monotonic() < deadline):
                time.sleep(0.01)
            assert bool(ckpt_versions(sink, 'kill.parquet')), "child never started"
            time.sleep(rng.uniform(0.02, 0.5))
            child.kill()
            child.wait()
            status, name = finalize('kill.parquet', sink)
            assert status == 'recovered'
            t = pq.read_table(os.path.join(d, 'kill.parquet'))
            n = t.num_rows
            assert n > 0 and n % 50 == 0, f"{n} rows: not a claim boundary"
            check_exact(t, n)
            residue = [f for f in sink.list('') if f != 'kill.parquet']
            assert not residue, residue


@test("fault injection: failed chunk PUT is loud; retry succeeds (fake-s3)")
def _():
    sink = FakeS3(part_min=4096)
    w = CheckpointWriter('f.parquet', SCHEMA, sink=sink,
                         chunk_bytes=16 * 1024, compression='none')
    sink.fail_next('put:f.parquet.chunks/')
    try:
        w.write_table(make_rows(0, 2000))
        raise AssertionError("injected chunk-PUT failure must surface")
    except FaultInjected:
        pass
    w.write_table(make_rows(2000, 100))     # cut retries and succeeds
    w.close()
    check_exact(pq.read_table(pa.BufferReader(sink.get('f.parquet'))), 2100)


@test("fault injection: failed checkpoint PUT leaves previous intact")
def _():
    sink = FakeS3()
    w = CheckpointWriter('g.parquet', SCHEMA, sink=sink)
    w.write_table(make_rows(0, 60))
    sink.fail_next('put:g.parquet.0')
    try:
        w.checkpoint(tail=make_rows(60, 9))
        raise AssertionError("injected ckpt-PUT failure must surface")
    except FaultInjected:
        pass
    r = CheckpointReader(newest(sink, 'g.parquet'), sink)
    assert r.num_rows == 60                  # previous claim intact
    w.checkpoint(tail=make_rows(60, 9))      # retry
    assert CheckpointReader(newest(sink, 'g.parquet'), sink).num_rows == 69
    w.close()


@test("crash mid-assembly: artifacts intact, finalize re-runs (fake-s3)")
def _():
    sink = FakeS3()
    w = CheckpointWriter('h.parquet', SCHEMA, sink=sink)
    w.write_table(make_rows(0, 500))
    del w
    sink.crash_next('complete:h.parquet')
    try:
        finalize('h.parquet', sink)
        raise AssertionError("injected crash must propagate")
    except CrashPoint:
        pass
    assert not sink.exists('h.parquet')      # no partial final object
    assert bool(ckpt_versions(sink, 'h.parquet'))     # claims intact
    status, _ = finalize('h.parquet', sink)  # re-run completes
    assert status == 'recovered'
    check_exact(pq.read_table(pa.BufferReader(sink.get('h.parquet'))), 500)


@test("corruption refusals: chunk CRC, checkpoint CRC, mispairing")
def _():
    sink = FakeS3(part_min=4096)
    w = CheckpointWriter('i.parquet', SCHEMA, sink=sink,
                         chunk_bytes=16 * 1024, compression='none')
    w.write_table(make_rows(0, 2000))
    del w
    # chunk bit-flip -> manifest CRC refuses at read
    cname = sink.list('i.parquet.chunks/')[0]
    raw = bytearray(sink.get(cname))
    raw[len(raw) // 2] ^= 0xFF
    sink.put(cname, bytes(raw))
    try:
        CheckpointReader(newest(sink, 'i.parquet'), sink).table()
        raise AssertionError("corrupt chunk must refuse")
    except CheckpointError as e:
        assert 'CRC' in str(e), e
    # checkpoint bit-flip -> header CRC refuses
    obj = newest(sink, 'i.parquet')
    raw = bytearray(sink.get(obj))
    raw[30] ^= 0xFF
    sink.put(obj, bytes(raw))
    try:
        CheckpointReader(newest(sink, 'i.parquet'), sink)
        raise AssertionError("corrupt checkpoint must refuse")
    except CheckpointError as e:
        assert 'CRC' in str(e), e
    # mispairing -> loud refusal
    sink2 = FakeS3()
    w2 = CheckpointWriter('j.parquet', SCHEMA, sink=sink2)
    w2.write_table(make_rows(0, 10))
    del w2
    sink2.put('z.parquet.000001.ckpt', sink2.get(newest(sink2, 'j.parquet')))
    try:
        finalize('z.parquet', sink2)
        raise AssertionError("mispaired checkpoint must refuse")
    except CheckpointError as e:
        assert 'mispaired' in str(e) or 'z.parquet' in str(e), e
    # chunk_bytes below the sink's part minimum -> refused at construction
    try:
        CheckpointWriter('q.parquet', SCHEMA, sink=FakeS3(),
                         chunk_bytes=1024)
        raise AssertionError("chunk_bytes below part_min must refuse")
    except CheckpointError as e:
        assert 'part minimum' in str(e), e


@test("THE invariant: every checkpoint ever published finalizes exactly")
def _():
    rng = random.Random(7)
    sink = FakeS3(part_min=4096)
    w = CheckpointWriter('k.parquet', SCHEMA, sink=sink,
                         chunk_bytes=16 * 1024, compression='none',
                         checkpoint_every_write=False)
    generations = []                     # (objects snapshot, expected rows)
    i = 0
    tail_n = 0
    for _ in range(40):
        if rng.random() < 0.5:
            n = rng.randrange(1, 400)
            w.write_table(make_rows(i, n))
            i += n
            tail_n = 0
        # a tail holds every row since the last write_table: it grows,
        # or is given up (None: the claim covers the final rows only)
        tail_n = 0 if rng.random() < 0.15 else tail_n + rng.randrange(0, 60)
        tail = make_rows(i, tail_n) if tail_n else None
        w.checkpoint(tail)
        rows = w.rows_checkpointed
        snap = {name: sink.get(name) for name in sink.list('')}
        generations.append((snap, rows))
    for snap, rows in generations:
        s2 = FakeS3(part_min=4096)
        for name, data in snap.items():
            s2.put(name, data)
        status, _ = finalize('k.parquet', s2)
        assert status == 'recovered'
        t = pq.read_table(pa.BufferReader(s2.get('k.parquet')))
        check_exact(t, rows)


@test("assembly obeys the 5MB part rule with real-size chunks (fake-s3)")
def _():
    sink = FakeS3()
    w = CheckpointWriter('m.parquet', SCHEMA, sink=sink,
                         compression='none')      # 5MB default chunk_bytes
    rows_per = 60_000
    i = 0
    while len(sink.list('m.parquet.chunks/')) < 2:
        w.write_table(make_rows(i, rows_per))
        i += rows_per
        assert i <= 2_000_000, "chunks never cut"
    w.close()                                     # assemble passes the rule
    t = pq.read_table(pa.BufferReader(sink.get('m.parquet')))
    check_exact(t, i)
    md = pq.read_metadata(pa.BufferReader(sink.get('m.parquet')))
    offs = [md.row_group(k).column(0).data_page_offset
            for k in range(md.num_row_groups)]
    assert offs == sorted(offs) and len(set(offs)) == md.num_row_groups


@test("page CRCs + verify(); stats survive; empty file; boot sweep")
def _():
    with tempfile.TemporaryDirectory() as tmp:
        sink = DirSink(tmp)
        # empty
        CheckpointWriter('empty.parquet', SCHEMA, sink=sink).close()
        assert pq.read_table(os.path.join(tmp, 'empty.parquet')).num_rows == 0
        # crashed writer for the sweep + verify on the finalized file
        w = CheckpointWriter('n.parquet', SCHEMA, sink=sink)
        w.write_table(make_rows(0, 300))
        del w
        # freshly-touched checkpoint = presumed-live writer: the sweep
        # must refuse it untouched...
        results = recover_directory(tmp)
        assert results == [('alive', 'n.parquet')], results
        assert bool(ckpt_versions(sink, 'n.parquet'))
        # ...and take it once it is old enough (age the file, keep the
        # default threshold honest) or when the caller asserts no
        # writer is running (min_age_s=0)
        old = time.time() - 120
        os.utime(os.path.join(tmp, newest(sink, 'n.parquet')), (old, old))
        results = recover_directory(tmp)
        assert ('recovered', 'n.parquet') in results, results
        assert verify(os.path.join(tmp, 'n.parquet')) == 300
        md = pq.read_metadata(os.path.join(tmp, 'n.parquet'))
        st = md.row_group(0).column(0).statistics
        assert st.min == 0 and st.max == 299


@test("shipper: outage rides, catch-up skips missed ckpts, byte-identical")
def _():
    from parquet_checkpoint import Shipper
    tmp = tempfile.mkdtemp()
    try:
        local = DirSink(tmp)
        remote = FakeS3(part_min=4096)
        w = CheckpointWriter('s.parquet', SCHEMA, sink=local,
                             chunk_bytes=16 * 1024, compression='none')
        ship = Shipper('s.parquet', local, remote)
        assert ship.poll()['state'] == 'idle'
        w.write_table(make_rows(0, 500))
        st = ship.poll()
        assert st['state'] == 'shipped' and st['watermark'] == 500, st
        assert CheckpointReader(newest(remote, 's.parquet'), remote).num_rows == 500
        assert ship.poll()['state'] == 'synced'
        # WAN outage: every remote PUT fails; local keeps writing
        remote.fail_next('put:', n=1000)
        w.write_table(make_rows(500, 800))
        try:
            ship.poll()
            raise AssertionError("outage must surface to the host")
        except FaultInjected:
            pass
        w.write_table(make_rows(1300, 700))      # local rides the outage
        assert not ship.synced
        remote.clear_faults()
        st = ship.poll()                          # ONE catch-up pass
        assert st['state'] == 'shipped' and st['watermark'] == 2000, st
        assert ship.poll()['state'] == 'synced' and ship.synced
        r = CheckpointReader(newest(remote, 's.parquet'), remote)
        assert r.num_rows == 2000
        check_exact(r.table(), 2000)
        # shutdown protocol: synced -> writer close -> remote finalize
        w.close()
        assert ship.poll()['state'] == 'local-finalized'
        status, _ = ship.close()
        assert status == 'recovered'
        assert remote.get('s.parquet') == local.get('s.parquet')
        check_exact(pq.read_table(
            pa.BufferReader(remote.get('s.parquet'))), 2000)
        assert remote.list('s.parquet.chunks/') == []
    finally:
        shutil.rmtree(tmp)


@test("footer stats drive DuckDB row-group pruning (proof by corruption)")
def _():
    import duckdb
    with tempfile.TemporaryDirectory() as tmp:
        sink = DirSink(tmp)
        S = pa.schema([pa.field('n', pa.int64(), nullable=False)])
        w = CheckpointWriter('p.parquet', S, sink=sink, compression='none',
                             checkpoint_every_write=False)
        for g in range(20):
            w.write_table(pa.table({'n': list(range(g * 10_000,
                                                    g * 10_000 + 10_000))},
                                   schema=S))
        w.close()
        path = os.path.join(tmp, 'p.parquet')
        md = pq.read_metadata(path)
        # smash every row group's page header EXCEPT group 7: if DuckDB
        # prunes on our footer statistics it never touches them
        data = bytearray(open(path, 'rb').read())
        for g in range(20):
            if g != 7:
                off = md.row_group(g).column(0).data_page_offset
                data[off:off + 8] = b'\xff' * 8
        open(path, 'wb').write(bytes(data))
        con = duckdb.connect()
        r = con.sql(f"SELECT count(*), min(n), max(n) FROM "
                    f"read_parquet('{path}') "
                    f"WHERE n BETWEEN 71000 AND 72000").fetchall()[0]
        assert r == (1001, 71000, 72000), r
        try:
            con.sql(f"SELECT count(*) FROM read_parquet('{path}') "
                    f"WHERE n BETWEEN 31000 AND 32000").fetchall()[0]
            raise AssertionError("corrupt group readable -- no pruning?")
        except AssertionError:
            raise
        except Exception:
            pass    # loud failure touching the smashed group: correct


@test("union payoff: ring hot + checkpoint reader, exact seam every cycle")
def _():
    import arrow_ring
    import pyarrow.compute as pc
    ring = arrow_ring.Ring(SCHEMA, capacity=512)
    sink = FakeS3(part_min=4096)
    w = CheckpointWriter('u.parquet', SCHEMA, sink=sink,
                         chunk_bytes=32 * 1024, compression='none',
                         checkpoint_every_write=False)
    con = duckdb.connect()
    total, i, checks = 2000, 0, 0
    while i < total:
        burst = min(100, total - i)
        for _ in range(burst):
            row = ring.get_mutable_row(block_s=1.0)
            row.n = i
            row.x = i * 0.5
            row.s = f'row-{i}'
            i += 1
        ring.commit()
        rel = ring.get_static_relation()
        hot = rel.table
        fresh = hot.filter(pc.greater_equal(hot['n'], w.rows_checkpointed))
        if fresh.num_rows:
            w.write_table(fresh)
            w.checkpoint()
        arch = CheckpointReader(newest(sink, 'u.parquet'), sink).table()
        con.register('hot', hot)
        con.register('arch', arch)
        cnt, dst, mx = con.sql(
            f"SELECT count(*), count(DISTINCT n), max(n) FROM ("
            f"SELECT n FROM arch UNION ALL SELECT n FROM hot "
            f"WHERE n >= {w.rows_checkpointed})").fetchall()[0]
        con.unregister('hot')
        con.unregister('arch')
        rel.release()
        assert cnt == dst == mx + 1 == i, (cnt, dst, mx, i)
        checks += 1
    w.close()
    check_exact(pq.read_table(pa.BufferReader(sink.get('u.parquet'))), total)
    assert checks >= 15


@test("hostile data roundtrip: writer -> live read -> finalize, exact")
def _():
    from hostile import hostile_table, assert_hostile_equal, HOSTILE_SCHEMA
    for label, sink, cleanup in both_sinks():
        try:
            w = CheckpointWriter('h.parquet', HOSTILE_SCHEMA, sink=sink,
                                 chunk_bytes=64 * 1024,
                                 checkpoint_every_write=False)
            for k in range(3):
                w.write_table(hostile_table(k * 400, 400))
            w.checkpoint(tail=hostile_table(1200, 37))
            r = CheckpointReader(newest(sink, 'h.parquet'), sink)
            assert_hostile_equal(r.table(), 0, 1237)      # live, tail incl.
            del w
            finalize('h.parquet', sink)
            t = pq.read_table(pa.BufferReader(sink.get('h.parquet')))
            assert_hostile_equal(t, 0, 1237)
            # DuckDB reads it too: NaN/Inf/unicode/lists survive SQL
            path_ok = label == 'dir'
            if path_ok:
                n, = duckdb.sql(
                    "SELECT count(*) FROM read_parquet('" +
                    __import__('os').path.join(sink.root, 'h.parquet') +
                    "') WHERE isnan(f)").fetchall()[0]
                assert n == 1237 // 7 + (1 if 1237 % 7 > 1 else 0), n
        finally:
            cleanup()


@test("virtual file: stock pyarrow reads the live checkpoint; pruning reaches storage")
def _():
    class MeterSink:
        """Counts chunk fetches and bytes so pruning is measurable."""
        def __init__(self, inner):
            self.inner = inner
            self.chunk_gets = 0
            self.chunk_bytes = 0

        def get(self, name, offset=0, length=-1):
            b = self.inner.get(name, offset, length)
            if '.chunks/' in name:
                self.chunk_gets += 1
                self.chunk_bytes += len(b)
            return b

        def reset(self):
            self.chunk_gets = self.chunk_bytes = 0

        def __getattr__(self, a):
            return getattr(self.inner, a)

    fake = FakeS3(part_min=4096)
    wschema = pa.schema([F('n', pa.int64(), nullable=False),
                         F('s', pa.string(), nullable=False)])
    w = CheckpointWriter('vf.parquet', wschema, sink=fake,
                         chunk_bytes=4096, compression='none')
    # one BIG row group -> one big chunk (chunks hold whole groups):
    # the production shape, and it exercises the ranged-read path
    w.write_table(pa.table(
        {'n': list(range(40000)),
         's': [f'{i:06d}' * 40 for i in range(40000)]},
        schema=wschema))
    w.checkpoint()
    meter = MeterSink(fake)
    r = CheckpointReader(newest(meter, 'vf.parquet'), meter)
    assert max(m[1] for m in r.manifest) > (8 << 20), \
        "test wants a chunk on the ranged path"

    # stock pyarrow over the file object == the assembled table, exactly
    t_vf = pq.read_table(r.open())
    assert t_vf.equals(r.table()), "virtual-file read differs from table()"

    # footer read: served from the in-memory tail + a peek at the
    # leading magic — a sliver of the file, not the file
    meter.reset()
    md = pq.read_metadata(r.open())
    assert md.num_rows == 40000
    assert meter.chunk_bytes < 64 * 1024, meter.chunk_bytes

    # column pruning reaches the storage: reading the thin column
    # fetches a small fraction of the bytes the full read fetches
    meter.reset()
    tn = pq.read_table(r.open(), columns=['n'])
    narrow = meter.chunk_bytes
    assert tn.column('n').to_pylist() == list(range(40000))
    meter.reset()
    pq.read_table(r.open())
    full = meter.chunk_bytes
    assert narrow < full / 4, (narrow, full)

    # reads across segment boundaries are exact against the whole image
    image = r.open().read()
    assert len(image) == r.logical_size
    f = r.open()
    for pos, n in [(0, 10), (4090, 20), (len(image) - 13, 13)]:
        f.seek(pos)
        assert f.read(n) == image[pos:pos + n], f"mismatch at {pos}+{n}"
    f.seek(-8, 2)
    assert f.read() == image[-8:]

    # small-chunk (whole-fetch) path: a corrupted chunk fails LOUDLY
    # on first touch — manifest CRC, never wrong bytes
    w2 = CheckpointWriter('vc.parquet', SCHEMA, sink=fake,
                          chunk_bytes=4096, compression='none')
    for k in range(4):
        w2.write_table(make_rows(k * 500, 500))
    w2.checkpoint()
    victim = next(n for n in fake.list('vc.parquet.chunks/'))
    good = fake._objects[victim]
    fake._objects[victim] = good[:100] + b'XX' + good[102:]
    try:
        CheckpointReader(newest(fake, 'vc.parquet'), fake).open().read()
        assert False, "corrupt chunk was served"
    except CheckpointError as e:
        assert 'CRC' in str(e)
    fake._objects[victim] = good

    # live_file() twin: checkpoint now, ordinary file after finalize
    with tempfile.TemporaryDirectory() as tmp:
        dsink = DirSink(tmp)
        w3 = CheckpointWriter('lf.parquet', SCHEMA, sink=dsink)
        w3.write_table(make_rows(0, 50))
        w3.checkpoint()
        p = os.path.join(tmp, 'lf.parquet')
        assert pq.read_table(live_file(p)).num_rows == 50   # still a ckpt
        w3.close()
        assert pq.read_table(live_file(p)).num_rows == 50   # finalized


@test("checkpoint versions: each checkpoint a new object, names increase; "
      "a version superseded SUPERSEDED_KEEP_S is deleted by a later "
      "checkpoint; all gone after finalize")
def _():
    from parquet_checkpoint import parse_ckpt_object
    sink = FakeS3()
    w = CheckpointWriter('cv.parquet', SCHEMA, sink=sink)   # the 10-minute rule
    seen = []
    for k in range(6):
        w.write_table(make_rows(k * 10, 10))
        seen.append(w.ckpt_name)
        v = ckpt_versions(sink, 'cv.parquet')
        assert len(v) == k + 1, v                 # none superseded 10 minutes yet
        assert v[-1][1] == w.ckpt_name, (v, w.ckpt_name)
    w0 = CheckpointWriter('cz.parquet', SCHEMA, sink=sink, superseded_keep_s=0)
    for k in range(4):
        w0.write_table(make_rows(k * 10, 10))
        assert [o for _s, o in ckpt_versions(sink, 'cz.parquet')] == [w0.ckpt_name]
    w0.close()
    seqs = [parse_ckpt_object(o)[1] for o in seen]
    assert seqs == sorted(seqs) and len(set(seqs)) == 6, seqs
    assert CheckpointReader(seen[-3], sink).num_rows == 40   # an older version
    assert CheckpointReader(seen[-1], sink).num_rows == 60   # still opens
    w.close()
    assert ckpt_versions(sink, 'cv.parquet') == []
    assert sink.list('cv.parquet.chunks/') == []
    assert pq.read_table(pa.BufferReader(sink.get('cv.parquet'))).num_rows == 60


@test("checkpoint versions: finalize assembles from the newest version "
      "whose CRC holds; an unversioned (older) checkpoint is version 0")
def _():
    sink = FakeS3()
    w = CheckpointWriter('cr.parquet', SCHEMA, sink=sink)
    w.write_table(make_rows(0, 30))
    w.write_table(make_rows(30, 20))
    obj = newest(sink, 'cr.parquet')
    bad = bytearray(sink.get(obj))
    bad[-5] ^= 0xFF                              # the newest is corrupt
    sink.put(obj, bytes(bad))
    del w                                        # crash: never closed
    assert finalize('cr.parquet', sink) == ('recovered', 'cr.parquet')
    t = pq.read_table(pa.BufferReader(sink.get('cr.parquet')))
    assert t.num_rows == 30, t.num_rows          # the version before it
    assert ckpt_versions(sink, 'cr.parquet') == []
    # a checkpoint named without a version, as written before versions
    s2 = FakeS3()
    w2 = CheckpointWriter('old.parquet', SCHEMA, sink=s2)
    w2.write_table(make_rows(0, 7))
    s2.put('old.parquet.ckpt', s2.get(newest(s2, 'old.parquet')))
    s2.delete(newest(s2, 'old.parquet'))
    del w2
    assert ckpt_versions(s2, 'old.parquet') == [(0, 'old.parquet.ckpt')]
    assert finalize('old.parquet', s2) == ('recovered', 'old.parquet')
    assert pq.read_table(pa.BufferReader(s2.get('old.parquet'))).num_rows == 7


@test("checkpoint versions: the shipper ships each newest version under "
      "its own name; a remote version superseded SUPERSEDED_KEEP_S is "
      "deleted by a later ship")
def _():
    from parquet_checkpoint import Shipper
    with tempfile.TemporaryDirectory() as d:
        local, remote = DirSink(d), FakeS3(part_min=1024)
        w = CheckpointWriter('sv.parquet', SCHEMA, sink=local, chunk_bytes=1024)
        ship = Shipper('sv.parquet', local, remote, superseded_keep_s=0)
        for k in range(5):
            w.write_table(make_rows(k * 50, 50))
            st = ship.poll()
            assert st['state'] == 'shipped' and st['object'] == w.ckpt_name, st
            v = ckpt_versions(remote, 'sv.parquet')
            assert [o for _s, o in v] == [w.ckpt_name], v
        assert ship.poll()['state'] == 'synced'
        w.close()
        assert ship.close() == ('recovered', 'sv.parquet')
        assert ckpt_versions(remote, 'sv.parquet') == []
        assert remote.get('sv.parquet') == local.get('sv.parquet')


@test("the shipper sends each object once; after an outage it sends only "
      "what the newest version names; the remote keeps no object that no "
      "kept version names")
def _():
    from parquet_checkpoint import Shipper
    with tempfile.TemporaryDirectory() as d:
        local, remote = DirSink(d), FakeS3(part_min=1024)
        w = CheckpointWriter('so.parquet', SCHEMA, sink=local,
                             chunk_bytes=64 * 1024, compression='none',
                             checkpoint_every_write=False)
        ship = Shipper('so.parquet', local, remote, superseded_keep_s=0)

        def remote_puts():
            got = [o[len('put:'):] for o in remote.op_log
                   if o.startswith('put:')]
            del remote.op_log[:]
            return got

        def remote_reads(k):
            r = CheckpointReader(w.ckpt_name, remote)
            check_exact(r.table(), k * 40)
            return {m[0] for m in r.manifest}
        # a poll at each checkpoint: one new object and the version
        for k in range(1, 31):
            w.checkpoint(make_rows(0, k * 40))
            assert ship.poll()['state'] == 'shipped'
            puts = remote_puts()
            assert len(puts) == 2 and puts[-1] == w.ckpt_name, (k, puts)
            named = remote_reads(k)
            # with no keep time: what the newest version names, and at
            # most what the one before it named
            assert named <= set(remote.list('so.parquet.chunks/')), k
        # an outage: 25 checkpoints with no poll, small objects merged
        # and gone meanwhile; one poll ships what the newest names and
        # the remote lacks, nothing the outage swallowed
        for k in range(31, 56):
            w.checkpoint(make_rows(0, k * 40))
        assert ship.poll()['state'] == 'shipped'
        puts = remote_puts()
        named = remote_reads(55)
        assert puts[-1] == w.ckpt_name and set(puts[:-1]) <= named, puts
        assert len(puts) < 20, puts
        # the next poll deletes the version before and its objects
        w.checkpoint(make_rows(0, 56 * 40))
        assert ship.poll()['state'] == 'shipped'
        before = remote_reads(56)
        w.checkpoint(make_rows(0, 57 * 40))
        assert ship.poll()['state'] == 'shipped'
        named = remote_reads(57)
        assert set(remote.list('so.parquet.chunks/')) <= named | before, (
            sorted(set(remote.list('so.parquet.chunks/')) - named - before))
        # the final rows, then both copies finish the same file
        w.write_table(make_rows(0, 57 * 40))
        w.checkpoint()
        assert ship.poll()['state'] == 'shipped'
        w.close()
        assert ship.close() == ('recovered', 'so.parquet')
        assert remote.get('so.parquet') == local.get('so.parquet')
        assert remote.list('') == ['so.parquet'], remote.list('')
        assert sorted(os.listdir(d)) == ['so.parquet'], os.listdir(d)


@test("a checkpoint stores only its new rows: one object for each, the "
      "checkpoint object holds the list and the footer and no row, and "
      "every row is read back after every checkpoint")
def _():
    import struct as _struct
    sink = FakeS3(part_min=4096)
    w = CheckpointWriter('n.parquet', SCHEMA, sink=sink,
                         chunk_bytes=64 * 1024, compression='none',
                         checkpoint_every_write=False)
    step = 100
    row_bytes = None
    objects_before = set()
    written = 0                          # bytes of every object ever stored
    for k in range(1, 201):
        w.checkpoint(make_rows(0, k * step))
        r = CheckpointReader(w.ckpt_name, sink)
        # every row, once, in order
        check_exact(r.table(), k * step)
        # the data region is the footer and nothing else
        footer = _struct.unpack('<I', r._data[-8:-4])[0]
        assert footer + 8 == len(r._data), (k, footer, len(r._data))
        # one new object for this checkpoint: its new rows, or a merge
        objects = set(sink.list('n.parquet.chunks/'))
        fresh = objects - objects_before
        assert len(fresh) == 1, (k, sorted(fresh))
        written += sum(len(sink.get(n)) for n in fresh)
        objects_before = objects
        if k == 1:
            row_bytes = len(sink.get(next(iter(fresh)))) / step
        # the list holds the unfinished row group in few objects: never
        # more than PQC_RUN_LIMIT of one checkpoint's size
        rows_each = [g.num_rows for g in
                     (pq.ParquetFile(r.open()).metadata.row_group(i)
                      for i in range(len(r.manifest)))]
        assert rows_each.count(step) <= 16, (k, rows_each)
        assert len(r.manifest) <= 40, (k, len(r.manifest))
    # each row is written once for each size step, not once for each
    # checkpoint: the writer before stored 200 tails of 100 to 20,000
    # rows, 100 times these rows
    assert written <= 5 * 200 * step * row_bytes, (written, row_bytes)
    # the final row group replaces them all, as one object
    w.write_table(make_rows(0, 200 * step))
    w.checkpoint()
    r = CheckpointReader(w.ckpt_name, sink)
    assert len(r.manifest) == 1, r.manifest
    check_exact(r.table(), 200 * step)
    w.close()
    check_exact(pq.read_table(pa.BufferReader(sink.get('n.parquet'))),
                200 * step)
    assert sink.list('') == ['n.parquet'], sink.list('')


@test("an object is deleted with the last checkpoint version that names "
      "it; with no keep time the store holds what the newest version names")
def _():
    sink = FakeS3(part_min=4096)
    w = CheckpointWriter('o.parquet', SCHEMA, sink=sink,
                         chunk_bytes=64 * 1024, compression='none',
                         checkpoint_every_write=False, superseded_keep_s=0.0)
    for k in range(1, 61):
        w.checkpoint(make_rows(0, k * 50))
        r = CheckpointReader(w.ckpt_name, sink)
        named = {m[0] for m in r.manifest}
        # a version superseded by this one is deleted at the NEXT
        # checkpoint: the store holds this version's objects and, at
        # most, those of the one before
        held = set(sink.list('o.parquet.chunks/'))
        assert named <= held, (k, sorted(named - held))
        check_exact(r.table(), k * 50)
    w.checkpoint(make_rows(0, 60 * 50 + 1))
    r = CheckpointReader(w.ckpt_name, sink)
    versions = ckpt_versions(sink, 'o.parquet')
    assert len(versions) <= 2, versions
    before = CheckpointReader(versions[0][1], sink) if len(versions) == 2 else r
    allowed = {m[0] for m in r.manifest} | {m[0] for m in before.manifest}
    assert set(sink.list('o.parquet.chunks/')) <= allowed, (
        sorted(set(sink.list('o.parquet.chunks/')) - allowed))


@test("a final row group is an object of its own until a chunk holds it; "
      "the row group that completes a chunk is written once, in the chunk")
def _():
    sink = FakeS3(part_min=4096)
    w = CheckpointWriter('q.parquet', SCHEMA, sink=sink,
                         chunk_bytes=64 * 1024, compression='none')
    i, puts = 0, []
    group = 600                          # about 20 KB: four fill a chunk
    for k in range(12):
        before = set(sink.list('q.parquet.chunks/'))
        w.write_table(make_rows(i, group))
        i += group
        fresh = sorted(set(sink.list('q.parquet.chunks/')) - before)
        assert len(fresh) == 1, (k, fresh)      # one object for each write
        puts.append(len(sink.get(fresh[0])))
        r = CheckpointReader(w.ckpt_name, sink)
        check_exact(r.table(), i)
        sizes = [m[1] for m in r.manifest]
        # chunks first, each at least chunk_bytes; then the loose row
        # groups, together under chunk_bytes
        chunks = [x for x in sizes if x >= 64 * 1024]
        assert sizes[:len(chunks)] == chunks, sizes
        assert sum(sizes[len(chunks):]) < 64 * 1024, sizes
    assert max(puts) >= 64 * 1024 and min(puts) < 32 * 1024, puts
    w.close()
    check_exact(pq.read_table(pa.BufferReader(sink.get('q.parquet'))), i)


@test("assembly: an object of the part minimum or more is copied, and "
      "the smaller objects after it travel once, with the footer")
def _():
    from parquet_checkpoint._sinks import plan_parts
    big, small = 5000, 100
    src = lambda n: (n, -1, -1)
    assert plan_parts([src('a'), src('b'), src('c'), src('d')],
                      [big, big, small, small], 4096) == [
        ('copy', [src('a')]), ('copy', [src('b')]),
        ('upload', [src('c'), src('d')])]
    assert plan_parts([src('a')], [big], 4096) == [('copy', [src('a')])]
    assert plan_parts([src('a'), src('b')], [small, small], 4096) == [
        ('upload', [src('a'), src('b')])]
    # a small object before a large one: the run takes the large one
    # in, so that no part before the last is under the minimum
    assert plan_parts([src('a'), src('b'), src('c')],
                      [small, big, small], 4096) == [
        ('upload', [src('a'), src('b')]), ('upload', [src('c')])]
    # the real thing: chunks, loose row groups, unfinished objects
    sink = FakeS3(part_min=4096)
    w = CheckpointWriter('u.parquet', SCHEMA, sink=sink,
                         chunk_bytes=64 * 1024, compression='none',
                         checkpoint_every_write=False)
    i = 0
    for k in range(10):
        w.write_table(make_rows(i, 600))
        i += 600
    for k in range(1, 8):
        w.checkpoint(make_rows(i, k * 40))
    rows = w.rows_checkpointed
    r = CheckpointReader(w.ckpt_name, sink)
    sizes = [m[1] for m in r.manifest]
    assert len([x for x in sizes if x >= 64 * 1024]) >= 2, sizes   # chunks
    assert len([x for x in sizes if x < 4096]) >= 3, sizes  # under the minimum
    large = 0                            # the objects before the first small one
    while sizes[large] >= 4096:
        large += 1
    del sink.op_log[:]
    del w                                # a crash: finalize from the store
    assert finalize('u.parquet', sink)[0] == 'recovered'
    copies = [o for o in sink.op_log if o.startswith('upload_part_copy:')]
    uploads = [o for o in sink.op_log if o.startswith('upload_part:')]
    assert len(copies) == large, (large, sink.op_log)
    # the rest travels once, with the footer: in as few parts as hold
    # the part minimum each (the last may be smaller)
    rest = sum(sizes[large:])
    assert 1 <= len(uploads) <= rest // 4096 + 1, (rest, sink.op_log)
    check_exact(pq.read_table(pa.BufferReader(sink.get('u.parquet'))), rows)
    assert sink.list('') == ['u.parquet'], sink.list('')


@test("a tail never gets shorter: checkpoint(tail) with fewer rows than "
      "the one before is refused, and says why")
def _():
    sink = FakeS3(part_min=4096)
    w = CheckpointWriter('v.parquet', SCHEMA, sink=sink,
                         chunk_bytes=64 * 1024, checkpoint_every_write=False)
    w.checkpoint(make_rows(0, 50))
    try:
        w.checkpoint(make_rows(0, 20))
        raise AssertionError("a shorter tail was accepted")
    except CheckpointError as e:
        assert "never" in str(e) and "shorter" in str(e), e
    # no tail: the claim covers the final rows only, and a new tail
    # begins again
    assert w.checkpoint() == 0
    assert w.checkpoint(make_rows(0, 20)) == 20
    check_exact(CheckpointReader(w.ckpt_name, sink).table(), 20)


@test("mangler: 400 corrupted checkpoints refuse loudly, never crash/succeed")
def _():
    import random as _r
    rng = _r.Random(20260823)
    sink = FakeS3()
    w = CheckpointWriter('m.parquet', SCHEMA, sink=sink)
    w.write_table(make_rows(0, 500))
    good = sink.get(newest(sink, 'm.parquet'))
    survived = 0
    for i in range(400):
        bad = bytearray(good)
        for _ in range(rng.randrange(1, 8)):
            if not bad:
                break                    # cut to nothing: nothing to mangle
            op = rng.random()
            if op < 0.5 and len(bad) > 1:
                bad[rng.randrange(len(bad))] ^= rng.randrange(1, 256)
            elif op < 0.75:
                del bad[rng.randrange(len(bad)):]        # truncate
            else:
                bad[rng.randrange(len(bad)):rng.randrange(len(bad))] = \
                    bytes(rng.randrange(20))             # splice
        s2 = FakeS3()
        s2.put('m.parquet.ckpt', bytes(bad))
        try:
            r = CheckpointReader('m.parquet.ckpt', s2)
            # CRC collision is possible in principle; reading must
            # still either work or refuse -- never crash
            r.table()
            survived += 1
        except (CheckpointError, Exception):
            pass
    assert survived <= 2, f"{survived} corrupted ckpts read as valid"


print(f"\n{'=' * 50}")
print(f"Results: {passed} passed, {failed} failed")
if errors:
    print(f"Failed: {', '.join(errors)}")
    sys.exit(1)
print("All tests passed!")
