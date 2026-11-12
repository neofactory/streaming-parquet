"""Standalone test runner for streaming_parquet (no pytest dependency).

Run:  python3 streaming_parquet/run_tests.py

Streams on the arrow_ring core, the Arrow-schema API, attach()
local/remote, the reader verbs (newest_row, cursor, arrow_table, the
three DuckDB views), commit-driven publishing, the archive (cut,
start/stop, checkpoints, the S3 mirror), playback clocks, and a real
Flight loopback. Each test tears the module down (sp.close()) so
state is fresh.
"""

import os
import signal
import sys
import tempfile
import time
import traceback

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streaming_parquet as sp
import parquet_checkpoint  # noqa: E402 (path set above)


def _newest_ckpt(names):
    """The newest checkpoint version among object names, or None."""
    import parquet_checkpoint
    got = [(parquet_checkpoint.parse_ckpt_object(n)[1], n) for n in names
           if parquet_checkpoint.parse_ckpt_object(n) is not None]
    return max(got)[1] if got else None
from streaming_parquet import StaleReadError, PinStalledError, MemoryOptions, ParquetOptions, ArchiveState, MirrorState

signal.alarm(180)   # global hang guard


class _ClosingTempDir(tempfile.TemporaryDirectory):
    """A test's temp directory closes the streams still writing into it
    before it goes: a stream left open keeps writing its files, ticket
    and catalog into a directory being deleted."""

    def cleanup(self):
        for st in sp._get_streams().values():
            where = [st._store if isinstance(st._store, str) else None,
                     st.archive_state().location]
            if any(isinstance(w, str) and w.startswith(self.name) for w in where):
                st.close()
        super().cleanup()


tempfile.TemporaryDirectory = _ClosingTempDir



F = pa.field
SCHEMA = pa.schema([
    F('tv', pa.float64(), nullable=False),
    F('servo', pa.struct([F('pos', pa.int32(), nullable=False),
                          F('vel', pa.int32(), nullable=False)]),
      nullable=False),
    F('mark', pa.int32(), nullable=True),
    F('label', pa.utf8(), nullable=False),
])

passed = 0
failed = 0
errors = []


def test(name):
    def decorator(fn):
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
        finally:
            try:
                sp.close()
            except Exception:
                pass
    return decorator


def all_rows(ring):
    """A record set over every row the ring holds."""
    return ring.pin(0, ring.newest_seq + 1)


def fill(stream, count, start=0):
    for i in range(start, start + count):
        row = stream.get_mutable_row(block_s=1.0)
        row.tv = float(i)
        row.servo.pos = i * 10
        row.servo.vel = -i
        row.mark = i if i % 2 == 0 else None
        row.label = f'r{i}'
    stream.commit()


def view_con():
    """A DuckDB connection the archive views can use: the
    parquet_checkpoint extension needs allow_unsigned_extensions."""
    return duckdb.connect(config={"allow_unsigned_extensions": "true"})


def wait_for(cond, timeout=8.0, what="condition"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return
        time.sleep(0.05)
    raise AssertionError(f"timed out waiting for {what}")


print("\n--- local streams ---")

@test("create stream, write rows, local attach reads")
def _():
    s = sp.create_stream("v2test/basic", SCHEMA, memory=MemoryOptions(lookback=500))
    fill(s, 10)
    a = sp.connect("v2test/basic")
    assert a.state == "streaming"
    assert a._newest_seq == 9
    r = a._read_row(3)
    assert r.tv == 3.0 and r.servo.pos == 30 and r.servo.vel == -3
    assert r.label == 'r3'
    assert a._read_row(1).mark is None and a._read_row(2).mark == 2


@test("append: a dict row and a record batch land identically")
def _():
    # a dict row is for flat schemas (struct fields are set through
    # row.servo.pos leaves; a dict write to a struct raises).
    flat = pa.schema([F('tv', pa.float64(), nullable=False),
                      F('label', pa.utf8(), nullable=False)])
    s = sp.create_stream("v2test/writes2", flat, memory=MemoryOptions(lookback=500))
    s.append({'tv': 1.0, 'label': 'a'})
    s.append(pa.record_batch({'tv': [2.0, 3.0],
                                    'label': ['b', 'c']}, schema=flat))
    a = sp.connect("v2test/writes2")
    assert a._newest_seq == 2
    assert [a._read_row(i).label for i in range(3)] == ['a', 'b', 'c']


@test("local attach accepts memory (the publisher's ring as it is); "
      "rejects columns")
def _():
    s = sp.create_stream("v2test/local", SCHEMA, memory=MemoryOptions(lookback=100))
    fill(s, 80)
    a = sp.connect("v2test/local", memory=MemoryOptions(lookback=368_640))
    assert a.newest_row() is not None
    a.close()
    try:
        sp.connect("v2test/local", columns=['tv'])
        assert False, "columns should be rejected for local attach"
    except ValueError:
        pass


@test("a record set over the ring's rows via a._ring + DuckDB over the hot window")
def _():
    s = sp.create_stream("v2test/rel", SCHEMA, memory=MemoryOptions(lookback=500))
    fill(s, 100)
    a = sp.connect("v2test/rel")
    with all_rows(a._ring) as rs:
        assert (rs.lo, rs.hi) == (0, 100)
        con = duckdb.connect()
        con.register('hot', rs.table)
        n, mx = con.sql("SELECT count(*), max(tv) FROM hot").fetchall()[0]
        assert n == 100 and mx == 99.0
        marks = con.sql("SELECT count(mark) FROM hot").fetchall()[0][0]
        assert marks == 50          # odd rows are NULL


@test("hot relation: register once, stays current")
def _():
    s = sp.create_stream("v2test/dyn", SCHEMA, memory=MemoryOptions(lookback=500))
    fill(s, 20)
    a = sp.connect("v2test/dyn")
    con = duckdb.connect()
    live = a.duckdb_live(con)
    assert con.sql(f"SELECT max(tv) FROM {live.name}").fetchall()[0][0] == 19.0
    fill(s, 20, start=20)
    wait_for(lambda: a._newest_seq == 39, what="rows over Flight")
    assert con.sql(f"SELECT max(tv) FROM {live.name}").fetchall()[0][0] == 39.0
    live.close()


@test("seq_at_or_after finds the window edge")
def _():
    s = sp.create_stream("v2test/seek", SCHEMA, memory=MemoryOptions(lookback=500))
    fill(s, 50)
    a = sp.connect("v2test/seek")
    assert a._seq_at_or_after('tv', 25.0) == 25
    assert a._seq_at_or_after('tv', 24.5) == 25
    assert a._seq_at_or_after('tv', 0.0) == 0
    assert a._seq_at_or_after('tv', 100.0) == -1


@test("wrap: old rows recycle, StaleReadError is loud")
def _():
    s = sp.create_stream("v2test/wrap", SCHEMA, memory=MemoryOptions(lookback=32))
    fill(s, 50)
    a = sp.connect("v2test/wrap")
    assert a._oldest_seq == 18 and a._newest_seq == 49
    try:
        a._read_row(5)
        assert False, "recycled seq should raise"
    except StaleReadError:
        pass
    assert a._read_row(49).label == 'r49'


@test("a held pin on the oldest row stalls the writer, loudly, then releases")
def _():
    s = sp.create_stream("v2test/pin", SCHEMA, memory=MemoryOptions(lookback=32))
    fill(s, 32)
    a = sp.connect("v2test/pin")
    rs = all_rows(a._ring)
    try:
        s.get_mutable_row(block_s=0.05)
        assert False, "pinned ring should stall the writer"
    except PinStalledError as e:
        assert "a pin on [0, 32)" in str(e), e
    rs.release()
    row = s.get_mutable_row()
    row.tv = 99.0
    row.servo.pos = 0
    row.servo.vel = 0
    row.label = 'after'
    s.commit()
    assert a._read_row(32).label == 'after'


print("\n--- wire maps ---")

@test("wire map scatter/gather through a stream")
def _():
    import struct as _s
    flat = pa.schema([F('cnt', pa.uint16(), nullable=False),
                      F('pos', pa.float64(), nullable=False)])
    s = sp.create_stream("v2test/wire", flat, memory=MemoryOptions(lookback=100))
    wm = s.wire_map([('cnt', 0, 2), ('pos', 2, 8)])
    for i in range(5):
        frame = _s.pack('<Hd', i, i * 1.5)
        s.get_mutable_row()
        s.from_wire(wm, frame)
    s.commit()
    a = sp.connect("v2test/wire")
    assert a._read_row(3).pos == 4.5
    assert s._ring.to_wire(wm, 3, 10) == _s.pack('<Hd', 3, 4.5)


print("\n--- parquet sessions ---")

@test("session-gated parquet with label; rows land; file readable")
def _():
    with tempfile.TemporaryDirectory() as tmp:
        s = sp.create_stream("v2test/pq", SCHEMA, memory=MemoryOptions(lookback=500), store=tmp, parquet=ParquetOptions(row_group_seconds=0.05))
        s.start_parquet("O9999")
        fill(s, 40)
        time.sleep(0.1)          # exceed row_group_seconds
        s.stop_parquet()
        pq_dir = s.archive_state().location
        wait_for(lambda: any('O9999' in f for f in os.listdir(pq_dir))
                 if os.path.isdir(pq_dir) else False,
                 what="labeled parquet file")

        def total_rows():
            return sum(pq.read_table(os.path.join(pq_dir, f)).num_rows
                       for f in os.listdir(pq_dir) if f.endswith('.parquet'))
        wait_for(lambda: total_rows() == 40, what="all rows flushed")
        # the finished file (its checkpoints stay until the catalog lists it)
        f = [f for f in os.listdir(pq_dir)
             if 'O9999' in f and f.endswith('.parquet')][0]
        t = pq.read_table(os.path.join(pq_dir, f))
        assert t.column('label').to_pylist()[0] == 'r0'
        assert t.column('servo').to_pylist()[3] == {'pos': 30, 'vel': -3}


@test("checkpoint_seconds: pending rows durable + readable between flushes")
def _():
    import parquet_checkpoint
    with tempfile.TemporaryDirectory() as tmp:
        # the row group (ring/2 = 250 rows) is never reached: only the tail
        # checkpoint makes the rows durable
        s = sp.create_stream("v2test/ck", SCHEMA, memory=MemoryOptions(lookback=500), store=tmp, parquet=ParquetOptions(checkpoint_seconds=0.05))
        s._parquet.superseded_keep_s = 0   # checkpoints go as soon as superseded
        s.start_parquet("CKPT")
        fill(s, 30)
        time.sleep(0.1)                  # exceed checkpoint_seconds
        fill(s, 12, start=30)
        pq_dir = s.archive_state().location

        def live_rows():
            if not os.path.isdir(pq_dir):
                return None
            newest = _newest_ckpt(os.listdir(pq_dir))
            if newest is None:
                return None
            final = os.path.join(
                pq_dir, parquet_checkpoint.parse_ckpt_object(newest)[0])
            return parquet_checkpoint.live_table(final)
        # every committed row is readable from the checkpoint alone --
        # no row group was ever flushed (threshold is 1M rows)
        wait_for(lambda: live_rows() is not None
                 and live_rows().num_rows == 42,
                 what="tail checkpoint covering all committed rows")
        t = live_rows()
        assert t.column('tv').to_pylist() == [float(i) for i in range(42)]
        # a crash HERE would lose zero rows; clean close finalizes
        s.stop_parquet()
        wait_for(lambda: any(f.endswith('.parquet') and 'CKPT' in f
                             for f in os.listdir(pq_dir))
                 and not any(f.endswith('.ckpt')
                             for f in os.listdir(pq_dir)),
                 what="finalized file, checkpoint swept")
        f = [f for f in os.listdir(pq_dir) if 'CKPT' in f][0]
        t = pq.read_table(os.path.join(pq_dir, f))
        assert t.num_rows == 42
        assert t.column('tv').to_pylist() == [float(i) for i in range(42)]


@test("an idle stream writes no checkpoint: the tail is claimed once")
def _():
    with tempfile.TemporaryDirectory() as tmp:
        s = sp.create_stream("v2test/idle", SCHEMA, memory=MemoryOptions(lookback=500), store=tmp, parquet=ParquetOptions(checkpoint_seconds=0.05))
        s.start_parquet("IDLE")
        fill(s, 10)
        time.sleep(0.1)
        pq_dir = s.archive_state().location

        def ckpt_mtime():
            # each checkpoint is a new version: the newest one's name
            return (_newest_ckpt(os.listdir(pq_dir))
                    if os.path.isdir(pq_dir) else None)
        wait_for(lambda: ckpt_mtime() is not None, what="first tail checkpoint")
        first = ckpt_mtime()
        for _ in range(8):               # idle: many cadences, no new row
            time.sleep(0.06)
        time.sleep(0.1)
        assert ckpt_mtime() == first, "idle stream rewrote its checkpoint"
        fill(s, 5, start=10)
        time.sleep(0.06)
        wait_for(lambda: ckpt_mtime() != first, what="checkpoint after new rows")


@test("start_parquet records from THIS moment (no pre-start rows)")
def _():
    with tempfile.TemporaryDirectory() as tmp:
        s = sp.create_stream("v2test/pq2", SCHEMA, memory=MemoryOptions(lookback=500))
        # no archive -> recording off; write some rows first
        sp.close()
        s = sp.create_stream("v2test/pq3", SCHEMA, memory=MemoryOptions(lookback=500), store=tmp, parquet=ParquetOptions(row_group_seconds=0.05))
        s.stop_parquet()                 # gate off
        fill(s, 25)                      # pre-session rows
        s.start_parquet("SES")
        fill(s, 15, start=25)            # session rows
        time.sleep(0.1)
        s.stop_parquet()
        pq_dir = s.archive_state().location

        def session_rows():
            if not os.path.isdir(pq_dir):
                return None
            fs = [f for f in os.listdir(pq_dir)
                  if 'SES' in f and f.endswith('.parquet')]
            if not fs:
                return None
            return pq.read_table(os.path.join(pq_dir, fs[0]))
        wait_for(lambda: session_rows() is not None
                 and session_rows().num_rows == 15,
                 what="session file with exactly the session's rows")
        t = session_rows()
        assert t.column('tv').to_pylist()[0] == 25.0


@test("hostile schema end to end: Flight -> ring -> archive -> relation")
def _():
    from hostile import hostile_table, assert_hostile_equal, HOSTILE_SCHEMA
    with tempfile.TemporaryDirectory() as tmp:
        s = sp.create_stream("v2test/hostile", HOSTILE_SCHEMA, memory=MemoryOptions(lookback=6000), store=tmp, parquet=ParquetOptions(checkpoint_seconds=0.15, row_group_rows=500, row_group_seconds=0.4))
        a = sp.connect(ticket=s._ticket, memory=MemoryOptions(lookback=6000))
        wait_for(lambda: a.state == "streaming", what="feeder connect")
        N = 2000
        for k in range(4):
            s.append(hostile_table(k * 500, 500).to_batches()[0])
            time.sleep(0.25)
        time.sleep(0.5)
        wait_for(lambda: a._newest_seq == N - 1, what="hostile rows over Flight")
        # the attached ring carried nulls/NaN/unicode/lists intact
        with all_rows(a._ring) as rs:
            assert_hostile_equal(rs.table, 0, N)
        # and the whole timeline (catalog + unfinished file + memory) is exact
        con = view_con()
        rel = a.duckdb_all(con, store=tmp)      # a direct ticket names no store
        wait_for(lambda: con.sql(f"SELECT count(*) FROM {rel.name}").fetchall()[0][0] == N,
                 what="the whole timeline")
        t = con.sql(f"SELECT * FROM {rel.name} ORDER BY n").to_arrow_table()
        assert_hostile_equal(t, 0, N)
        # SQL can actually work with the hostile values
        nan_n, uni = con.sql(
            f"SELECT count(*) FILTER (isnan(f)), "
            f"count(*) FILTER (s LIKE '%🎯%') FROM {rel.name}").fetchall()[0]
        assert nan_n == len([i for i in range(N) if i % 7 == 1]), nan_n
        assert uni == len([i for i in range(N) if i % 5 == 3]), uni
        rel.close()
        a.close()


@test("archive view on a live reader: the catalog, the unfinished file and "
      "memory, one view, exact; current as the stream writes and cuts")
def _():
    with tempfile.TemporaryDirectory() as tmp:
        flat = pa.schema([F('n', pa.int64(), nullable=False),
                          F('x', pa.float64(), nullable=False)])
        s = sp.create_stream("v2test/cr", flat, memory=MemoryOptions(lookback=5000), store=tmp, parquet=ParquetOptions(checkpoint_seconds=0.2, row_group_rows=250, row_group_seconds=0.5))

        def burst(a, k):
            s.append(pa.record_batch(
                [pa.array(range(a, a + k)),
                 pa.array([i * 0.5 for i in range(a, a + k)])],
                schema=flat))

        def counts(rel):
            return con.sql(f"SELECT count(*), count(DISTINCT n), min(n), max(n) "
                           f"FROM {rel.name}").fetchall()[0]
        for w in range(4):
            burst(w * 200, 200)
            time.sleep(0.3)
        time.sleep(0.5)
        a = sp.connect(ticket=s._ticket, memory=MemoryOptions(lookback=5000))
        wait_for(lambda: a.state == "streaming", what="feeder connect")
        con = view_con()
        rel = a.duckdb_all(con, store=tmp)       # a direct ticket names no store
        # mid-stream attach: the history before the attach comes from the
        # store, though the connection never carried it
        assert counts(rel) == (800, 800, 0, 799), counts(rel)
        # keep writing: no refresh call; the view follows the ticket
        for w in range(4, 6):
            burst(w * 200, 200)
            time.sleep(0.3)
        wait_for(lambda: counts(rel) == (1200, 1200, 0, 1199), what="1200 rows")
        # a cut: the file finishes, the catalog lists it, the view follows
        s.cut()
        wait_for(lambda: any(f.endswith('.parquet') for f in
                             os.listdir(s.archive_state().location)),
                 what="finalized file")
        assert counts(rel) == (1200, 1200, 0, 1199), counts(rel)
        burst(1200, 100)
        wait_for(lambda: counts(rel) == (1300, 1300, 0, 1299), what="1300 rows")
        rel.close()
        a.close()


@test("flush_parquet(label): cut at a boundary, no row skipped, label set then cleared")
def _():
    with tempfile.TemporaryDirectory() as tmp:
        s = sp.create_stream("v2test/cut", SCHEMA, memory=MemoryOptions(lookback=500), store=tmp, parquet=ParquetOptions())
        fill(s, 30)
        s.cut(label="O1105")         # cut; next file labeled
        fill(s, 20, start=30)
        time.sleep(0.3)
        s.cut(label=None)            # cut; next file unlabeled
        fill(s, 10, start=50)
        time.sleep(0.3)
        s.cut()                      # cut; label stays cleared
        loc = s.archive_state().location
        wait_for(lambda: len([f for f in os.listdir(loc)
                              if f.endswith('.parquet')]) == 3,
                 what="three finalized files")
        files = sorted(os.listdir(loc))
        labeled = [f for f in files if '-O1105' in f and f.endswith('.parquet')]
        assert len(labeled) == 1, files
        total = sum(pq.read_table(os.path.join(loc, f)).num_rows
                    for f in files if f.endswith('.parquet'))
        assert total == 60, (total, files)
        assert pq.read_table(os.path.join(loc, labeled[0])).num_rows == 20, files
        assert s.archive_state().lost_rows == 0


@test("ParquetOptions(compression=...): per-column encodings (delta, BSS, dictionary list) reach the file")
def _():
    with tempfile.TemporaryDirectory() as tmp:
        s = sp.create_stream("v2test/encoding", SCHEMA, memory=MemoryOptions(lookback=500), store=tmp, parquet=ParquetOptions(compression={"compression": "zstd", "compression_level": 9,
                          "use_dictionary": ["label"],
                          "column_encoding": {"servo.pos": "DELTA_BINARY_PACKED",
                                              "servo.vel": "DELTA_BINARY_PACKED",
                                              "mark": "DELTA_BINARY_PACKED",
                                              "tv": "BYTE_STREAM_SPLIT"}}))
        fill(s, 300)
        s.cut()
        loc = s.archive_state().location
        wait_for(lambda: any(f.endswith('.parquet') for f in os.listdir(loc)),
                 what="finalized file")
        f = [f for f in os.listdir(loc) if f.endswith('.parquet')][0]
        md = pq.ParquetFile(os.path.join(loc, f)).metadata.row_group(0)
        enc = {md.column(i).path_in_schema: set(md.column(i).encodings)
               for i in range(md.num_columns)}
        assert "DELTA_BINARY_PACKED" in enc["servo.pos"], enc
        assert "BYTE_STREAM_SPLIT" in enc["tv"], enc
        assert "RLE_DICTIONARY" in enc["label"] or "PLAIN_DICTIONARY" in enc["label"], enc
        assert md.column(0).compression == "ZSTD", md.column(0).compression
        t = pq.read_table(os.path.join(loc, f))
        assert t.num_rows == 300 and t.column("label")[299].as_py() == "r299"


@test("the writer's defaults: zstd, delta on integers, BSS on floats, dictionary off; an option overrides")
def _():
    with tempfile.TemporaryDirectory() as tmp:
        def written(name, archive):
            s = sp.create_stream(name, SCHEMA, memory=MemoryOptions(lookback=500),
                                        store=tmp, parquet=archive)
            fill(s, 300)
            s.cut()
            loc = s.archive_state().location
            wait_for(lambda: any(f.endswith('.parquet') for f in os.listdir(loc)),
                     what="finalized file")
            f = [f for f in os.listdir(loc) if f.endswith('.parquet')][0]
            md = pq.ParquetFile(os.path.join(loc, f)).metadata.row_group(0)
            return {md.column(i).path_in_schema:
                    (md.column(i).physical_type, set(md.column(i).encodings),
                     md.column(i).compression) for i in range(md.num_columns)}
        enc = written("v2test/defaults", ParquetOptions())
        for path, (phys, e, comp) in enc.items():
            assert comp == "ZSTD", (path, comp)
            if phys in ("INT32", "INT64"):
                assert "DELTA_BINARY_PACKED" in e, (path, e)
            elif phys in ("FLOAT", "DOUBLE"):
                assert "BYTE_STREAM_SPLIT" in e, (path, e)
            assert "RLE_DICTIONARY" not in e and "PLAIN_DICTIONARY" not in e, (path, e)
        enc = written("v2test/override", ParquetOptions(compression={
            "compression": "snappy", "use_dictionary": ["label", "mark"],
            "column_encoding": {"tv": "PLAIN"}}))
        assert all(c == "SNAPPY" for _, _, c in enc.values()), enc
        assert "RLE_DICTIONARY" in enc["label"][1] or "PLAIN_DICTIONARY" in enc["label"][1], enc
        assert "DELTA_BINARY_PACKED" not in enc["mark"][1], enc        # dictionary instead
        assert "BYTE_STREAM_SPLIT" not in enc["tv"][1], enc            # the option's PLAIN
        assert "DELTA_BINARY_PACKED" in enc["servo.pos"][1], enc       # untouched default


@test("an overheld pin jams the writer, loudly, then release resumes")
def _():
    with tempfile.TemporaryDirectory() as tmp:
        flat = pa.schema([F('n', pa.int64(), nullable=False)])
        s = sp.create_stream("v2test/jam", flat, memory=MemoryOptions(lookback=64), store=tmp, parquet=ParquetOptions(row_group_rows=32, row_group_seconds=0.2, checkpoint_seconds=0.1))
        a = sp.connect("v2test/jam")

        def alloc(i, block_s=0.05):
            row = s.get_mutable_row(block_s=block_s)
            row.n = i

        # fill 60 rows in one commit
        for i in range(60):
            alloc(i)
        s.commit()
        time.sleep(0.3)                 # archived before any recycle

        # a reader's table IS a pin on its rows: held while the writer
        # laps the ring, the writer stalls, loudly, and the stall names
        # the range and the holder
        t = a.arrow_table(rows=60)      # the whole window, held
        assert t.num_rows == 60, t.num_rows
        stalled = False
        for i in range(60, 70):         # seq 64 must recycle pinned seq 0
            try:
                alloc(i)
            except PinStalledError as e:
                assert "a pin on [0, 60)" in str(e) and "arrow_table" in str(e), e
                stalled = True
                break
        assert stalled, "a held table must stall the writer"
        del t                           # dropping the table drops the pin
        import gc
        gc.collect()
        alloc(i, block_s=4.0)           # released -> resumes
        s.commit()
        alloc(999)
        s.commit()
        assert a.newest_row().n == 999
        a.close()


@test("dynamic relation on the Stream face (publisher's own view)")
def _():
    with tempfile.TemporaryDirectory() as tmp:
        flat = pa.schema([F('n', pa.int64(), nullable=False)])
        s = sp.create_stream("v2test/crs", flat, memory=MemoryOptions(lookback=4000), store=tmp, parquet=ParquetOptions(checkpoint_seconds=0.15, row_group_rows=200, row_group_seconds=0.4))
        for w in range(3):
            s.append(pa.record_batch([pa.array(range(w * 150,
                                                           w * 150 + 150))],
                                           schema=flat))
            time.sleep(0.25)
        time.sleep(0.4)
        con = view_con()
        rel = s.duckdb_all(con)
        cnt, dst, mx = con.sql(
            f"SELECT count(*), count(DISTINCT n), max(n) "
            f"FROM {rel.name}").fetchall()[0]
        assert cnt == dst == 450 and mx == 449, (cnt, dst, mx)
        rel.close()


print("\n--- playback ---")

@test("playback stream: clock-gated visibility")
def _():
    now = time.time()
    flat = pa.schema([F('tv', pa.float64(), nullable=False),
                      F('label', pa.utf8(), nullable=False)])
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, 'rec.parquet')
        pq.write_table(pa.table({
            'tv': [now - 10, now - 5, now + 60],
            'label': ['a', 'b', 'future']}, schema=flat), path)
        s = sp.create_playback_stream("v2test/play", path,
                                             timestamp_column='tv')
        a = sp.connect("v2test/play")
        assert a._newest_seq == 1            # the future row is invisible
        assert a._read_row(1).label == 'b'
        try:
            a._read_row(2)
            assert False, "future row should be clamped"
        except StaleReadError:
            pass
        assert a.arrow_table().num_rows == 2
        assert a._seq_at_or_after('tv', now + 30) == -1



print("\n--- the reader verbs ---")

@test("newest_row: None while empty, then the newest row")
def _():
    s = sp.create_stream("v2test/newest", SCHEMA, memory=MemoryOptions(lookback=100))
    a = sp.connect("v2test/newest")
    assert a.newest_row() is None and s.newest_row() is None
    fill(s, 7)
    assert a.newest_row().tv == 6.0 and s.newest_row().label == 'r6'


@test("cursor: the rows since the last pass, lost counted, since= starts in the past")
def _():
    s = sp.create_stream("v2test/cursor", SCHEMA, memory=MemoryOptions(lookback=32))
    fill(s, 10)
    a = sp.connect("v2test/cursor")
    c = a.cursor()                       # starts at the newest row
    assert [r.tv for r in c] == []
    fill(s, 5, start=10)
    assert [r.tv for r in c] == [10.0, 11.0, 12.0, 13.0, 14.0]
    assert [r.tv for r in c] == []       # consumed
    fill(s, 3, start=15)
    t = c.arrow_table(columns=['tv'])    # the same rows as one table
    assert t.column('tv').to_pylist() == [15.0, 16.0, 17.0]
    assert [r.tv for r in c] == []
    del t                                # the table is a pin; drop it before the ring wraps
    # a reader that falls behind the ring: the recycled rows are counted
    fill(s, 60, start=18)                # 60 rows into a 32-row ring
    got = [r.tv for r in c]
    assert c.lost > 0 and got[-1] == 77.0 and got[0] == 78.0 - len(got), (c.lost, got[:3])
    # since: start at the first row at or after a value, as far back as memory holds
    c2 = s.cursor(since=('tv', 70.0))
    assert [r.tv for r in c2][:3] == [70.0, 71.0, 72.0]
    # since a value not yet reached: the cursor begins with the next row
    c3 = s.cursor(since=('tv', 1000.0))
    fill(s, 2, start=1000)
    assert [r.tv for r in c3] == [1000.0, 1001.0]


@test("arrow_table: unarchived by default, rows=N, since=, columns=; the table is the pin")
def _():
    with tempfile.TemporaryDirectory() as tmp:
        s = sp.create_stream("v2test/table", SCHEMA, memory=MemoryOptions(lookback=500),
                                    store=tmp, parquet=ParquetOptions(checkpoint_seconds=1.0))
        fill(s, 40)
        a = sp.connect("v2test/table")
        assert a.arrow_table(rows=5).column('tv').to_pylist() == [35.0, 36.0, 37.0, 38.0, 39.0]
        assert a.arrow_table(since=('tv', 37.5), columns=['tv', 'label']).column_names == ['tv', 'label']
        assert a.arrow_table(since=('tv', 37.5)).column('tv').to_pylist() == [38.0, 39.0]
        assert a.arrow_table(since=('tv', 99.0)).num_rows == 0
        # no selector: the rows not yet on disk; a checkpoint shrinks it
        assert a.arrow_table().num_rows == 40
        wait_for(lambda: s.memory_state().rows_unarchived == 0, what="tail checkpoint")
        assert a.arrow_table().num_rows == 0
        fill(s, 3, start=40)
        assert a.arrow_table().column('tv').to_pylist() == [40.0, 41.0, 42.0]
        assert a.memory_state().rows == 43 and a.memory_state().capacity == 500
        # a reader with no archive of its own: everything it holds
        s2 = sp.create_stream("v2test/table2", SCHEMA, memory=MemoryOptions(lookback=50))
        fill(s2, 12)
        assert sp.connect("v2test/table2").arrow_table().num_rows == 12
        # the table pins ITS rows and no others: held across a near-full
        # ring of writes, the writer runs until it reaches the table's
        # first row (seq 38 of a 500-row ring: at seq 538), not before
        from arrow_ring import PinStalledError
        t = a.arrow_table(rows=5)            # seq 38..42
        fill(s, 495, start=43)               # seq 43..537 recycle 0..37: free
        try:
            s.get_mutable_row(block_s=0.05)  # seq 538 would recycle 38
            assert False, "the held table's rows were recycled"
        except PinStalledError:
            pass
        del t
        fill(s, 1, start=538)
        assert s.newest_row().tv == 538.0


@test("duckdb views: live is memory, archive is the files, all is both, each registered on the connection")
def _():
    with tempfile.TemporaryDirectory() as tmp:
        flat = pa.schema([F('n', pa.int64(), nullable=False)])
        s = sp.create_stream("v2test/views", flat, memory=MemoryOptions(lookback=4000),
                                    store=tmp, parquet=ParquetOptions(row_group_rows=200, row_group_seconds=0.4, checkpoint_seconds=0.15))
        for w in range(3):
            s.append(pa.record_batch([pa.array(range(w * 150, w * 150 + 150))], schema=flat))
            time.sleep(0.25)
        s.cut()
        loc = s.archive_state().location
        wait_for(lambda: any(f.endswith('.parquet') for f in os.listdir(loc)), what="finalized file")
        s.append(pa.record_batch([pa.array(range(450, 480))], schema=flat))
        con = view_con()
        live = s.duckdb_live(con)
        arch = s.duckdb_archive(con)
        both = s.duckdb_all(con)
        assert {live.name, arch.name, both.name} == {'sp_live_v2test_views', 'sp_archive_v2test_views', 'sp_v2test_views'}
        assert con.sql(f"SELECT count(*) FROM {live.name}").fetchall()[0][0] == 480
        n_arch = con.sql(f"SELECT count(*), max(n) FROM {arch.name}").fetchall()[0]
        assert n_arch[0] >= 450 and n_arch[1] >= 449, n_arch       # the files, finalized plus live checkpoint
        wait_for(lambda: con.sql(f"SELECT count(*), count(DISTINCT n), max(n) FROM {both.name}").fetchall()[0]
                 == (480, 480, 479), what="480 rows, each once")
        for v in (live, arch, both):
            v.close()


@test("archive_state().file: the open file's name, None between files")
def _():
    with tempfile.TemporaryDirectory() as tmp:
        s = sp.create_stream("v2test/openfile", SCHEMA, memory=MemoryOptions(lookback=500),
                                    store=tmp, parquet=ParquetOptions(checkpoint_seconds=0.05))
        assert s.archive_state().file is None          # nothing written yet
        fill(s, 5)
        wait_for(lambda: s.archive_state().file is not None, what="first file opened")
        name = s.archive_state().file
        assert name.endswith('.parquet'), name
        s.cut(label="NEXT")
        wait_for(lambda: s.archive_state().file is None, what="file closed by the cut")
        fill(s, 5, start=5)
        wait_for(lambda: (s.archive_state().file or '').endswith('-NEXT.parquet'),
                 what="the labeled file opened")


@test("a file is named by the commit time of its first row, not by the cut or its first write")
def _():
    from streaming_parquet import _clock
    with tempfile.TemporaryDirectory() as tmp:
        s = sp.create_stream("v2test/stamp", SCHEMA, memory=MemoryOptions(lookback=500),
                                    store=tmp, parquet=ParquetOptions(checkpoint_seconds=0.2))
        fill(s, 5)
        s.cut(label="STAMP")            # ends the first file; the next is unnamed until a row
        time.sleep(0.5)                 # an idle gap: the cut time is NOT the next file's time
        frozen = 1_800_000_000.0        # 2027-01-15 08:00:00 UTC
        sp.set_clock(lambda: frozen)
        try:
            fill(s, 1, start=5)         # the first row of the next file, committed at `frozen`
        finally:
            sp.set_clock(time.time)
        time.sleep(0.5)                 # the first write into it comes later still
        fill(s, 4, start=6)
        loc = s.archive_state().location
        wait_for(lambda: any('STAMP' in f for f in os.listdir(loc)), what="labeled file opened")
        f = [f for f in os.listdir(loc) if 'STAMP' in f][0]
        from datetime import datetime
        assert f.startswith(datetime.fromtimestamp(frozen).strftime("%Y%m%d-%H%M%S")), f
        s.close()                        # before the directory goes


@test("a reader that attaches later gets rows from then on: its first request starts at the newest row")
def _():
    s = sp.create_stream("v2test/nosub", SCHEMA, memory=MemoryOptions(lookback=500))
    fill(s, 50)                          # nobody attached; nothing is sent to anyone
    a = sp.connect(ticket=s._ticket, memory=MemoryOptions(lookback=500))
    wait_for(lambda: a.state == "streaming", what="feeder connect")
    fill(s, 10, start=50)
    wait_for(lambda: a._newest_seq == 59, what="rows after attach")
    assert a._oldest_seq == 50, a._oldest_seq
    a.close()


@test("the pull wire: one answer per request carries everything since the seq asked; a quiet stream answers empty with the revision")
def _():
    import struct as _s
    import pyarrow.flight as flight
    s = sp.create_stream("v2test/pull", SCHEMA, memory=MemoryOptions(lookback=500))
    for i in range(100):                 # one hundred commits
        fill(s, 1, start=i)
    t = s._ticket
    client = flight.FlightClient(f"grpc://{t['host']}:{t['port']}")
    info = client.get_flight_info(flight.FlightDescriptor.for_path("v2test/pull"))
    assert info.total_records == 100     # a new reader starts here
    # one request from 0: one answer with all 100 rows, side data (seq, revision)
    chunks = list(client.do_get(flight.Ticket(b"v2test/pull@0")))
    assert len(chunks) == 1 and chunks[0].data.num_rows == 100
    seq, rev = _s.unpack('<qq', chunks[0].app_metadata)
    assert seq == 0 and rev == s._ticket_revision
    # a column subset
    chunks = list(client.do_get(flight.Ticket(b"v2test/pull@90:tv,label")))
    assert chunks[0].data.column_names == ['tv', 'label'] and chunks[0].data.num_rows == 10
    # nothing new: the answer waits up to a second, then comes back empty with the revision
    t0 = time.monotonic()
    chunks = list(client.do_get(flight.Ticket(b"v2test/pull@100")))
    assert 0.8 < time.monotonic() - t0 < 2.0
    assert len(chunks) == 1 and chunks[0].data.num_rows == 0
    assert _s.unpack('<qq', chunks[0].app_metadata) == (-1, s._ticket_revision)
    # a row committed while a request waits answers it at once
    def later():
        time.sleep(0.2)
        fill(s, 1, start=100)
    import threading as _th
    _th.Thread(target=later).start()
    t0 = time.monotonic()
    chunks = list(client.do_get(flight.Ticket(b"v2test/pull@100")))
    assert time.monotonic() - t0 < 0.8 and chunks[0].data.num_rows == 1
    # the old ticket form is refused
    try:
        list(client.do_get(flight.Ticket(b"v2test/pull")))
        assert False, "a ticket without @seq must be refused"
    except flight.FlightError:
        pass
    client.close()
    # the feeder on the same wire: at rest, one row commits and arrives within a round trip
    a = sp.connect(ticket=s._ticket, memory=MemoryOptions(lookback=500))
    wait_for(lambda: a.state == "streaming", what="feeder connect")
    t0 = time.monotonic()
    fill(s, 1, start=101)
    wait_for(lambda: a._newest_seq == 101, what="the row after one commit")
    assert time.monotonic() - t0 < 0.5
    a.close()

print("\n--- flight loopback (remote attach) ---")

@test("store discovery: create -> ticket in the store, list, connect by name, close deletes")
def _():
    import tempfile
    with tempfile.TemporaryDirectory() as root:
        s = sp.create_stream("v2test/disc", SCHEMA, memory=MemoryOptions(lookback=500), store=root)
        wait_for(lambda: sp.list_streams(store=root) == ["v2test/disc"],
                 what="ticket lands in the store")
        # by-name connect from "another process": defeat the local fast
        # path by connecting through the fully-qualified URI (the store
        # rides in the name; the feeder discovers the ticket)
        a = sp.connect(f"file://{root}/streams/v2test/disc",
                              memory=MemoryOptions(lookback=500))
        wait_for(lambda: a.state == "streaming",
                 what="feeder connect via store discovery")
        assert a.standing() == [], a.standing()
        fill(s, 25)
        wait_for(lambda: a._newest_seq == 24, what="rows over Flight")
        assert a._read_row(24).tv == 24.0
        a.close()
        s.close()
        wait_for(lambda: sp.list_streams(store=root) == [],
                 what="close deletes the ticket")


@test("a store is a whole bucket: a prefix inside one is refused")
def _():
    try:
        sp.create_stream("v2test/prefix", SCHEMA, store="s3://bucket/some/prefix")
        assert False, "a prefix must be refused"
    except ValueError as e:
        assert "whole bucket" in str(e), e
    try:
        sp.connect("v2test/prefix", store="s3://bucket/some/prefix")
        assert False, "a prefix must be refused"
    except ValueError as e:
        assert "whole bucket" in str(e), e


@test("the archive views need no connection: a dead publisher's files still answer, and standing() says why")
def _():
    import json as _json
    import tempfile
    flat = pa.schema([F('n', pa.int64(), nullable=False)])
    with tempfile.TemporaryDirectory() as root:
        # a publisher wrote two files and died, leaving a stale ticket
        s = sp.create_stream("v2test/dead", flat, memory=MemoryOptions(lookback=500), store=root,
                                    parquet=ParquetOptions(checkpoint_seconds=0.05))
        s.append(pa.record_batch([pa.array(range(100))], schema=flat))
        s.cut()
        s.append(pa.record_batch([pa.array(range(100, 150))], schema=flat))
        s.close()
        tpath = os.path.join(root, "tickets", "v2test", "dead.json")
        os.makedirs(os.path.dirname(tpath), exist_ok=True)
        with open(tpath, "w") as f:
            _json.dump({"name": "v2test/dead", "host": "127.0.0.1", "port": 1,
                        "ts": time.time() - 3600, "run": "x"}, f)
        a = sp.connect("v2test/dead", store=root, memory=MemoryOptions(lookback=500))
        wait_for(lambda: a.state == "waiting", what="stale ticket seen")
        assert a.standing() and "stale" in a.standing()[0], a.standing()
        con = view_con()
        arch = a.duckdb_archive(con)
        assert con.sql(f"SELECT count(*), max(n) FROM {arch.name}").fetchall()[0] == (150, 149)
        both = a.duckdb_all(con)                       # the memory piece is empty; the files answer
        assert con.sql(f"SELECT count(*), max(n) FROM {both.name}").fetchall()[0] == (150, 149)
        arch.close(); both.close(); a.close()


@test("root discovery: dead publisher's stale ticket -> feeder waits, "
      "never connects to a corpse")
def _():
    import json as _json
    import os as _os
    import tempfile
    with tempfile.TemporaryDirectory() as root:
        path = _os.path.join(root, "tickets", "v2test", "stale.json")
        _os.makedirs(_os.path.dirname(path))
        with open(path, "w") as f:
            _json.dump({"name": "v2test/stale", "host": "127.0.0.1",
                        "port": 1, "ts": time.time() - 3600}, f)
        a = sp.connect("v2test/stale", memory=MemoryOptions(lookback=500), store=root)
        wait_for(lambda: a.state == "waiting", what="stale ticket seen")
        a.close()


@test("publisher respawn: a new run -> feeder resyncs on a fresh ring")
def _():
    import tempfile
    with tempfile.TemporaryDirectory() as root:
        s = sp.create_stream("v2test/epoch", SCHEMA, memory=MemoryOptions(lookback=500), store=root)
        # through the URI so the feeder path is exercised, not the local face
        a = sp.connect(f"file://{root}/streams/v2test/epoch", memory=MemoryOptions(lookback=500))
        wait_for(lambda: a.state == "streaming", what="first epoch connect")
        fill(s, 20)
        wait_for(lambda: a._newest_seq == 19, what="epoch-1 rows")
        s.close()   # deletes the ticket AND unregisters the name
        # respawn: same name, new epoch, seq axis restarts at 0 — the
        # feeder must resync (fresh ring), not livelock on regression
        s2 = sp.create_stream("v2test/epoch", SCHEMA, memory=MemoryOptions(lookback=500), store=root)
        # a real publisher keeps publishing through the reconnect; feed
        # a row at a time until an epoch-2 row (tv >= 100, offset makes
        # them distinguishable from epoch-1's) lands in the fresh ring
        def epoch2_visible():
            try:
                seq = a._newest_seq
                return seq >= 0 and a._read_row(seq).tv >= 100.0
            except StaleReadError:
                return False
        n = 0
        deadline = time.monotonic() + 25.0
        while time.monotonic() < deadline and not epoch2_visible():
            fill(s2, 1, start=100 + n)   # epoch-2 row n carries tv=100+n
            n += 1
            time.sleep(0.2)
        assert epoch2_visible(), "no epoch-2 row ever reached the feeder"
        seq = a._newest_seq
        assert a._read_row(seq).tv == 100.0 + seq  # NEW axis, from seq 0
        a.close()
        s2.close()


@test("every file records the run that wrote it (sp_run, beside "
      "sp_base_seq); a respawned writer's run is later")
def _():
    import re
    import tempfile
    import pyarrow.parquet as pq
    flat = pa.schema([("n", pa.int64())])
    with tempfile.TemporaryDirectory() as root:
        def write_run(start):
            s = sp.create_stream("v2test/run", flat,
                                        memory=MemoryOptions(lookback=500), store=root,
                                        parquet=ParquetOptions(checkpoint_seconds=0.05))
            run = s._ticket["run"]
            s.append(pa.record_batch([pa.array(range(start, start + 10))],
                                     schema=flat))
            s.cut()
            s.append(pa.record_batch([pa.array(range(start + 10, start + 15))],
                                     schema=flat))
            s.close()
            return run
        run1 = write_run(0)
        run2 = write_run(100)
        shape = r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z"
        assert re.fullmatch(shape, run1) and re.fullmatch(shape, run2), (run1, run2)
        assert run2 > run1, (run1, run2)
        d = os.path.join(root, "streams", "v2test", "run")
        seen = {}
        # all finalized: a checkpoint left is a finished file's, kept for readers
        assert all(parquet_checkpoint.parse_ckpt_object(f)[0] in os.listdir(d)
                   for f in os.listdir(d) if f.endswith(".ckpt")), os.listdir(d)
        for f in sorted(f for f in os.listdir(d) if f.endswith(".parquet")):
            kv = pq.read_metadata(os.path.join(d, f)).metadata
            first = pq.read_table(os.path.join(d, f)).column("n")[0].as_py()
            seen[first] = (kv[b"sp_run"].decode(), int(kv[b"sp_base_seq"]))
        # run 1 wrote rows 0.. then 10.. ; run 2 rows 100.. then 110..;
        # each run's row numbers start over at 0
        assert seen == {0: (run1, 0), 10: (run1, 10),
                        100: (run2, 0), 110: (run2, 10)}, seen


@test("remote attach by ticket: publish -> Flight -> local ring")
def _():
    s = sp.create_stream("v2test/flight", SCHEMA, memory=MemoryOptions(lookback=500))
    a = sp.connect(ticket=s._ticket, memory=MemoryOptions(lookback=500))
    wait_for(lambda: a.state == "streaming", what="feeder connect")
    fill(s, 30)
    wait_for(lambda: a._newest_seq == 29, what="rows over Flight")
    r = a._read_row(29)
    assert r.tv == 29.0 and r.label == 'r29' and r.servo.pos == 290
    assert a._read_row(3).mark is None or a._read_row(2).mark == 2
    # second wave
    fill(s, 30, start=30)
    wait_for(lambda: a._newest_seq == 59, what="second wave")
    with all_rows(a._ring) as rs:
        assert rs.table.num_rows == 60


@test("a reader that stops asking holds nothing: the writer laps the ring twice; the reader's next request starts at the oldest row left (2026-11-07)")
def _():
    # Readers pull. A reader that stops, or whose network path died,
    # leaves nothing at the publisher: no queue, no sender, no pin.
    # (The push protocol this replaced held a pin per batch in transit;
    # one stuck 2.6 s stopped the writer and ended brother-connector.)
    import pyarrow.flight as flight
    s = sp.create_stream("v2test/stuck", SCHEMA, memory=MemoryOptions(lookback=2_000))
    fill(s, 100)
    t = s._ticket
    client = flight.FlightClient(f"grpc://{t['host']}:{t['port']}")
    first = list(client.do_get(flight.Ticket(b"v2test/stuck@0")))[0].data
    assert first.num_rows == 100
    del first                               # the reader is now "stuck": it asks for nothing more
    fill(s, 4_000, start=100)               # two whole rings; the writer never waits
    assert s.memory_state().rows == 2_000 and s.newest_row().tv == 4_099.0
    # when it asks again from where it was, it gets the oldest row left and sees the gap
    chunks = list(client.do_get(flight.Ticket(b"v2test/stuck@100")))
    import struct as _s
    seq, _rev = _s.unpack('<qq', chunks[0].app_metadata)
    assert seq == 4_100 - 2_000, seq        # the oldest row left: seq 2100
    # one answer; two Arrow batches, because the ring wrap cuts the run
    assert sum(c.data.num_rows for c in chunks) == 2_000
    client.close()


@test("wire seq alignment: mid-stream attach lands on publisher's axis")
def _():
    s = sp.create_stream("v2test/seqax", SCHEMA, memory=MemoryOptions(lookback=500))
    fill(s, 40)                      # published BEFORE anyone attaches
    a = sp.connect(ticket=s._ticket, memory=MemoryOptions(lookback=500))
    wait_for(lambda: a.state == "streaming", what="feeder connect")
    fill(s, 20, start=40)
    wait_for(lambda: a._newest_seq == 59, what="post-attach rows")
    # the local ring is on the PUBLISHER's seq axis: the first received
    # row sits at seq 40, not 0, and every row's payload matches its seq
    assert a._oldest_seq == 40, a._oldest_seq
    for seq in (40, 50, 59):
        assert a._read_row(seq).tv == float(seq)
    with all_rows(a._ring) as rs:
        assert rs.lo == 40 and rs.hi == 60


@test("remote attach with a column subset")
def _():
    flat = pa.schema([F('tv', pa.float64(), nullable=False),
                      F('big', pa.utf8(), nullable=False)])
    s = sp.create_stream("v2test/cols", flat, memory=MemoryOptions(lookback=500))
    a = sp.connect(ticket=s._ticket, memory=MemoryOptions(lookback=500), columns=['tv'])
    wait_for(lambda: a.state == "streaming", what="feeder connect")
    for i in range(10):
        s.append({'tv': float(i), 'big': 'x' * 100})
    wait_for(lambda: a._newest_seq == 9, what="subset rows")
    assert a.schema.names == ['tv']
    assert a._read_row(9).tv == 9.0


print("\n--- the S3 mirror (FakeS3 remote) ---")


def pump(stream, cond, timeout=8.0, what="condition"):
    """Wait for the mirror: its passes ride the stream's own heartbeat
    thread now, nothing to call."""
    wait_for(cond, timeout=timeout, what=what)


def _local_names(pq_dir, label):
    if not os.path.isdir(pq_dir):
        return []
    return [f for f in os.listdir(pq_dir) if label in f]


def _remote_ckpt_rows(remote, label):
    import parquet_checkpoint
    newest = _newest_ckpt([n for n in remote.list('') if label in n])
    if newest is None:
        return None
    return parquet_checkpoint.CheckpointReader(newest, remote).num_rows


@test("sink: the first copy on S3 (FakeS3), no local directory at all")
def _():
    import parquet_checkpoint
    from parquet_checkpoint import FakeS3
    with tempfile.TemporaryDirectory() as tmp:
        only = FakeS3(part_min=4096)
        s = sp.create_stream("v2test/only", SCHEMA, memory=MemoryOptions(lookback=500), store=only, spool=None, parquet=ParquetOptions(checkpoint_seconds=0.05))
        assert s.archive_state() == ArchiveState(failing=False, error=None, lost_rows=0,
                                                 location='fake-s3://streams/v2test/only/',
                                                 file=None)
        s.start_parquet("ONLY")
        fill(s, 25)
        time.sleep(0.1)
        wait_for(lambda: _remote_ckpt_rows(only, 'ONLY') == 25,
                 what="checkpoint on the sink")
        assert not os.path.exists(os.path.join(tmp, "streams")), "no local copy"
        s.stop_parquet()
        wait_for(lambda: any(n.endswith('.parquet') and 'ONLY' in n for n in only.list(''))
                 and not [n for n in only.list('') if '.ckpt' in n],
                 what="finalized on the sink")
        final = [n for n in only.list('') if 'ONLY' in n][0]
        assert pq.read_table(pa.BufferReader(only.get(final))).num_rows == 25


@test("archive state: a failing first copy is said once, read by the host, and recovers")
def _():
    from parquet_checkpoint import FakeS3
    with tempfile.TemporaryDirectory() as tmp:
        only = FakeS3(part_min=4096)
        s = sp.create_stream("v2test/arch", SCHEMA, memory=MemoryOptions(lookback=500), store=only, spool=None, parquet=ParquetOptions(checkpoint_seconds=0.05))
        s.start_parquet("ARCH")
        only.fail_next('put:streams/', n=3)   # the files' writes, not the ticket's
        fill(s, 10)
        time.sleep(0.1)
        wait_for(lambda: s.archive_state().failing, what="archive failing")
        assert 'injected' in s.archive_state().error
        only.clear_faults()
        fill(s, 10, start=10)
        time.sleep(0.1)
        wait_for(lambda: not s.archive_state().failing, what="archive recovered")
        assert s.archive_state().error is None


@test("archive state: rows recycled before their flush count as lost, forever")
def _():
    with tempfile.TemporaryDirectory() as tmp:
        s = sp.create_stream("v2test/lost", SCHEMA, memory=MemoryOptions(lookback=64), store=tmp, parquet=ParquetOptions(row_group_rows=32))
        s.start_parquet("LOST")
        # 200 rows in ONE commit into a 64-row ring: the flush pins the
        # pending region at the commit, and by then rows 0..135 are
        # recycled.
        fill(s, 200)
        st = s.archive_state()
        assert st.lost_rows == 136, st
        lost = st.lost_rows
        fill(s, 5, start=200)
        assert s.archive_state().lost_rows == lost, "never resets"
        s.close()                        # before the directory goes


@test("remote mirror: live ships; outage rides; file ended during it assembles after, byte-identical")
def _():
    import parquet_checkpoint
    from parquet_checkpoint import FakeS3
    with tempfile.TemporaryDirectory() as tmp:
        remote = FakeS3(part_min=4096)
        s = sp.create_stream("v2test/rem", SCHEMA, memory=MemoryOptions(lookback=500), store=remote, spool=tmp, parquet=ParquetOptions(checkpoint_seconds=0.05, ship_seconds=0.02))
        pq_dir = s.archive_state().location
        s.start_parquet("REM")
        fill(s, 30)
        time.sleep(0.1)
        fill(s, 12, start=30)
        # live: the remote checkpoint catches up to the local one
        pump(s, lambda: _remote_ckpt_rows(remote, 'REM') == 42,
             what="remote checkpoint at 42 rows")
        # WAN outage: every remote PUT fails; local keeps checkpointing;
        # the session ENDS during the outage
        remote.fail_next('put:', n=10 ** 6)
        fill(s, 8, start=42)
        time.sleep(0.1)
        s.stop_parquet()                 # final checkpoint locally; pending
        pump(s, lambda: any(f.endswith('.ckpt') for f in _local_names(pq_dir, 'REM'))
             and parquet_checkpoint.live_table(os.path.join(
                 pq_dir, parquet_checkpoint.parse_ckpt_object(
                     _newest_ckpt(_local_names(pq_dir, 'REM')))[0])).num_rows == 50,
             what="local final checkpoint at 50 rows")
        for _ in range(10):              # the outage holds: nothing assembles
            time.sleep(0.02)
        assert not any(f.endswith('.parquet') for f in _local_names(pq_dir, 'REM')), \
            "local must not assemble before its checkpoint is on the remote"
        assert _remote_ckpt_rows(remote, 'REM') == 42
        assert s._parquet.get_in_flight()
        # link back; the remote assembly fails ONCE more (retry path)
        remote.clear_faults()
        remote.fail_next('assemble:', n=1)
        pump(s, lambda: any(f.endswith('.parquet') for f in _local_names(pq_dir, 'REM'))
             and not any(f.endswith('.ckpt') for f in _local_names(pq_dir, 'REM'))
             and any(n.endswith('.parquet') and 'REM' in n for n in remote.list('')),
             what="both copies assembled")
        pump(s, lambda: not s._parquet.get_in_flight(), what="nothing in flight")
        final = [f for f in _local_names(pq_dir, 'REM') if f.endswith('.parquet')][0]
        local_bytes = open(os.path.join(pq_dir, final), 'rb').read()
        key = "streams/v2test/rem/" + final
        assert remote.get(key) == local_bytes
        assert pq.read_table(pa.BufferReader(remote.get(key))).num_rows == 50
        assert not [n for n in remote.list('') if '.ckpt' in n or '.chunks/' in n]


@test("remote mirror: S3 unreachable at start delays the mirror, never the stream")
def _():
    from parquet_checkpoint import FakeS3

    class Dark(FakeS3):                 # no credentials / no link: every call raises
        down = True

        def list(self, prefix):
            if self.down:
                raise RuntimeError("Unable to locate credentials")
            return super().list(prefix)

        def put(self, name, data):
            if self.down:
                raise RuntimeError("Unable to locate credentials")
            return super().put(name, data)

    with tempfile.TemporaryDirectory() as tmp:
        remote = Dark(part_min=4096)
        s = sp.create_stream("v2test/dark", SCHEMA, memory=MemoryOptions(lookback=500), store=remote, spool=tmp, parquet=ParquetOptions(checkpoint_seconds=0.05, ship_seconds=0.02))
        s.start_parquet("DARK")
        fill(s, 30)
        time.sleep(0.1)
        pq_dir = s.archive_state().location
        wait_for(lambda: any(f.endswith('.ckpt') for f in _local_names(pq_dir, 'DARK')),
                 what="local checkpoint with the remote dark")
        for _ in range(10):              # the mirror keeps failing, quietly after the first
            time.sleep(0.02)
        assert not [n for n in FakeS3.list(remote, '') if 'DARK' in n], \
            "nothing may reach a dark remote"
        m = s.mirror_state()
        assert m.failing and 'credentials' in m.error, m
        remote.down = False              # link up: the sweep, then the catch-up
        pump(s, lambda: _remote_ckpt_rows(remote, 'DARK') == 30,
             what="remote checkpoint after the link returned")
        pump(s, lambda: not s.mirror_state().failing, what="mirror recovered")
        assert s.mirror_state() == MirrorState(failing=False, error=None, pending=[])


@test("remote mirror: a checkpoint left on the remote by a crash between the assemblies is finished by the next start")
def _():
    import parquet_checkpoint
    from parquet_checkpoint import FakeS3
    with tempfile.TemporaryDirectory() as tmp:
        remote = FakeS3(part_min=4096)
        pq_dir = os.path.join(tmp, "streams", "v2test/half")
        s = sp.create_stream("v2test/half", SCHEMA, memory=MemoryOptions(lookback=500), store=remote, spool=tmp, parquet=ParquetOptions(checkpoint_seconds=0.05, ship_seconds=0.02))
        s.start_parquet("HALF")
        fill(s, 20)
        time.sleep(0.1)
        pump(s, lambda: _remote_ckpt_rows(remote, 'HALF') == 20,
             what="remote checkpoint")
        remote.fail_next('assemble:', n=10 ** 6)   # every remote assembly fails
        s.stop_parquet()
        pump(s, lambda: any(f.endswith('.parquet') for f in _local_names(pq_dir, 'HALF')),
             what="local assembled, remote not")
        sp.close()                # the closing pass fails the same way
        assert [n for n in remote.list('') if n.endswith('.ckpt') and 'HALF' in n]
        remote.clear_faults()
        s2 = sp.create_stream("v2test/half", SCHEMA, memory=MemoryOptions(lookback=500), store=remote, spool=tmp, parquet=ParquetOptions(ship_seconds=0.02))
        s2.stop_parquet()                # nothing local in flight: the sweep alone
        pump(s2, lambda: not s2._parquet.get_in_flight()
             and any(n.endswith('.parquet') and 'HALF' in n for n in remote.list(''))
             and not [n for n in remote.list('') if '.ckpt' in n or '.chunks/' in n],
             what="remote finished by the next start")
        final = [f for f in _local_names(pq_dir, 'HALF') if f.endswith('.parquet')][0]
        assert remote.get("streams/v2test/half/" + final) == \
            open(os.path.join(pq_dir, final), 'rb').read()


@test("remote mirror: a stop during an outage leaves the file pending; the next start finishes it")
def _():
    import parquet_checkpoint
    from parquet_checkpoint import FakeS3
    with tempfile.TemporaryDirectory() as tmp:
        remote = FakeS3(part_min=4096)
        pq_dir = os.path.join(tmp, "streams", "v2test/boot")
        s = sp.create_stream("v2test/boot", SCHEMA, memory=MemoryOptions(lookback=500), store=remote, spool=tmp, parquet=ParquetOptions(checkpoint_seconds=0.05, ship_seconds=0.02))
        s.start_parquet("BOOT")
        fill(s, 20)
        time.sleep(0.1)
        fill(s, 5, start=20)
        pump(s, lambda: _remote_ckpt_rows(remote, 'BOOT') == 25,
             what="remote checkpoint at 25 rows")
        remote.fail_next('put:', n=10 ** 6)
        fill(s, 15, start=25)
        time.sleep(0.1)
        sp.close()                # clean stop, link down
        names = _local_names(pq_dir, 'BOOT')
        assert any(f.endswith('.ckpt') for f in names) and \
            not any(f.endswith('.parquet') for f in names), names
        assert _remote_ckpt_rows(remote, 'BOOT') == 25
        # next start, link up: the boot sweep registers the pending
        # file and the first mirror passes finish it
        remote.clear_faults()
        # (the checkpoint is seconds old: the owner finishes it anyway)
        s2 = sp.create_stream("v2test/boot", SCHEMA, memory=MemoryOptions(lookback=500), store=remote, spool=tmp, parquet=ParquetOptions(ship_seconds=0.02))
        assert s2._parquet.get_in_flight(), "boot sweep must register the pending file"
        s2.stop_parquet()                # recording off: the heartbeat alone drives the mirror
        pump(s2, lambda: not s2._parquet.get_in_flight(), what="pending file finished")
        final = [f for f in _local_names(pq_dir, 'BOOT') if f.endswith('.parquet')][0]
        local_bytes = open(os.path.join(pq_dir, final), 'rb').read()
        assert remote.get("streams/v2test/boot/" + final) == local_bytes
        assert pq.read_table(pa.BufferReader(local_bytes)).num_rows == 40
        assert not [n for n in remote.list('') if '.ckpt' in n or '.chunks/' in n]


_OWNER_CHILD = r"""
import os, sys, time
import pyarrow as pa
import streaming_parquet as sp
root, mode = sys.argv[1], sys.argv[2]
flat = pa.schema([("n", pa.int64())])
s = sp.create_stream("v2test/own", flat, store=root,
                     memory=sp.MemoryOptions(lookback=500),
                     parquet=sp.ParquetOptions(checkpoint_seconds=0.05))
s.append(pa.record_batch([pa.array(range(40))], schema=flat))
d = os.path.join(root, "streams", "v2test", "own")
while not any(f.endswith(".ckpt") for f in os.listdir(d)):
    time.sleep(0.02)
time.sleep(0.3)                          # the checkpoint covers all 40 rows
print("READY", os.getpid(), flush=True)
if mode == "crash":
    os.kill(os.getpid(), 9)              # no close: the checkpoint is left behind
sys.stdin.read()                         # hold the stream until the test lets go
s.close()
"""


def _owner_child(root, mode):
    import subprocess
    env = dict(os.environ)
    env["PYTHONPATH"] = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    p = subprocess.Popen([sys.executable, "-c", _OWNER_CHILD, root, mode],
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         stderr=subprocess.DEVNULL, env=env, text=True)
    line = p.stdout.readline().split()
    assert line and line[0] == "READY", f"child did not start: {line}"
    return p, int(line[1])


@test("ownership: a second process on this server is refused, naming the "
      "holder's pid; the stream is free once the holder closes")
def _():
    import tempfile
    flat = pa.schema([("n", pa.int64())])
    with tempfile.TemporaryDirectory() as root:
        child, pid = _owner_child(root, "hold")
        try:
            try:
                sp.create_stream("v2test/own", flat, store=root,
                                        memory=MemoryOptions(lookback=500))
                raise AssertionError("second writer was not refused")
            except RuntimeError as e:
                assert "another process on this server" in str(e), e
                assert f"pid {pid}" in str(e), e
        finally:
            child.stdin.close()
            child.wait(timeout=30)
        s = sp.create_stream("v2test/own", flat, store=root,
                                    memory=MemoryOptions(lookback=500))
        s.close()


@test("ownership: a writer killed mid-file leaves a checkpoint; the next "
      "writer owns the stream at once and finishes it, however recent")
def _():
    import tempfile
    flat = pa.schema([("n", pa.int64())])
    with tempfile.TemporaryDirectory() as root:
        child, _pid = _owner_child(root, "crash")
        child.wait(timeout=30)
        assert child.returncode == -9, child.returncode
        d = os.path.join(root, "streams", "v2test", "own")
        left = [f for f in os.listdir(d) if f.endswith(".ckpt")]
        assert len(left) == 1, os.listdir(d)
        s = sp.create_stream("v2test/own", flat, store=root,
                                    memory=MemoryOptions(lookback=500),
                                    parquet=ParquetOptions(checkpoint_seconds=0.05))
        try:
            assert not [f for f in os.listdir(d) if f.endswith(".ckpt")], os.listdir(d)
            final = os.path.join(
                d, parquet_checkpoint.parse_ckpt_object(left[0])[0])
            t = pq.read_table(final)
            assert t.column("n").to_pylist() == list(range(40)), t.num_rows
        finally:
            s.close()


@test("ownership: a fresh ticket from another server refuses, naming it; "
      "a stale one is taken over")
def _():
    import json as _json
    import tempfile
    flat = pa.schema([("n", pa.int64())])
    with tempfile.TemporaryDirectory() as root:
        tpath = os.path.join(root, "tickets", "v2test", "other.json")
        os.makedirs(os.path.dirname(tpath))
        def ticket(age_s):
            with open(tpath, "w") as f:
                _json.dump({"name": "v2test/other", "host": "10.9.9.9",
                            "port": 4242, "ts": time.time() - age_s,
                            "run": "2026-10-26T00:00:00.000000Z"}, f)
        ticket(5)
        try:
            sp.create_stream("v2test/other", flat, store=root,
                                    memory=MemoryOptions(lookback=500))
            raise AssertionError("a fresh ticket from another server was not refused")
        except RuntimeError as e:
            assert "the server at 10.9.9.9:4242 writes this stream" in str(e), e
        ticket(3600)
        s = sp.create_stream("v2test/other", flat, store=root,
                                    memory=MemoryOptions(lookback=500))
        s.close()


@test("ticket: written as the store's data changes -- stored row, the "
      "unfinished file as its newest checkpoint version's path, the beat "
      "and stale_after "
      "(directory store)")
def _():
    import json as _json
    import tempfile
    flat = pa.schema([("n", pa.int64())])
    with tempfile.TemporaryDirectory() as root:
        tpath = os.path.join(root, "tickets", "v2test", "tk.json")
        def ticket():
            try:
                with open(tpath) as f:
                    return _json.load(f)
            except (FileNotFoundError, ValueError):
                return None
        s = sp.create_stream("v2test/tk", flat, store=root,
                                    memory=MemoryOptions(lookback=500),
                                    parquet=ParquetOptions(checkpoint_seconds=0.05))
        try:
            s.append(pa.record_batch([pa.array(range(30))], schema=flat))
            wait_for(lambda: (ticket() or {}).get("stored_row") == 29,
                     what="ticket stored_row 29")
            t = ticket()
            assert t["run"] == s._run and t["beat_seconds"] == 0.05, t
            assert t["stale_after"] == 30.0, t
            assert t["checkpoint_seconds"] == 0.05 and t["ship_seconds"] is None, t
            assert isinstance(t["stored_at"], float), t
            d = os.path.join(root, "streams", "v2test", "tk")
            assert len(t["unfinished"]) == 1, t
            u = t["unfinished"][0]
            assert u.startswith(d + "/") and u.endswith(".ckpt"), u
            assert parquet_checkpoint.parse_ckpt_object(os.path.basename(u)), u
            assert os.path.exists(u), u
            s.cut()
            wait_for(lambda: (ticket() or {}).get("unfinished") == [],
                     what="the cut file finished: nothing unfinished")
            s.append(pa.record_batch([pa.array(range(30, 35))], schema=flat))
            wait_for(lambda: (ticket() or {}).get("stored_row") == 34 and
                     len(ticket()["unfinished"]) == 1, what="next file in the ticket")
            assert ticket()["unfinished"] != [u], "a new file, a new name"
        finally:
            s.close()
        assert ticket() is None, "close deletes the ticket"


@test("ticket: a bucket behind a spool lists what the bucket holds -- "
      "rows shipped, the remote checkpoint's path, the ship interval as the beat")
def _():
    import tempfile
    from parquet_checkpoint import FakeS3
    flat = pa.schema([("n", pa.int64())])
    with tempfile.TemporaryDirectory() as spool:
        remote = FakeS3(part_min=4096)
        s = sp.create_stream("v2test/tkr", flat, store=remote, spool=spool,
                                    memory=MemoryOptions(lookback=500),
                                    parquet=ParquetOptions(checkpoint_seconds=0.05,
                                                    ship_seconds=0.05))
        try:
            s.append(pa.record_batch([pa.array(range(20))], schema=flat))
            def t():
                return sp._tickets.get_ticket(remote, "v2test/tkr") or {}
            wait_for(lambda: t().get("stored_row") == 19, what="shipped rows in the ticket")
            tk = t()
            assert tk["beat_seconds"] == 0.05 and tk["ship_seconds"] == 0.05, tk
            assert len(tk["unfinished"]) == 1, tk
            u = tk["unfinished"][0]
            want = remote.uri + "streams/v2test/tkr/"
            assert u.startswith(want) and u.endswith(".ckpt"), (u, want)
            key = u[len(remote.uri):]
            assert remote.exists(key), key
        finally:
            s.close()


@test("ticket: a reader calls a writer dead after the ticket's own "
      "stale_after; a ticket without one after the rule for a writer "
      "with no beat, 30 s")
def _():
    import json as _json
    import tempfile
    with tempfile.TemporaryDirectory() as root:
        path = os.path.join(root, "tickets", "v2test", "st.json")
        os.makedirs(os.path.dirname(path))
        with open(path, "w") as f:
            _json.dump({"name": "v2test/st", "host": "127.0.0.1", "port": 1,
                        "ts": time.time() - 3, "stale_after": 1.0}, f)
        a = sp.connect("v2test/st", memory=MemoryOptions(lookback=500), store=root)
        wait_for(lambda: a.state == "waiting", what="stale by its own stale_after")
        assert "stale after 1 s" in a.standing()[0], a.standing()
        a.close()
        with open(path, "w") as f:
            _json.dump({"name": "v2test/st", "host": "127.0.0.1", "port": 1,
                        "ts": time.time() - 20}, f)
        a = sp.connect("v2test/st", memory=MemoryOptions(lookback=500), store=root)
        wait_for(lambda: a.state in ("connecting", "reconnecting"),
                 what="a 20 s old ticket without stale_after is live")
        a.close()
        with open(path, "w") as f:
            _json.dump({"name": "v2test/st", "host": "127.0.0.1", "port": 1,
                        "ts": time.time() - 40}, f)
        a = sp.connect("v2test/st", memory=MemoryOptions(lookback=500), store=root)
        wait_for(lambda: a.state == "waiting",
                 what="a 40 s old ticket without stale_after is stale")
        assert "stale after 30 s" in a.standing()[0], a.standing()
        a.close()
        # the one rule: three beats, at least 30 seconds
        assert sp._tickets.stale_after({}) == parquet_checkpoint.dead_after() == 30.0
        assert parquet_checkpoint.dead_after(5) == 30.0
        assert parquet_checkpoint.dead_after(60) == 180.0


def _catalog_rows(folder, sql="SELECT count(*), min(n), max(n) FROM b.data"):
    c = duckdb.connect()
    c.execute(f"ATTACH 'ducklake:{os.path.join(folder, 'catalog.ducklake')}' "
              f"AS b (READ_ONLY)")
    try:
        return c.execute(sql).fetchall()
    finally:
        c.close()


def _catalog_writer_run(root, schema, batches, name="v2test/cat"):
    s = sp.create_stream(name, schema, store=root,
                                memory=MemoryOptions(lookback=500),
                                parquet=ParquetOptions(checkpoint_seconds=0.05))
    for b in batches:
        s.append(pa.record_batch(b, schema=schema))
        s.cut()
    s.close()
    return s


@test("catalog: every finished file is registered and published beside the "
      "files; the ticket carries its ETag; no stray folders")
def _():
    import json as _json
    flat = pa.schema([("n", pa.int64())])
    with tempfile.TemporaryDirectory() as root:
        s = sp.create_stream("v2test/cat", flat, store=root,
                                    memory=MemoryOptions(lookback=500),
                                    parquet=ParquetOptions(checkpoint_seconds=0.05))
        d = os.path.join(root, "streams", "v2test", "cat")
        tpath = os.path.join(root, "tickets", "v2test", "cat.json")
        try:
            for k in range(3):
                s.append(pa.record_batch([pa.array(range(k * 10, k * 10 + 10))],
                                         schema=flat))
                s.cut()
            wait_for(lambda: s._parquet.catalog.files == 3, what="3 files catalogued")
            assert s.archive_state().catalog_error is None
            etag = s._parquet.catalog.etag
            wait_for(lambda: _json.load(open(tpath)).get("catalog_etag") == etag,
                     what="the ticket names the catalog's ETag")
        finally:
            s.close()
        assert _catalog_rows(d) == [(30, 0, 29)]
        # the catalog makes no folder of its own. A finished file's
        # objects stay beside its checkpoint versions, for readers told
        # of them, until the keep time has passed.
        assert [n for n in os.listdir(d) if os.path.isdir(os.path.join(d, n))
                and not n.endswith('.parquet.chunks')] == []


@test("catalog: the reconcile at start registers every finished file the "
      "catalog lacks (a lost catalog is rebuilt from the files)")
def _():
    flat = pa.schema([("n", pa.int64())])
    with tempfile.TemporaryDirectory() as root:
        _catalog_writer_run(root, flat, [[pa.array(range(0, 10))],
                                         [pa.array(range(10, 25))]])
        d = os.path.join(root, "streams", "v2test", "cat")
        os.remove(os.path.join(d, "catalog.ducklake"))
        s = sp.create_stream("v2test/cat", flat, store=root,
                                    memory=MemoryOptions(lookback=500),
                                    parquet=ParquetOptions(checkpoint_seconds=0.05))
        try:
            wait_for(lambda: s._parquet.catalog.files == 2, what="rebuilt from the files")
        finally:
            s.close()
        assert _catalog_rows(d) == [(25, 0, 24)]


@test("catalog: a new column is added to the table before its file "
      "registers (older rows read null); a changed type is refused, loudly")
def _():
    with tempfile.TemporaryDirectory() as root:
        one = pa.schema([("n", pa.int64())])
        two = pa.schema([("n", pa.int64()), ("extra", pa.string())])
        _catalog_writer_run(root, one, [[pa.array([1, 2])]])
        _catalog_writer_run(root, two, [[pa.array([3]), pa.array(["x"])]])
        d = os.path.join(root, "streams", "v2test", "cat")
        assert _catalog_rows(d, "SELECT n, extra FROM b.data ORDER BY n") == \
            [(1, None), (2, None), (3, "x")]
        bad = pa.schema([("n", pa.string())])
        s = sp.create_stream("v2test/cat", bad, store=root,
                                    memory=MemoryOptions(lookback=500),
                                    parquet=ParquetOptions(checkpoint_seconds=0.05,
                                                    ship_seconds=60))
        try:
            s.append(pa.record_batch([pa.array(["four"])], schema=bad))
            s.cut()
            wait_for(lambda: s.archive_state().catalog_error is not None,
                     what="the changed type refused")
            assert not s.archive_state().failing, "the files are fine"
        finally:
            s.close()
        assert _catalog_rows(d, "SELECT count(*) FROM b.data") == [(3,)]


@test("catalog: a catalog changed by someone else since this writer's "
      "upload is fetched again and reconciled, never overwritten blind")
def _():
    import shutil
    flat = pa.schema([("n", pa.int64())])
    with tempfile.TemporaryDirectory() as root:
        s = sp.create_stream("v2test/cat", flat, store=root,
                                    memory=MemoryOptions(lookback=500),
                                    parquet=ParquetOptions(checkpoint_seconds=0.05))
        d = os.path.join(root, "streams", "v2test", "cat")
        try:
            s.append(pa.record_batch([pa.array(range(10))], schema=flat))
            s.cut()
            wait_for(lambda: s._parquet.catalog.files == 1, what="first file")
            pub = os.path.join(d, "catalog.ducklake")
            shutil.copyfile(pub, pub + ".x")
            time.sleep(0.01)
            os.replace(pub + ".x", pub)         # someone else's upload: a new ETag
            s.append(pa.record_batch([pa.array(range(10, 20))], schema=flat))
            s.cut()
            wait_for(lambda: s._parquet.catalog.files == 2, what="second file")
            assert s.archive_state().catalog_error is None
        finally:
            s.close()
        assert _catalog_rows(d) == [(20, 0, 19)]


@test("catalog: a dated copy once the interval has passed, only if the "
      "catalog changed since the newest copy; None makes none")
def _():
    import re
    flat = pa.schema([("n", pa.int64())])
    copy_name = re.compile(r"^catalog\.\d{8}-\d{6}\.ducklake$")
    with tempfile.TemporaryDirectory() as root:
        d = os.path.join(root, "streams", "v2test", "copy")
        def copies():
            return sorted(n for n in os.listdir(d) if copy_name.match(n))
        s = sp.create_stream("v2test/copy", flat, store=root,
                                    memory=MemoryOptions(lookback=500),
                                    parquet=ParquetOptions(checkpoint_seconds=0.05,
                                                    ship_seconds=0.1,
                                                    catalog_copy_seconds=1.5))
        try:
            s.append(pa.record_batch([pa.array(range(10))], schema=flat))
            s.cut()
            wait_for(lambda: len(copies()) == 1, what="the first copy")
            s.append(pa.record_batch([pa.array(range(10, 20))], schema=flat))
            s.cut()                             # changed, but within the interval
            wait_for(lambda: s._parquet.catalog.files == 2, what="second file")
            time.sleep(0.3)
            assert len(copies()) == 1, copies()
            wait_for(lambda: len(copies()) == 2, timeout=10.0,
                     what="the changed catalog copied once the interval passed")
            time.sleep(2.0)                     # unchanged since: no third copy
            assert len(copies()) == 2, copies()
        finally:
            s.close()
        c = duckdb.connect()
        c.execute(f"ATTACH 'ducklake:{os.path.join(d, copies()[-1])}' AS b (READ_ONLY)")
        assert c.sql("SELECT count(*) FROM b.data").fetchall()[0] == (20,)
    with tempfile.TemporaryDirectory() as root:
        _catalog_writer_run(root, flat, [[pa.array([1])]], name="v2test/nocopy")
        s = sp.create_stream("v2test/nocopy", flat, store=root,
                                    memory=MemoryOptions(lookback=500),
                                    parquet=ParquetOptions(checkpoint_seconds=0.05,
                                                    catalog_copy_seconds=None))
        s.append(pa.record_batch([pa.array([2])], schema=flat))
        s.cut()
        s.close()
        d = os.path.join(root, "streams", "v2test", "nocopy")
        # the first run used the default (a day): one copy; this run none
        assert len([n for n in os.listdir(d) if copy_name.match(n)]) == 1, os.listdir(d)



def _slow_catalog(seconds_per_file):
    """Make every catalog registration take `seconds_per_file` longer
    (a first pass over a full bucket); returns the undo."""
    from streaming_parquet import _catalog
    orig = _catalog.Catalog._register

    def slow(self, con, path):
        time.sleep(seconds_per_file)
        return orig(self, con, path)
    _catalog.Catalog._register = slow

    def undo():
        _catalog.Catalog._register = orig
    return undo


@test("catalog: stop() ends a long pass between two files, publishes "
      "nothing, and the next start's reconcile registers every file")
def _():
    import duckdb as _duckdb
    from streaming_parquet import _catalog
    flat = pa.schema([("n", pa.int64())])
    with tempfile.TemporaryDirectory() as root:
        undo = _slow_catalog(0.0)
        try:
            _catalog_writer_run(root, flat, [[pa.array([k])] for k in range(20)],
                                name="v2test/cstop")
        finally:
            undo()
        d = os.path.join(root, "streams", "v2test", "cstop")
        os.remove(os.path.join(d, _catalog.CATALOG))    # as if never published
        work = os.path.join(root, "work.ducklake")
        cat = _catalog.Catalog("v2test/cstop", d + "/", work, 1.0, None)
        undo = _slow_catalog(0.1)                       # 20 files: about 2 s
        try:
            cat.poke()
            wait_for(lambda: cat.progress[0] >= 3, what="the pass under way")
            t0 = time.monotonic()
            cat.stop()
            assert cat.wait(5.0), "a stopped pass did not end"
            assert time.monotonic() - t0 < 1.0, time.monotonic() - t0
        finally:
            undo()
        n, total = cat.progress
        assert total == 20 and 3 <= n < 20, cat.progress
        assert cat.error is None, cat.error
        assert not os.path.exists(os.path.join(d, _catalog.CATALOG)), \
            "a stopped pass published"
        # the next start: a fresh catalog reconciles all 20
        cat2 = _catalog.Catalog("v2test/cstop", d + "/", work, 1.0, None)
        cat2.poke()
        assert cat2.wait(30.0) and cat2.error is None, cat2.error
        c = _duckdb.connect()
        c.execute(f"ATTACH 'ducklake:{os.path.join(d, _catalog.CATALOG)}' "
                  f"AS b (READ_ONLY)")
        assert c.sql("SELECT count(*) FROM b.data").fetchall()[0] == (20,)


def _block_size(path):
    c = duckdb.connect()
    try:
        c.execute(f"ATTACH '{path}' AS f (READ_ONLY)")
        return c.execute("SELECT block_size FROM pragma_database_size() "
                         "WHERE database_name = 'f'").fetchone()[0]
    finally:
        c.close()


@test("catalog: the catalog has 16 KB blocks; one published with 256 KB "
      "blocks is copied into them once and published")
def _():
    from streaming_parquet import _catalog
    flat = pa.schema([("n", pa.int64())])
    with tempfile.TemporaryDirectory() as root:
        _catalog_writer_run(root, flat, [[pa.array(range(0, 10))],
                                         [pa.array(range(10, 25))]],
                            name="v2test/blocks")
        d = os.path.join(root, "streams", "v2test", "blocks")
        pub = os.path.join(d, _catalog.CATALOG)
        assert _block_size(pub) == 16384, _block_size(pub)
        # as a writer before 2026-11-03 published it: DuckDB's default blocks
        old = os.path.join(root, "old.ducklake")
        c = duckdb.connect()
        c.execute(f"ATTACH '{pub}' AS a (READ_ONLY)")
        c.execute(f"ATTACH '{old}' AS b (BLOCK_SIZE 262144)")
        c.execute("COPY FROM DATABASE a TO b")
        c.close()
        os.replace(old, pub)
        assert _block_size(pub) == 262144
        work = os.path.join(root, "work.ducklake")
        cat = _catalog.Catalog("v2test/blocks", d + "/", work, 1.0, None)
        cat.poke()
        assert cat.wait(10.0) and cat.error is None, cat.error
        assert _block_size(pub) == 16384, _block_size(pub)
        assert cat.files == 2, cat.files
        assert _catalog_rows(d) == [(25, 0, 24)]
        # copied once: the next writer's pass leaves it alone
        etag = cat.etag
        cat2 = _catalog.Catalog("v2test/blocks", d + "/", work, 1.0, None)
        cat2.poke()
        assert cat2.wait(10.0) and cat2.error is None, cat2.error
        assert cat2.etag == etag, (cat2.etag, etag)


@test("catalog: two streams' passes run at the same time")
def _():
    from streaming_parquet import _catalog
    flat = pa.schema([("n", pa.int64())])
    with tempfile.TemporaryDirectory() as root:
        cats = []
        for name in ("v2test/par1", "v2test/par2"):
            _catalog_writer_run(root, flat, [[pa.array([k])] for k in range(5)],
                                name=name)
            d = os.path.join(root, "streams", *name.split("/"))
            os.remove(os.path.join(d, _catalog.CATALOG))
            cats.append(_catalog.Catalog(name, d + "/", os.path.join(
                root, name.replace("/", "-") + ".ducklake"), 1.0, None))
        undo = _slow_catalog(0.3)                       # 5 files: 1.5 s a pass
        try:
            t0 = time.monotonic()
            for cat in cats:
                cat.poke()
            for cat in cats:
                assert cat.wait(10.0) and cat.error is None, cat.error
            took = time.monotonic() - t0
        finally:
            undo()
        assert all(cat.files == 5 for cat in cats), [c.files for c in cats]
        assert took < 2.5, f"{took:.1f} s: the passes waited on each other"


@test("catalog: a stream's close shows the pass's progress, stops it "
      "between two files, and returns within seconds")
def _():
    import contextlib
    import io as _io
    from streaming_parquet import _catalog
    _d = sys.modules["streaming_parquet._diag"]     # the module, not the logger
    flat = pa.schema([("n", pa.int64())])
    with tempfile.TemporaryDirectory() as root:
        undo = _slow_catalog(0.0)
        try:
            _catalog_writer_run(root, flat, [[pa.array([k])] for k in range(40)],
                                name="v2test/cclose")
        finally:
            undo()
        d = os.path.join(root, "streams", "v2test", "cclose")
        os.remove(os.path.join(d, _catalog.CATALOG))    # a backlog of 40
        undo = _slow_catalog(0.25)                      # 40 files: about 10 s
        err = _io.StringIO()
        was = _d._console_suppressed
        _d._console_suppressed = False
        try:
            with contextlib.redirect_stderr(err):
                s = sp.create_stream("v2test/cclose", flat, store=root,
                                            memory=MemoryOptions(lookback=500),
                                            parquet=ParquetOptions(checkpoint_seconds=0.05))
                wait_for(lambda: s._parquet.catalog.progress[0] >= 2,
                         what="the start's reconcile under way")
                t0 = time.monotonic()
                s.close()
                took = time.monotonic() - t0
        finally:
            _d._console_suppressed = was
            undo()
        out = err.getvalue()
        assert took < 8.0, took
        assert "catalog pass running," in out, out
        assert "stopping the catalog pass after its current file" in out, out
        assert "catalog pass stopped by close after" in out, out


@test("close: a slow last upload is named on the console, with its size, "
      "and a timeout says the file is safe in the spool")
def _():
    import contextlib
    import io as _io
    from parquet_checkpoint import FakeS3
    _d = sys.modules["streaming_parquet._diag"]
    flat = pa.schema([("n", pa.int64())])
    with tempfile.TemporaryDirectory() as spool:
        remote = FakeS3(part_min=4096)
        s = sp.create_stream("v2test/slowup", flat, store=remote, spool=spool,
                                    memory=MemoryOptions(lookback=500),
                                    parquet=ParquetOptions(checkpoint_seconds=0.05,
                                                    ship_seconds=0.05))
        s.append(pa.record_batch([pa.array(range(100))], schema=flat))
        wait_for(lambda: s.archive_state() is not None and
                 sp._tickets.get_ticket(remote, "v2test/slowup") is not None,
                 what="first ship")
        put = remote.put

        def slow_put(name, data):
            if name.endswith(".ckpt"):
                time.sleep(5.0)              # the last checkpoint, slowly
            return put(name, data)
        s.append(pa.record_batch([pa.array(range(100, 200))], schema=flat))
        err = _io.StringIO()
        was = _d._console_suppressed
        _d._console_suppressed = False
        try:
            with contextlib.redirect_stderr(err):
                remote.put = slow_put
                s.close()
        finally:
            _d._console_suppressed = was
            remote.put = put
        out = err.getvalue()
        assert "close: uploading checkpoint" in out and " MB), " in out, out
        assert "the file is complete in the local spool" in out, out
        assert "the next start finishes the upload" in out, out


@test("parquet_checkpoint extension: a connection opened without "
      "allow_unsigned_extensions is refused, naming the line that opens one")
def _():
    from streaming_parquet import _extension
    try:
        _extension.load(duckdb.connect(), "duckdb_all('v2test/ext')")
        raise AssertionError("a plain connection loaded the unsigned extension")
    except RuntimeError as e:
        assert "duckdb_all('v2test/ext')" in str(e), e
        assert 'duckdb.connect(config={"allow_unsigned_extensions": "true"})' in str(e), e


@test("parquet_checkpoint extension: DuckDB reads the ticket's unfinished "
      "file, by its newest version's path, every stored row; an older kept "
      "version still reads its own rows (directory store)")
def _():
    import json as _json
    import tempfile
    from streaming_parquet import _extension
    flat = pa.schema([("n", pa.int64()), ("s", pa.string())])
    with tempfile.TemporaryDirectory() as root:
        tpath = os.path.join(root, "tickets", "v2test", "ext.json")
        def ticket():
            try:
                with open(tpath) as f:
                    return _json.load(f)
            except (FileNotFoundError, ValueError):
                return None
        s = sp.create_stream("v2test/ext", flat, store=root,
                                    memory=MemoryOptions(lookback=500),
                                    parquet=ParquetOptions(checkpoint_seconds=0.05))
        try:
            con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
            _extension.load(con, "test")
            _extension.load(con, "test")        # a second load is a no-op
            con.sql("SELECT 1").fetchall()      # load leaves no unread result
            def rows(n):
                return pa.record_batch([pa.array(range(n, n + 40)),
                                        pa.array([f"r{i}" for i in range(n, n + 40)])],
                                       schema=flat)
            s.append(rows(0))
            wait_for(lambda: (ticket() or {}).get("stored_row") == 39,
                     what="ticket stored_row 39")
            u = ticket()["unfinished"][0]
            got = con.sql(f"SELECT count(*), sum(n), max(s) FROM "
                          f"read_parquet('pqc://{u}') WHERE n >= 10").fetchall()[0]
            assert got == (30, sum(range(10, 40)), "r39"), got
            s.append(rows(40))                  # the same file grows
            wait_for(lambda: (ticket() or {}).get("stored_row") == 79,
                     what="ticket stored_row 79")
            u2 = ticket()["unfinished"][0]
            same = parquet_checkpoint.parse_ckpt_object
            assert u2 != u and same(os.path.basename(u2))[0] == \
                same(os.path.basename(u))[0], (u, u2)   # a newer version
            got = con.sql(f"SELECT count(*), max(n) FROM "
                          f"read_parquet('pqc://{u2}')").fetchall()[0]
            assert got == (80, 79), got
            if os.path.exists(u):               # kept: superseded under 10 minutes
                got = con.sql(f"SELECT count(*), max(n) FROM "
                              f"read_parquet('pqc://{u}')").fetchall()[0]
                assert got == (40, 39), got
        finally:
            s.close()


@test("batch side data: 16 bytes (first row, ticket revision), 8 (first "
      "row only), or refused")
def _():
    import struct as _s
    from streaming_parquet._attach import _side_data
    assert _side_data(pa.py_buffer(_s.pack('<qq', 41, 7))) == (41, 7)
    assert _side_data(pa.py_buffer(_s.pack('<qq', -1, 8))) == (None, 8)
    assert _side_data(pa.py_buffer(_s.pack('<q', 41))) == (41, None)
    assert _side_data(None) == (None, None)
    try:
        _side_data(pa.py_buffer(b"12345"))
        raise AssertionError("a 5-byte side data was accepted")
    except ValueError as e:
        assert "want 16" in str(e), e


@test("ticket revision: each ticket write raises it; a live reader learns of "
      "a new ticket at once, even with no new rows (an empty batch)")
def _():
    import json as _json
    flat = pa.schema([("n", pa.int64())])
    with tempfile.TemporaryDirectory() as root:
        tpath = os.path.join(root, "tickets", "v2test", "rev.json")

        def ticket():
            try:
                with open(tpath) as f:
                    return _json.load(f)
            except (FileNotFoundError, ValueError):
                return None
        s = sp.create_stream("v2test/rev", flat, store=root,
                                    memory=MemoryOptions(lookback=500),
                                    parquet=ParquetOptions(checkpoint_seconds=0.05))
        try:
            wait_for(lambda: (ticket() or {}).get("revision", 0) >= 1,
                     what="the first ticket")
            a = sp.connect(ticket=s._ticket, memory=MemoryOptions(lookback=500))
            s.append(pa.record_batch([pa.array(range(10))], schema=flat))
            wait_for(lambda: a._feeder.ticket_revision is not None,
                     what="a revision on the connection")
            wait_for(lambda: (ticket() or {}).get("stored_row") == 9,
                     what="the rows stored")
            r1 = ticket()["revision"]
            wait_for(lambda: a._feeder.ticket_revision == r1,
                     what="the reader at the ticket's revision")
            s.cut()                      # no new rows: the file finishes
            got = a._feeder.wait_ticket_revision(r1, 8.0)
            assert got is not None and got > r1, (got, r1)
            assert ticket()["revision"] >= got, (ticket(), got)
        finally:
            s.close()


@test("a finished file stays in the ticket until the catalog lists it; its "
      "checkpoints are deleted only after the ticket drops it")
def _():
    import json as _json
    from streaming_parquet import _catalog
    flat = pa.schema([("n", pa.int64())])
    with tempfile.TemporaryDirectory() as root:
        tpath = os.path.join(root, "tickets", "v2test", "seqb.json")
        d = os.path.join(root, "streams", "v2test", "seqb")

        def ticket():
            try:
                with open(tpath) as f:
                    return _json.load(f)
            except (FileNotFoundError, ValueError):
                return None
        undo = _slow_catalog(1.5)        # the catalog pass takes 1.5 s a file
        s = sp.create_stream("v2test/seqb", flat, store=root,
                                    memory=MemoryOptions(lookback=500),
                                    parquet=ParquetOptions(checkpoint_seconds=0.05))
        s._parquet.superseded_keep_s = 0.5  # 10 minutes in service
        try:
            s.append(pa.record_batch([pa.array(range(20))], schema=flat))
            wait_for(lambda: (ticket() or {}).get("stored_row") == 19,
                     what="rows stored")
            u = ticket()["unfinished"][0]
            final = parquet_checkpoint.parse_ckpt_object(os.path.basename(u))[0]
            s.cut()
            wait_for(lambda: os.path.exists(os.path.join(d, final)),
                     what="the final file assembled")
            # assembled, not yet in the catalog: still listed, still readable
            t = ticket()
            listed = [x for x in t["unfinished"]
                      if os.path.basename(x).startswith(final + ".")]
            assert listed, t
            assert os.path.exists(listed[0]), listed
            # once the catalog lists it: dropped from the ticket, then deleted
            wait_for(lambda: not [x for x in (ticket() or {}).get("unfinished", [])
                                  if os.path.basename(x).startswith(final + ".")],
                     timeout=20.0, what="the ticket dropping the file")
            # dropped: superseded; a later upload step deletes them once
            # they have been superseded long enough
            assert parquet_checkpoint.ckpt_versions(
                parquet_checkpoint.DirSink(d), final), "deleted too early"
            time.sleep(0.6)
            s.append(pa.record_batch([pa.array([20])], schema=flat))
            wait_for(lambda: not parquet_checkpoint.ckpt_versions(
                parquet_checkpoint.DirSink(d), final),
                what="its checkpoints deleted")
            assert ticket()["catalog_etag"] is not None
            c = duckdb.connect()
            c.execute(f"ATTACH 'ducklake:{os.path.join(d, _catalog.CATALOG)}' "
                      f"AS b (READ_ONLY)")
            assert c.sql("SELECT count(*) FROM b.data").fetchall()[0] == (20,)
        finally:
            s.close()
            undo()


@test("a restart that finds a finished file beside its kept checkpoints "
      "clears them once the catalog lists the file")
def _():
    from streaming_parquet import _catalog
    flat = pa.schema([("n", pa.int64())])
    with tempfile.TemporaryDirectory() as root:
        d = os.path.join(root, "streams", "v2test", "kept")
        os.makedirs(d)
        sink = parquet_checkpoint.DirSink(d)
        w = parquet_checkpoint.CheckpointWriter("20260101-000000.parquet", flat,
                                                sink=sink)
        w.write_table(pa.table({"n": list(range(7))}, schema=flat))
        w.close(keep_checkpoints=True)   # finished; checkpoints kept, then a crash
        assert sink.exists("20260101-000000.parquet")
        assert parquet_checkpoint.ckpt_versions(sink, "20260101-000000.parquet")
        s = sp.create_stream("v2test/kept", flat, store=root,
                                    memory=MemoryOptions(lookback=500),
                                    parquet=ParquetOptions(checkpoint_seconds=0.05))
        s._parquet.superseded_keep_s = 0.5  # 10 minutes in service
        try:
            time.sleep(0.6)
            s.append(pa.record_batch([pa.array([7])], schema=flat))  # an upload step
            wait_for(lambda: not parquet_checkpoint.ckpt_versions(
                sink, "20260101-000000.parquet"), what="the kept checkpoints cleared")
            c = duckdb.connect()
            c.execute(f"ATTACH 'ducklake:{os.path.join(d, _catalog.CATALOG)}' "
                      f"AS b (READ_ONLY)")
            assert c.sql("SELECT count(*) FROM b.data").fetchall()[0] == (7,)
        finally:
            s.close()


@test("archive view without a live connection: follows the ticket on the "
      "schedule its write time and beat give; exact across a cut")
def _():
    from streaming_parquet._views import StoreView, TicketSource
    flat = pa.schema([("n", pa.int64())])
    with tempfile.TemporaryDirectory() as root:
        # a 1 s beat: the reader polls about every 2 s
        s = sp.create_stream("v2test/poll", flat, store=root,
                                    memory=MemoryOptions(lookback=500),
                                    parquet=ParquetOptions(checkpoint_seconds=1.0))
        try:
            s.append(pa.record_batch([pa.array(range(30))], schema=flat))
            wait_for(lambda: (sp._tickets.get_ticket(root, "v2test/poll")
                              or {}).get("stored_row") == 29, timeout=15.0,
                     what="rows stored")
            con = view_con()
            v = StoreView(con, TicketSource("v2test/poll", root), "sp_poll",
                          flat, with_memory=False)       # no revisions: polls
            q = f"SELECT count(*), count(DISTINCT n), max(n) FROM {v.name}"
            assert con.sql(q).fetchall()[0] == (30, 30, 29)
            s.cut()
            s.append(pa.record_batch([pa.array(range(30, 45))], schema=flat))
            wait_for(lambda: con.sql(q).fetchall()[0] == (45, 45, 44),
                     timeout=15.0, what="the polled view at 45 rows")
            v.close()
        finally:
            s.close()


@test("a process that exits with archive views open exits cleanly (views "
      "close before the interpreter stops their threads)")
def _():
    import subprocess as _sp
    code = """
import os, sys, tempfile, time
sys.path.insert(0, %r)
import duckdb, pyarrow as pa
import streaming_parquet as sp
from streaming_parquet import MemoryOptions, ParquetOptions
flat = pa.schema([("n", pa.int64())])
root = tempfile.mkdtemp()
s = sp.create_stream("x/exit", flat, store=root, memory=MemoryOptions(lookback=500),
                     parquet=ParquetOptions(checkpoint_seconds=0.05))
s.append(pa.record_batch([pa.array(range(10))], schema=flat))
time.sleep(0.5)
con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
v = s.duckdb_all(con)
print(con.sql(f"SELECT count(*) FROM {v.name}").fetchall()[0][0], flush=True)
s.close()
""" % os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    r = _sp.run([sys.executable, '-c', code], capture_output=True, text=True,
                timeout=60)
    assert r.returncode == 0 and r.stdout.strip() == "10", (r.returncode,
                                                           r.stdout, r.stderr[-800:])

print("\n--- views and the ring ---")

@test("the seam: the oldest row boundary the store has that is above the "
      "ring's oldest quarter")
def _():
    from streaming_parquet._views import choose_seam, _ticket_files
    one = [{"first_row": 500, "final_rows": 250, "rows": 480}]
    full = (600, 999, 400)               # a full ring: rows 600 to 999
    # above the oldest quarter: row 700 and up
    assert choose_seam(990, one, None) == (990, False, None), "no ring"
    assert choose_seam(990, one, (0, -1, 400)) == (990, True, None), "empty ring"
    # the end of the file's finished row groups, row 749
    assert choose_seam(990, one, full) == (749, True, None)
    # that boundary inside the oldest quarter: the next one is stored_row
    low = [{"first_row": 500, "final_rows": 150, "rows": 480}]
    assert choose_seam(990, low, full) == (990, True, None)
    # two files: the older file's finished rows end at 749, the newer
    # file begins at 800; the oldest boundary above the quarter wins
    two = one + [{"first_row": 800, "final_rows": 0, "rows": 190}]
    assert choose_seam(990, two, full) == (749, True, None)
    assert choose_seam(990, low + [two[1]], full) == (799, True, None)
    # the store far behind: only stored_row is left, inside the quarter
    assert choose_seam(650, [], full) == (650, True, 'tight')
    # the ring begins after the store ends: full = too short, else late
    assert choose_seam(500, [], full) == (500, False, 'short')
    assert choose_seam(500, [], (600, 700, 400)) == (500, False, 'late')
    # a ring that is not full has its free rows as time too: the whole
    # unfinished file comes from the ring, nothing of it from the store
    young = [{"first_row": 0, "final_rows": 0, "rows": 90}]
    assert choose_seam(90, young, (0, 100, 400)) == (-1, True, None)
    # a ticket from a writer before `files`: paths alone, stored_row only
    old = _ticket_files({"unfinished": ["d/x.parquet.000003.ckpt"]})
    assert old == [{"checkpoint": "d/x.parquet.000003.ckpt",
                    "first_row": None, "final_rows": None, "rows": None}]
    assert choose_seam(990, old, full) == (990, True, None)


@test("the ticket says where each unfinished file is: its first row, the "
      "rows of its finished row groups, its rows")
def _():
    import json as _json
    flat = pa.schema([("n", pa.int64())])
    with tempfile.TemporaryDirectory() as root:
        tpath = os.path.join(root, "tickets", "v2test", "tfiles.json")

        def ticket():
            try:
                with open(tpath) as f:
                    return _json.load(f)
            except (FileNotFoundError, ValueError):
                return None
        s = sp.create_stream("v2test/tfiles", flat, store=root,
                             memory=MemoryOptions(lookback=400),
                             parquet=ParquetOptions(checkpoint_seconds=0.05,
                                             row_group_rows=200))
        try:
            # ten rows at a time: a row group ends at exactly 200 rows (one
            # append of 250 would be written as one row group of 250)
            for k in range(0, 250, 10):
                s.append(pa.record_batch([pa.array(range(k, k + 10))],
                                         schema=flat))
            wait_for(lambda: (ticket() or {}).get("stored_row") == 249,
                     what="250 rows stored")
            t = ticket()
            assert t["keep_seconds"] == parquet_checkpoint.SUPERSEDED_KEEP_S, t
            assert t["files"] == [{"checkpoint": t["unfinished"][0],
                                   "first_row": 0, "final_rows": 200,
                                   "rows": 250}], t
            s.cut()
            s.append(pa.record_batch([pa.array(range(250, 260))], schema=flat))
            wait_for(lambda: (ticket() or {}).get("stored_row") == 259
                     and [f["first_row"] for f in ticket()["files"]][-1:] == [250],
                     what="the next file in the ticket, from row 250")
            last = ticket()["files"][-1]
            assert (last["final_rows"], last["rows"]) == (0, 10), last
        finally:
            s.close()


@test("the all view takes from the ring what the ring holds: its seam moves "
      "with the row group, not with the ticket, and every row is there once "
      "while it moves")
def _():
    flat = pa.schema([F('n', pa.int64(), nullable=False)])
    with tempfile.TemporaryDirectory() as tmp:
        s = sp.create_stream("v2test/ringseam", flat, store=tmp,
                             memory=MemoryOptions(lookback=400),
                             parquet=ParquetOptions(checkpoint_seconds=0.05,
                                             row_group_rows=200))
        try:
            con = view_con()
            v = s.duckdb_all(con)
            first_revision = s.ticket_revision
            seams = set()
            written = 0
            for _ in range(150):         # 1,500 rows: 7 row groups and a half
                s.append(pa.record_batch(
                    [pa.array(range(written, written + 10))], schema=flat))
                written += 10
                time.sleep(0.02)
                cnt, dst, lo, hi = con.sql(
                    f"SELECT count(*), count(DISTINCT n), min(n), max(n) "
                    f"FROM {v.name}").fetchall()[0]
                assert (cnt, dst, lo, hi) == (written, written, 0, written - 1), \
                    (cnt, dst, lo, hi, written, v.seam)
                seams.add(v.seam)
            tickets = s.ticket_revision - first_revision
            assert tickets >= 30, f"only {tickets} tickets: the test is too quick"
            # a seam is the row before a file or the end of a row group,
            # never a row a checkpoint added in the middle of one
            assert all((x + 1) % 200 == 0 for x in seams), sorted(seams)
            assert v.rebuilds <= tickets // 3, (
                f"{v.rebuilds} view texts for {tickets} tickets")
            assert v.ring_note is None, v.ring_note
            v.close()
        finally:
            s.close()


@test("a query that reaches the ring after its rows are gone fails and "
      "says so; it does not answer with rows missing")
def _():
    flat = pa.schema([F('n', pa.int64(), nullable=False)])
    with tempfile.TemporaryDirectory() as tmp:
        s = sp.create_stream("v2test/ringgone", flat, store=tmp,
                             memory=MemoryOptions(lookback=400),
                             parquet=ParquetOptions(checkpoint_seconds=0.05,
                                             row_group_rows=200))
        try:
            con = view_con()
            # 650 rows, ten at a time: three row groups of 200 and 50 rows
            for k in range(0, 650, 10):
                s.append(pa.record_batch([pa.array(range(k, k + 10))],
                                         schema=flat))
                time.sleep(0.01)         # a writer the archive keeps up with
            v = s.duckdb_all(con)
            wait_for(lambda: v.seam == 599,
                     what="the seam at the end of the third row group")
            assert con.sql(f"SELECT count(*) FROM {v.name}").fetchall()[0][0] == 650
            # the view's text stays as it is (as for a query that began
            # now) while the stream writes more than the ring holds
            with v._lock:
                for k in range(650, 1150, 10):
                    s.append(pa.record_batch([pa.array(range(k, k + 10))],
                                             schema=flat))
                    time.sleep(0.01)
                assert s.memory_state().rows == 400
                try:
                    got = con.sql(f"SELECT count(*) FROM {v.name}").fetchall()
                    raise AssertionError(f"the query answered {got}")
                except duckdb.Error as e:
                    assert ("the ring does not hold row 600; it begins "
                            "at row 750") in str(e), e
                    assert "MemoryOptions(lookback" in str(e), e
            # the view follows again, and every row is there

            def counts():
                try:
                    return con.sql(f"SELECT count(*), count(DISTINCT n) "
                                   f"FROM {v.name}").fetchall()[0]
                except duckdb.Error:
                    return None          # the text of before, for a moment
            wait_for(lambda: counts() == (1150, 1150),
                     what="the view current again")
            v.close()
        finally:
            s.close()


@test("a ring too short for the store's lag: the view says so, and names "
      "the setting")
def _():
    flat = pa.schema([F('n', pa.int64(), nullable=False)])
    with tempfile.TemporaryDirectory() as tmp:
        s = sp.create_stream("v2test/ringshort", flat, store=tmp,
                             memory=MemoryOptions(lookback=400),
                             parquet=ParquetOptions(checkpoint_seconds=0.05,
                                             row_group_rows=200))
        try:
            con = view_con()
            for k in range(0, 650, 10):
                s.append(pa.record_batch([pa.array(range(k, k + 10))],
                                         schema=flat))
                time.sleep(0.01)         # a writer the archive keeps up with
            v = s.duckdb_all(con)
            wait_for(lambda: v.seam == 599,
                     what="the seam at the end of the third row group")
            with v._lock:
                v._note_ring('short', 10)
                assert "too short for the store's lag" in v.ring_note, v.ring_note
                assert "MemoryOptions(lookback=" in v.ring_note, v.ring_note
            wait_for(lambda: v.ring_note is None, what="the note cleared")
            v.close()
        finally:
            s.close()


@test("a file is cut when its row groups reach FILE_CUT_BYTES: the next "
      "row group begins a new file with the same label, and every row "
      "is there once")
def _():
    import random as _random
    from streaming_parquet import _stream
    flat = pa.schema([F('n', pa.int64(), nullable=False),
                      F('s', pa.utf8(), nullable=False)])
    rng = _random.Random(3)
    was = _stream.FILE_CUT_BYTES
    _stream.FILE_CUT_BYTES = 10_000      # about two row groups of these rows
    try:
        with tempfile.TemporaryDirectory() as tmp:
            s = sp.create_stream("v2test/bytecut", flat, store=tmp,
                                 memory=MemoryOptions(lookback=400),
                                 parquet=ParquetOptions(checkpoint_seconds=0.05,
                                                 row_group_rows=200))
            try:
                s.start_parquet("RUN")
                for k in range(0, 2400, 10):
                    s.append(pa.record_batch(
                        [pa.array(range(k, k + 10)),
                         pa.array([f"{rng.getrandbits(160):040x}"
                                   for _ in range(10)])], schema=flat))
                    time.sleep(0.01)
                d = s.archive_state().location

                def finished():
                    return sorted(f for f in os.listdir(d)
                                  if f.endswith('.parquet'))
                wait_for(lambda: len(finished()) >= 4,
                         what="four files cut by their bytes")
                con = view_con()
                v = s.duckdb_all(con)
                wait_for(lambda: con.sql(
                    f"SELECT count(*), count(DISTINCT n), min(n), max(n) "
                    f"FROM {v.name}").fetchall()[0] == (2400, 2400, 0, 2399),
                    what="every row once, across the files and memory")
                files = finished()
                assert all('-RUN' in f for f in files), files
                for f in files:           # a file is cut after it is large
                    size = pq.ParquetFile(os.path.join(d, f)).metadata
                    assert os.path.getsize(os.path.join(d, f)) >= 10_000, (f, size)
                    assert size.num_row_groups <= 4, (f, size.num_row_groups)
                v.close()
            finally:
                s.close()
    finally:
        _stream.FILE_CUT_BYTES = was


print("\n--- diagnostics ---")

@test("diag log: $STREAMING_PARQUET_DIAG_LOG moves it; default is the cwd")
def _():
    import subprocess
    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    probe = ("import logging, streaming_parquet; "
             "logging.getLogger('telemetry.diag').info('diag-log-probe')")
    with tempfile.TemporaryDirectory() as tmp:
        env = dict(os.environ, PYTHONPATH=repo)
        env.pop('STREAMING_PARQUET_DIAG_LOG', None)
        default = os.path.join(tmp, 'telemetry-diag.log')
        subprocess.run([sys.executable, '-c', probe], cwd=tmp, env=env,
                       check=True, capture_output=True)
        assert 'diag-log-probe' in open(default).read()
        os.remove(default)
        moved = os.path.join(tmp, 'state', 'sp-diag.log')
        os.mkdir(os.path.dirname(moved))
        env['STREAMING_PARQUET_DIAG_LOG'] = moved
        subprocess.run([sys.executable, '-c', probe], cwd=tmp, env=env,
                       check=True, capture_output=True)
        assert 'diag-log-probe' in open(moved).read()
        assert not os.path.exists(default)


print("\n--- parquet on and off at any time ---")


def files_rows(folder):
    """{file name: its 'mark' column... the tv values} of the finished
    parquet files in a stream's folder."""
    out = {}
    if os.path.isdir(folder):
        for f in sorted(os.listdir(folder)):
            if f.endswith(".parquet"):
                out[f] = [int(v) for v in
                          pq.read_table(os.path.join(folder, f)).column("tv").to_pylist()]
    return out


@test("start_parquet on a recording stream is a mark: the open file ends there, no row is lost")
def _():
    with tempfile.TemporaryDirectory() as tmp:
        s = sp.create_stream("v2test/mark", SCHEMA, store=tmp,
                             memory=MemoryOptions(lookback=500),
                             parquet=ParquetOptions())
        fill(s, 25)
        s.start_parquet("B")
        fill(s, 15, start=25)
        s.close()
        got = files_rows(os.path.join(tmp, "streams", "v2test/mark"))
        assert len(got) == 2, got
        second = [f for f in got if f.endswith("-B.parquet")][0]
        first = [f for f in got if f != second][0]
        assert got[first] == list(range(25)), got
        assert got[second] == list(range(25, 40)), got


@test("a stream created with parquet=None writes no file until start_parquet, then only the later rows")
def _():
    with tempfile.TemporaryDirectory() as tmp:
        s = sp.create_stream("v2test/late", SCHEMA, store=tmp,
                             memory=MemoryOptions(lookback=500))
        assert s.parquet_options is None
        assert s.archive_state().location is None
        fill(s, 25)
        folder = os.path.join(tmp, "streams", "v2test/late")
        assert files_rows(folder) == {}
        s.start_parquet("DBG")
        fill(s, 15, start=25)
        s.stop_parquet()
        wait_for(lambda: list(files_rows(folder).values()) == [list(range(25, 40))],
                 what="the file with the rows after the mark")
        assert s.parquet_options == ParquetOptions()
        assert s.archive_state().location is not None
        fill(s, 5, start=40)             # off again: these rows are in no file
        s.close()
        assert list(files_rows(folder).values()) == [list(range(25, 40))]


@test("start_parquet(options=...) replaces the stream's options whole, from that file on")
def _():
    with tempfile.TemporaryDirectory() as tmp:
        s = sp.create_stream("v2test/opts", SCHEMA, store=tmp,
                             memory=MemoryOptions(lookback=500),
                             parquet=ParquetOptions(checkpoint_seconds=7.0))
        fill(s, 10)
        snappy = ParquetOptions(compression={"compression": "snappy"})
        s.start_parquet("S", options=snappy)
        fill(s, 10, start=10)
        s.start_parquet("Z", options=ParquetOptions())
        fill(s, 10, start=20)
        s.close()
        folder = os.path.join(tmp, "streams", "v2test/opts")

        def codec(tag):
            f = [f for f in os.listdir(folder)
                 if f.endswith(".parquet") and (f"-{tag}" in f if tag else "-" not in f[16:])][0]
            return pq.ParquetFile(os.path.join(folder, f)).metadata.row_group(0).column(0).compression
        assert codec("S") == "SNAPPY", codec("S")
        assert codec("Z") == "ZSTD", codec("Z")
        # whole replacement: checkpoint_seconds went back to its default
        assert s.parquet_options == ParquetOptions()
        assert s._checkpoint_seconds is None


@test("start_parquet refuses a row group the ring cannot hold")
def _():
    with tempfile.TemporaryDirectory() as tmp:
        s = sp.create_stream("v2test/toobig", SCHEMA, store=tmp,
                             memory=MemoryOptions(lookback=500))
        try:
            s.start_parquet(options=ParquetOptions(row_group_rows=400))
        except ValueError as e:
            assert "less than two" in str(e), e
        else:
            raise AssertionError("a row group of 400 in a ring of 500 was accepted")
        assert s.parquet_options is None


@test("start_parquet then stop_parquet at once: the rows between are in the file, the stream is off")
def _():
    with tempfile.TemporaryDirectory() as tmp:
        s = sp.create_stream("v2test/quick", SCHEMA, store=tmp,
                             memory=MemoryOptions(lookback=500))
        fill(s, 5)
        s.start_parquet("Q")
        fill(s, 5, start=5)
        s.stop_parquet()
        fill(s, 5, start=10)
        folder = os.path.join(tmp, "streams", "v2test/quick")
        wait_for(lambda: list(files_rows(folder).values()) == [list(range(5, 10))],
                 what="the file with the five rows between start and stop")
        s.close()
        assert list(files_rows(folder).values()) == [list(range(5, 10))]


@test("start_parquet with no store and no spool is refused at the call")
def _():
    s = sp.create_stream("v2test/nowhere", SCHEMA, spool=None,
                         memory=MemoryOptions(lookback=500))
    try:
        s.start_parquet()
    except ValueError as e:
        assert "nowhere to write parquet" in str(e), e
    else:
        raise AssertionError("accepted")


@test("module start_parquet leaves a stream with no parquet options alone")
def _():
    with tempfile.TemporaryDirectory() as tmp:
        on = sp.create_stream("v2test/mod-on", SCHEMA, store=tmp,
                              memory=MemoryOptions(lookback=500),
                              parquet=ParquetOptions())
        off = sp.create_stream("v2test/mod-off", SCHEMA, store=tmp,
                               memory=MemoryOptions(lookback=500))
        sp.start_parquet("M")
        fill(on, 5)
        fill(off, 5)
        sp.close()
        assert list(files_rows(os.path.join(tmp, "streams", "v2test/mod-on")).values()) == [list(range(5))]
        assert files_rows(os.path.join(tmp, "streams", "v2test/mod-off")) == {}
        assert off.parquet_options is None


@test("new options change the ticket's beat")
def _():
    with tempfile.TemporaryDirectory() as tmp:
        s = sp.create_stream("v2test/beat", SCHEMA, store=tmp,
                             memory=MemoryOptions(lookback=500))
        wait_for(lambda: sp._tickets.get_ticket(tmp, "v2test/beat") is not None,
                 what="the first ticket")
        assert sp._tickets.get_ticket(tmp, "v2test/beat")["beat_seconds"] == 5.0
        s.start_parquet(options=ParquetOptions(checkpoint_seconds=1.0))
        wait_for(lambda: sp._tickets.get_ticket(tmp, "v2test/beat")["beat_seconds"] == 1.0,
                 what="the ticket with the new beat")
        assert sp._tickets.get_ticket(tmp, "v2test/beat")["checkpoint_seconds"] == 1.0


print("\n--- published or internal ---")


@test("an internal stream has no ticket and is not served; this process reads it by name")
def _():
    with tempfile.TemporaryDirectory() as tmp:
        s = sp.create_stream("v2test/int", SCHEMA, store=tmp, publish=False,
                             memory=MemoryOptions(lookback=500))
        assert not s.published
        fill(s, 10)
        time.sleep(0.3)                  # a heartbeat or two
        assert sp._tickets.get_ticket(tmp, "v2test/int") is None
        assert "v2test/int" not in sp.list_streams(tmp)
        # this process reads it
        a = sp.connect("v2test/int")
        assert a.state == "streaming" and a._newest_seq == 9
        # another process, even one that knows the host and port, does not
        direct = {"name": "v2test/int", "host": "127.0.0.1", "port": s._port,
                  "run": s._run}
        b = sp.connect(ticket=direct, memory=MemoryOptions(lookback=500))
        time.sleep(1.0)
        assert b.state != "streaming", b.state
        b.close()


@test("publish() and unpublish() change a running stream")
def _():
    with tempfile.TemporaryDirectory() as tmp:
        s = sp.create_stream("v2test/pub", SCHEMA, store=tmp, publish=False,
                             memory=MemoryOptions(lookback=500))
        fill(s, 10)
        s.publish()
        assert s.published
        wait_for(lambda: sp._tickets.get_ticket(tmp, "v2test/pub") is not None,
                 what="the ticket after publish()")
        b = sp.connect(ticket=sp._tickets.get_ticket(tmp, "v2test/pub"),
                       memory=MemoryOptions(lookback=500))
        wait_for(lambda: b.state == "streaming",
                 what="a reader over Flight after publish()")
        fill(s, 5, start=10)
        wait_for(lambda: b._newest_seq == 14, what="rows over Flight")
        s.unpublish()
        assert not s.published
        assert sp._tickets.get_ticket(tmp, "v2test/pub") is None
        time.sleep(0.5)                  # heartbeats: none writes it back
        assert sp._tickets.get_ticket(tmp, "v2test/pub") is None
        wait_for(lambda: b.state != "streaming", what="the reader losing the stream")
        b.close()
        # still readable here, and still writable
        fill(s, 5, start=15)
        assert sp.connect("v2test/pub")._newest_seq == 19


@test("an internal stream writes parquet and its own views read the files, with no ticket in the store")
def _():
    with tempfile.TemporaryDirectory() as tmp:
        s = sp.create_stream("v2test/intpq", SCHEMA, store=tmp, publish=False,
                             memory=MemoryOptions(lookback=500))
        s.start_parquet("DBG", options=ParquetOptions(row_group_rows=50))
        fill(s, 120)
        con = view_con()
        rel = s.duckdb_all(con)
        wait_for(lambda: con.sql(f"SELECT count(*) FROM {rel.name}").fetchall()[0][0] == 120,
                 what="all rows through the stream's own view")
        rel.close()
        assert sp._tickets.get_ticket(tmp, "v2test/intpq") is None
        s.close()
        folder = os.path.join(tmp, "streams", "v2test/intpq")
        assert sum(len(v) for v in files_rows(folder).values()) == 120


@test("two processes can have an internal stream of one name; the second to write parquet is refused")
def _():
    import subprocess
    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    child = (
        "import sys, pyarrow as pa, streaming_parquet as sp\n"
        "s = sp.create_stream('v2test/shared', pa.schema([('tv', pa.float64())]),\n"
        "                     store=sys.argv[1], publish=False)\n"
        "print('CREATED', flush=True); sys.stdin.readline()\n"
        "s.start_parquet('C')\n"
        "print('STARTED', flush=True); sys.stdin.readline()\n")
    with tempfile.TemporaryDirectory() as tmp:
        env = dict(os.environ, PYTHONPATH=repo,
                   STREAMING_PARQUET_DIAG_LOG=os.path.join(tmp, "child.log"))
        proc = subprocess.Popen([sys.executable, "-c", child, tmp], env=env,
                                stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                text=True)
        try:
            assert proc.stdout.readline().strip() == "CREATED"
            # the same internal name here: no lock, so no refusal
            s = sp.create_stream("v2test/shared", SCHEMA, store=tmp, publish=False,
                                 memory=MemoryOptions(lookback=500))
            proc.stdin.write("\n"); proc.stdin.flush()
            assert proc.stdout.readline().strip() == "STARTED"
            for call in (s.start_parquet, s.publish):
                try:
                    call()
                except RuntimeError as e:
                    assert "another process on this server" in str(e), e
                else:
                    raise AssertionError(f"{call.__name__} took a stream "
                                         f"another process writes")
            assert not s.published and s.parquet_options is None
        finally:
            proc.kill()
            proc.wait()


print("\n--- old names ---")

@test("the old names Memory, Archive, parquet= and archive= still work")
def _():
    from streaming_parquet import _deprecations
    _deprecations._told.clear()
    old_memory = sp.Memory(lookback=500)
    assert old_memory == MemoryOptions(lookback=500)
    old_archive = sp.Archive(checkpoint_seconds=1, parquet={"compression": "snappy"})
    assert old_archive == ParquetOptions(checkpoint_seconds=1,
                                         compression={"compression": "snappy"})
    with tempfile.TemporaryDirectory() as tmp:
        s = sp.create_stream("v2test/oldnames", SCHEMA, store=tmp,
                             memory=old_memory, archive=old_archive)
        fill(s, 10)
        s.close()
        files = [f for _r, _d, fs in os.walk(tmp) for f in fs
                 if f.endswith(".parquet")]
        assert len(files) == 1, files
    assert _deprecations._told == set(_deprecations.OLD_NAMES), _deprecations._told


@test("an old name warns once, with the new name and the day of removal")
def _():
    import logging
    from streaming_parquet import _deprecations
    got = []

    class Keep(logging.Handler):
        def emit(self, record):
            got.append(record.getMessage())
    handler = Keep()
    log = logging.getLogger("telemetry.diag")
    log.addHandler(handler)
    try:
        _deprecations._told.clear()
        sp.Memory()
        sp.Memory()
    finally:
        log.removeHandler(handler)
    lines = [g for g in got if "old name" in g]
    assert len(lines) == 1, lines
    assert "Memory is an old name for MemoryOptions" in lines[0]
    assert _deprecations.REMOVED_AFTER in lines[0]


@test("archive= and parquet= together are refused")
def _():
    try:
        sp.create_stream("v2test/both", SCHEMA, parquet=ParquetOptions(),
                         archive=ParquetOptions())
    except TypeError as e:
        assert "old name of parquet" in str(e)
    else:
        raise AssertionError("both were accepted")


@test("docs/deprecations.md and _deprecations.py list the same Python names")
def _():
    from streaming_parquet import _deprecations
    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    rows = {}
    for line in open(os.path.join(repo, "docs", "deprecations.md")):
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) == 5 and cells[2] == "Python":
            rows[cells[0].strip("`")] = (cells[1].strip("`"), cells[3], cells[4])
    want = {old: (new, _deprecations.DEPRECATED_ON, _deprecations.REMOVED_AFTER)
            for old, new in _deprecations.OLD_NAMES.items()}
    assert rows == want, (rows, want)


# ---------------------------------------------------------------------------
print(f"\n{'=' * 50}")
print(f"Results: {passed} passed, {failed} failed")
if errors:
    print(f"Failed: {', '.join(errors)}")
    sys.exit(1)
print("All tests passed!")
