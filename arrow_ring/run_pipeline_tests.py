"""Pipeline tests for arrow_ring: parquet archival, the DuckDB
archive+memory union view (the watermark seam, under a live writer), and
a real Arrow Flight loopback over gRPC.

Run:  python3 packages/arrow_ring/run_pipeline_tests.py

These prove the platform end-to-end before streaming_parquet builds on it:
ring -> record set -> parquet row groups -> DuckDB, ring -> Flight ->
ring, and the single-view invariant (every seq exactly once across the
flush seam, no duplicates, no gaps).
"""

import os
import sys
import tempfile
import threading
import time
import traceback

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.flight as fl
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from arrow_ring import Ring

F = pa.field
SCHEMA = pa.schema([F('seq', pa.int64(), nullable=False),
                    F('tv', pa.float64(), nullable=False),
                    F('label', pa.utf8(), nullable=False)])


def fill(ring, count, start=0):
    for i in range(start, start + count):
        row = ring.get_mutable_row()
        row.seq = i
        row.tv = i * 0.5
        row.label = f'v{i}'
    ring.commit()


# ---- parquet ---------------------------------------------------------------

def test_parquet_write_and_duckdb_readback():
    ring = Ring(SCHEMA, capacity=256)
    fill(ring, 100)
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, 'archive-0.parquet')
        with ring.get_static_relation() as rs:
            pq.write_table(rs.table, path)
        got = duckdb.sql(
            f"SELECT count(*) c, min(seq) mn, max(seq) mx, sum(tv) s "
            f"FROM read_parquet('{path}')").fetchall()[0]
        assert got == (100, 0, 99, sum(i * 0.5 for i in range(100))), got
        labels = duckdb.sql(
            f"SELECT label FROM read_parquet('{path}') "
            f"WHERE seq = 42").fetchall()[0]
        assert labels == ('v42',)


def test_parquet_rotation_row_groups():
    # One file per flushed region -- the per-cycle capture pattern.
    ring = Ring(SCHEMA, capacity=256)
    with tempfile.TemporaryDirectory() as tmp:
        files = []
        wm = -1                     # last flushed seq (the watermark)
        for cycle in range(4):
            fill(ring, 50, start=(wm + 1))
            with ring.get_static_relation() as rs:
                t = rs.table.filter(pc.greater(rs.table['seq'], wm))
                path = os.path.join(tmp, f'archive-{cycle}.parquet')
                pq.write_table(t, path)
                files.append(path)
                wm = rs.hi - 1
        total = duckdb.sql(
            f"SELECT count(*), count(DISTINCT seq), max(seq) "
            f"FROM read_parquet({files!r})").fetchall()[0]
        assert total == (200, 200, 199), total


# ---- the union view: archive + hot, one seam, zero dupes/gaps ---------------

def test_union_view_invariant_under_live_writer():
    """THE invariant of the whole design: at any instant,
    parquet(seq <= watermark) UNION ALL memory(seq > watermark) contains
    every committed seq exactly once. Checked repeatedly while a writer
    thread runs.

    The two readers are the ones the library has. The archive pins the
    rows after its watermark and no others, and keeps the pin of one
    pass until it has the pin of the next: the writer waits only when
    it is a whole ring ahead of the archive, so no row is recycled
    before it is in a file. The memory side is the standing relation,
    which pins one chunk at a time."""
    N = 3000
    ring = Ring(SCHEMA, capacity=512)
    write_errors = []

    def writer():
        try:
            i = 0
            while i < N:
                burst = min(7, N - i)
                for _ in range(burst):
                    row = ring.get_mutable_row(block_s=2.0)
                    row.seq = i
                    row.tv = i * 0.5
                    row.label = f'v{i}'
                    i += 1
                ring.commit()
                time.sleep(0.0005)
        except Exception as e:
            write_errors.append(e)

    with tempfile.TemporaryDirectory() as tmp:
        con = duckdb.connect()
        con.register('live', ring.get_dynamic_relation())
        files = []
        wm = -1
        checks = 0
        held = None
        t = threading.Thread(target=writer)
        t.start()
        while wm < N - 1 and not write_errors:
            rel = ring.pin(wm + 1, N)
            if len(rel) == 0:
                rel.release()
                time.sleep(0.0005)
                continue
            assert rel.lo == wm + 1, \
                f"rows recycled before the archive had them: " \
                f"lo={rel.lo} wm={wm}"
            if held is not None:
                held.release()
            held = rel
            path = os.path.join(tmp, f'archive-{len(files)}.parquet')
            pq.write_table(rel.table, path)
            files.append(path)
            # The seam moves with the flush: the query below uses the
            # watermark of the archive it reads.
            wm = rel.hi - 1
            q = (f"SELECT count(*), count(DISTINCT seq), min(seq), max(seq) "
                 f"FROM (SELECT seq FROM read_parquet({files!r}) "
                 f"      UNION ALL "
                 f"      SELECT seq FROM live WHERE seq > {wm})")
            cnt, dst, mn, mx = con.sql(q).fetchall()[0]
            assert cnt == dst == mx + 1 and mn == 0 and mx >= wm, \
                f"view broke: count={cnt} distinct={dst} min={mn} max={mx}"
            checks += 1
        if held is not None:
            held.release()
        t.join()
        assert not write_errors, write_errors[0]
        # Final: the archive alone holds everything exactly once.
        cnt, dst, mx = duckdb.sql(
            f"SELECT count(*), count(DISTINCT seq), max(seq) "
            f"FROM read_parquet({files!r})").fetchall()[0]
        assert (cnt, dst, mx) == (N, N, N - 1), (cnt, dst, mx)
        assert checks > 10, f"only {checks} live checks ran"


def test_duckdb_query_over_hot_relation():
    ring = Ring(SCHEMA, capacity=256)
    fill(ring, 200)
    with ring.get_static_relation() as rs:
        con = duckdb.connect()
        con.register('hot', rs.table)
        got = con.sql("SELECT sum(tv) FROM hot "
                      "WHERE seq BETWEEN 50 AND 59").fetchall()[0][0]
        assert got == sum(i * 0.5 for i in range(50, 60))
        top = con.sql("SELECT label FROM hot ORDER BY tv DESC "
                      "LIMIT 1").fetchall()[0]
        assert top == ('v199',)


def test_dynamic_relation_standing_view():
    # Register ONCE; every query sees current data, as of its own scan.
    ring = Ring(SCHEMA, capacity=256)
    fill(ring, 50)
    con = duckdb.connect()
    con.register('live', ring.get_dynamic_relation())
    assert con.sql("SELECT max(seq) FROM live").fetchall()[0][0] == 49
    fill(ring, 50, start=50)
    assert con.sql("SELECT max(seq) FROM live").fetchall()[0][0] == 99
    assert con.sql("SELECT count(*) FROM live").fetchall()[0][0] == 100
    # a scan in flight pins nothing between chunks: the writer wraps the
    # ring while a query that reads memory LAST waits on a slow source
    fill(ring, 156, start=100)            # full: 256 rows, seq 0..255
    slow_rows = pa.table({'k': pa.array(range(5), type=pa.int64())})

    class Slow:
        def __arrow_c_stream__(self, requested_schema=None):
            def gen():
                for i in range(5):
                    time.sleep(0.2)       # 1 s in all, while the writer wraps
                    fill(ring, 60, start=256 + i * 60)
                    yield slow_rows.slice(i, 1).to_batches()[0]
            return pa.RecordBatchReader.from_batches(
                slow_rows.schema, gen()).__arrow_c_stream__(requested_schema)
    con.register('slow', Slow())
    n_slow, n_live = con.sql(
        "SELECT (SELECT count(*) FROM slow), (SELECT count(*) FROM live)"
    ).fetchall()[0]
    assert n_slow == 5 and n_live == 256  # memory read as of its own scan
    assert ring.newest_seq == 555         # the writer never waited
    # writer keeps running; view keeps answering (pins self-release)
    for i in range(556, 856):
        row = ring.get_mutable_row(block_s=1.0)
        row.seq = i; row.tv = i * 0.5; row.label = f'v{i}'
        ring.commit()
        if i % 50 == 0:
            assert con.sql("SELECT max(seq) FROM live").fetchall()[0][0] == i


# ---- Flight loopback: the real wire -----------------------------------------

def test_flight_loopback_ring_to_ring():
    """Ring A served over real gRPC Flight; ring B ingests the stream.
    This is telemetry's remote-subscriber path minus ticket discovery."""
    a = Ring(SCHEMA, capacity=256)
    fill(a, 150)

    class Server(fl.FlightServerBase):
        def __init__(self):
            super().__init__('grpc://127.0.0.1:0')

        def do_get(self, context, ticket):
            rel = a.get_static_relation()
            table = rel.table       # zero-copy of pinned ring memory
            rel.release()           # table's refcounts keep memory alive
            return fl.RecordBatchStream(table)

    server = Server()
    try:
        client = fl.connect(f'grpc://127.0.0.1:{server.port}')
        got = client.do_get(fl.Ticket(b'hot')).read_all()
        b = Ring(SCHEMA, capacity=256)
        b.append_table(got)
        with b.get_static_relation() as rs_b:
            with a.get_static_relation() as rs_a:
                assert rs_b.table.to_pylist() == rs_a.table.to_pylist()
        assert b.read_row(149).label == 'v149'
    finally:
        server.shutdown()


def main():
    tests = [(name, fn) for name, fn in sorted(globals().items())
             if name.startswith('test_') and callable(fn)]
    failed = []
    for name, fn in tests:
        t0 = time.monotonic()
        try:
            fn()
            print(f"  PASS {name} ({time.monotonic() - t0:.1f}s)")
        except Exception:
            failed.append(name)
            print(f"  FAIL {name}")
            traceback.print_exc()
    print(f"\n{len(tests) - len(failed)}/{len(tests)} passed")
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
