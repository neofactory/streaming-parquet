"""arrow_ring exerciser — a persistent endurance/throughput tool.

Runs a telemetry-shaped load for a chosen duration: a writer thread at
a target row rate (fixed-width + strings + lists), a hot-path reader
polling read_row, a relation consumer that queries the standing
relation, and a parquet flusher that pins the rows after its watermark
and maintains the archive+memory union view. Verifies the union-view
invariant continuously and prints throughput/memory statistics at the
end. No reader pins the whole ring: the writer waits only when it is a
whole ring ahead of the flusher.

    python3 packages/arrow_ring/exerciser.py --seconds 10 --rate 20000

Not a unit test: this is the soak/measurement harness. Exit 1 on any
invariant violation.
"""

import argparse
import os
import resource
import sys
import tempfile
import threading
import time

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from arrow_ring import Ring, StaleReadError

F = pa.field
SCHEMA = pa.schema([F('seq', pa.int64(), nullable=False),
                    F('tv', pa.float64(), nullable=False),
                    F('v', pa.list_(pa.float64(), 3), nullable=False),
                    F('label', pa.utf8(), nullable=False)])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--seconds', type=float, default=10.0)
    ap.add_argument('--rate', type=float, default=20000.0,
                    help='target rows/second')
    ap.add_argument('--capacity', type=int, default=1 << 16)
    args = ap.parse_args()

    ring = Ring(SCHEMA, capacity=args.capacity)
    stop = threading.Event()
    errors = []
    stats = {'written': 0, 'hot_reads': 0, 'stale_reads': 0,
             'relations': 0, 'lapped': 0, 'flushes': 0, 'view_checks': 0}

    def writer():
        try:
            i = 0
            burst = max(1, int(args.rate / 500))   # ~500 commits/s
            t0 = time.monotonic()
            while not stop.is_set():
                for _ in range(burst):
                    row = ring.get_mutable_row(block_s=5.0)
                    row.seq = i
                    row.tv = i * 0.001
                    row.v = (i * 1.0, i * 2.0, i * 3.0)
                    row.label = f'v{i}'
                    i += 1
                ring.commit()
                stats['written'] = i
                # pace to the target rate
                ahead = i / args.rate - (time.monotonic() - t0)
                if ahead > 0.002:
                    time.sleep(ahead)
        except Exception as e:
            errors.append(('writer', e))
            stop.set()

    def hot_reader():
        try:
            while not stop.is_set():
                newest = ring.newest_seq
                if newest >= 0:
                    try:
                        r = ring.read_row(newest)
                        assert r.seq == newest
                        stats['hot_reads'] += 1
                    except StaleReadError:
                        stats['stale_reads'] += 1
                time.sleep(0.0002)
        except Exception as e:
            errors.append(('hot_reader', e))
            stop.set()

    def relation_consumer():
        try:
            con = duckdb.connect()
            con.register('live', ring.get_dynamic_relation())
            while not stop.is_set():
                try:
                    n, mn, mx = con.sql(
                        "SELECT count(*), min(seq), max(seq) "
                        "FROM live").fetchall()[0]
                except duckdb.Error as e:
                    if 'StaleReadError' not in str(e):
                        raise
                    stats['lapped'] += 1    # the writer passed the scan
                    continue
                if n:
                    assert n == mx - mn + 1, (n, mn, mx)
                    stats['relations'] += 1
                time.sleep(0.005)
        except Exception as e:
            errors.append(('relation_consumer', e))
            stop.set()

    with tempfile.TemporaryDirectory() as tmp:
        con = duckdb.connect()
        con.register('live', ring.get_dynamic_relation())
        files = []
        wm = -1
        held = None
        threads = [threading.Thread(target=writer),
                   threading.Thread(target=hot_reader),
                   threading.Thread(target=relation_consumer)]
        t_start = time.monotonic()
        for t in threads:
            t.start()
        try:
            while time.monotonic() - t_start < args.seconds \
                    and not stop.is_set():
                time.sleep(0.05)
                # The rows after the watermark, and no others. The pin
                # of one pass is kept until the next pass has its own:
                # the writer cannot recycle a row the archive has not
                # written.
                rel = ring.pin(wm + 1, 1 << 62)
                if len(rel) == 0:
                    rel.release()
                    continue
                if rel.lo != wm + 1:
                    errors.append(('flusher', AssertionError(
                        f"rows recycled before the archive had them: "
                        f"lo={rel.lo} wm={wm}")))
                    break
                if held is not None:
                    held.release()
                held = rel
                path = os.path.join(tmp, f'a{len(files)}.parquet')
                pq.write_table(rel.table, path)
                files.append(path)
                wm = rel.hi - 1
                stats['flushes'] += 1
                cnt, dst, mn, mx = con.sql(
                    f"SELECT count(*), count(DISTINCT seq), min(seq), "
                    f"max(seq) FROM (SELECT seq FROM "
                    f"read_parquet({files!r}) UNION ALL SELECT seq FROM "
                    f"live WHERE seq > {wm})").fetchall()[0]
                if not (cnt == dst == mx + 1 and mn == 0):
                    errors.append(('view', AssertionError(
                        f"count={cnt} distinct={dst} min={mn} max={mx}")))
                    break
                stats['view_checks'] += 1
        finally:
            stop.set()
            if held is not None:
                held.release()
            for t in threads:
                t.join()

        elapsed = time.monotonic() - t_start
        maxrss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 2**20
        pool_segs = ring.pool_segment_count(3)   # label leaf
        print(f"\nelapsed          {elapsed:8.1f} s")
        print(f"rows written     {stats['written']:8d} "
              f"({stats['written'] / elapsed:,.0f} rows/s)")
        print(f"hot reads        {stats['hot_reads']:8d} "
              f"(stale: {stats['stale_reads']})")
        print(f"relations        {stats['relations']:8d} "
              f"(lapped: {stats['lapped']})")
        print(f"parquet flushes  {stats['flushes']:8d} ({len(files)} files)")
        print(f"view checks      {stats['view_checks']:8d} (all exact)")
        print(f"label pool segs  {pool_segs:8d} (bounded = reclaim works)")
        print(f"max RSS          {maxrss_mb:8.0f} MB")
        for who, e in errors:
            print(f"ERROR [{who}]: {e}")
        return 1 if errors else 0


if __name__ == '__main__':
    sys.exit(main())
