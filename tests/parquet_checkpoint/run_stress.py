"""parquet_checkpoint stress harness — a persistent soak/kill tool.

Two modes (combine freely):

  python3 run_stress.py --kills 25
      Crash loop: spawn a sacrificial writer subprocess (rows at full
      speed, checkpoint per write), SIGKILL it at a random moment,
      finalize, verify EXACT content to the checkpoint boundary, repeat.

  python3 run_stress.py --seconds 60 --rate 20000
      Sustained: telemetry-rate writer with checkpoint-per-row-group
      and a live reader thread; reports throughput, checkpoint cost,
      logical size, and verifies the final file exactly.

Exit 1 on any violation. Run on BOTH Mac (APFS) and the Linux box
(ext4) before telemetry adopts the writer.
"""

import argparse
import os
import random
import subprocess
import sys
import tempfile
import threading
import time

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from parquet_checkpoint import (ckpt_versions,
                                CheckpointError, CheckpointReader,
                                CheckpointWriter, DirSink, FakeS3,
                                Shipper, finalize, live_table)
from parquet_checkpoint._sinks import FaultInjected, SinkError

F = pa.field
SCHEMA = pa.schema([F('n', pa.int64(), nullable=False),
                    F('x', pa.float64(), nullable=False),
                    F('s', pa.string(), nullable=False)])


def make_rows(start, count):
    return pa.table({'n': list(range(start, start + count)),
                     'x': [i * 0.5 for i in range(start, start + count)],
                     's': [f'row-{i}' for i in range(start, start + count)]},
                    schema=SCHEMA)


# ---- child role -------------------------------------------------------------
if len(sys.argv) > 1 and sys.argv[1] == '--child':
    path, group = sys.argv[2], int(sys.argv[3])
    w = CheckpointWriter(path, SCHEMA, compression='zstd')
    i = 0
    while True:
        w.write_table(make_rows(i, group))     # implies checkpoint
        i += group


def kill_loop(n_kills):
    rng = random.Random()
    recovered = 0
    rows_total = 0
    with tempfile.TemporaryDirectory() as tmp:
        sink = DirSink(tmp)
        for k in range(n_kills):
            name = f'k{k}.parquet'
            path = os.path.join(tmp, name)
            child = subprocess.Popen(
                [sys.executable, os.path.abspath(__file__), '--child',
                 path, '200'],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            deadline = time.monotonic() + 15.0
            while (not ckpt_versions(sink, name)
                   and time.monotonic() < deadline):
                time.sleep(0.01)
            assert ckpt_versions(sink, name), "child never checkpointed"
            time.sleep(rng.uniform(0.01, 1.0))
            child.kill()
            child.wait()
            status, where = finalize(name, sink)
            assert status == 'recovered'
            recovered += 1
            t = pq.read_table(path)
            n = t.num_rows
            assert n % 200 == 0 and n > 0, f"kill {k}: {n} rows"
            assert t.column('n').to_pylist() == list(range(n)), \
                f"kill {k}: content mismatch"
            residue = [f for f in sink.list('')
                       if not (f.endswith('.parquet'))]
            assert not residue, f"kill {k}: residue {residue}"
            rows_total += n
            print(f"  kill {k:3d}: {status:9s} ({n:7d} rows)", flush=True)
    print(f"\nkill loop: {n_kills} kills, {recovered} recovered, "
          f"{rows_total:,} rows verified exact")
    return 0


def sustained(seconds, rate, group_rows):
    stop = threading.Event()
    problems = []
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, 'soak.parquet')
        w = CheckpointWriter(path, SCHEMA, compression='zstd',
                             checkpoint_every_write=False)

        def reader():
            seen = 0
            reads = 0
            while not stop.is_set():
                try:
                    t = live_table(path)
                except (CheckpointError, SinkError, FileNotFoundError):
                    time.sleep(0.02)
                    continue
                if t.num_rows < seen:
                    problems.append("reader went backwards")
                    return
                seen = t.num_rows
                reads += 1
                time.sleep(0.05)
            print(f"  reader: {reads} live reads, final view {seen:,} rows")

        r = threading.Thread(target=reader)
        r.start()
        i = 0
        ckpt_time = 0.0
        ckpts = 0
        t0 = time.monotonic()
        while time.monotonic() - t0 < seconds:
            w.write_table(make_rows(i, group_rows))
            i += group_rows
            c0 = time.monotonic()
            w.checkpoint()
            ckpt_time += time.monotonic() - c0
            ckpts += 1
            ahead = i / rate - (time.monotonic() - t0)
            if ahead > 0.01:
                time.sleep(ahead)
        stop.set()
        r.join()
        rd = CheckpointReader(ckpt_versions(DirSink(tmp), 'soak.parquet')[-1][1],
                              DirSink(tmp))
        logical = rd.logical_size
        n_chunks = len(rd.manifest)
        w.close()
        elapsed = time.monotonic() - t0
        t = pq.read_table(path)
        assert t.num_rows == i and \
            t.column('n').to_pylist() == list(range(i)), "final mismatch"
        cnt = duckdb.sql(
            f"SELECT count(*) FROM read_parquet('{path}')").fetchall()[0][0]
        assert cnt == i
        print(f"\nsustained: {i:,} rows in {elapsed:.1f}s "
              f"({i / elapsed:,.0f} rows/s target {rate:,.0f})")
        print(f"  checkpoints: {ckpts} "
              f"(avg {1000 * ckpt_time / max(1, ckpts):.1f} ms each)")
        print(f"  logical: {logical / 1e6:.1f} MB in {n_chunks} chunks + "
              f"checkpoint; final: "
              f"{pq.ParquetFile(path).metadata.num_row_groups} row groups, "
              f"ONE file")
        assert not problems, problems[0]
    return 0


def pipeline(seconds, rate, group_rows):
    """The full two-copies pipeline under stress: ring-rate writer with
    tail checkpoints -> local DirSink -> Shipper (random injected S3
    outages) -> FakeS3, with a remote reader hammering the shipped
    checkpoint. Ends with the shutdown protocol; local and remote final
    files must be byte-identical and exact."""
    ckpt_seconds = 0.2
    stop = threading.Event()
    problems = []
    stats = {'reads': 0, 'outage_polls': 0, 'ships': 0}
    with tempfile.TemporaryDirectory() as tmp:
        local = DirSink(tmp)
        remote = FakeS3(part_min=64 * 1024)
        w = CheckpointWriter('pipe.parquet', SCHEMA, sink=local,
                             chunk_bytes=256 * 1024, compression='none',
                             checkpoint_every_write=True)
        ship = Shipper('pipe.parquet', local, remote)
        rng = random.Random()

        def remote_reader():
            seen = 0
            while not stop.is_set():
                v = ckpt_versions(remote, 'pipe.parquet')
                if not v:                      # nothing shipped yet
                    time.sleep(0.02)
                    continue
                try:
                    r = CheckpointReader(v[-1][1], remote)
                except (SinkError, CheckpointError):
                    time.sleep(0.02)           # deleted after the listing
                    continue
                if r.watermark < seen:
                    problems.append(f"remote watermark went backwards "
                                    f"{seen}->{r.watermark}")
                    return
                seen = r.watermark
                t = r.table()
                if (t.num_rows != r.watermark
                        or (t.num_rows and
                            t.column('n').to_pylist()[-1] != t.num_rows - 1)):
                    problems.append(f"remote content wrong at {r.watermark}")
                    return
                stats['reads'] += 1
                time.sleep(0.05)
            print(f"  remote reader: {stats['reads']} exact reads, "
                  f"final view {seen:,} rows")

        rd = threading.Thread(target=remote_reader)
        rd.start()
        i = 0
        pending = []                     # tables committed but not written
        pending_rows = 0
        last_ckpt_mono = time.monotonic()
        last_poll_mono = 0.0
        t0 = time.monotonic()
        while time.monotonic() - t0 < seconds:
            burst = make_rows(i + pending_rows, 200)
            pending.append(burst)
            pending_rows += 200
            if pending_rows >= group_rows:
                w.write_table(pa.concat_tables(pending))
                i += pending_rows
                pending, pending_rows = [], 0
                last_ckpt_mono = time.monotonic()
            elif time.monotonic() - last_ckpt_mono >= ckpt_seconds:
                w.checkpoint(tail=pa.concat_tables(pending)
                             if pending else None)
                last_ckpt_mono = time.monotonic()
            if time.monotonic() - last_poll_mono >= 0.05:
                last_poll_mono = time.monotonic()
                if rng.random() < 0.10:              # WAN flaps
                    remote.fail_next('put:', n=rng.randrange(1, 30))
                try:
                    st = ship.poll()
                    if st['state'] == 'shipped':
                        stats['ships'] += 1
                except FaultInjected:
                    stats['outage_polls'] += 1       # host logs + retries
            ahead = (i + pending_rows) / rate - (time.monotonic() - t0)
            if ahead > 0.005:
                time.sleep(ahead)
        # shutdown protocol: pending -> final, sync, close both sides
        if pending:
            w.write_table(pa.concat_tables(pending))
            i += pending_rows
        remote.clear_faults()
        while not ship.synced:
            try:
                ship.poll()
            except FaultInjected:
                pass
        w.close()
        assert ship.poll()['state'] == 'local-finalized'
        status, _ = ship.close()
        assert status == 'recovered'
        stop.set()
        rd.join()
        elapsed = time.monotonic() - t0
        local_bytes = local.get('pipe.parquet')
        remote_bytes = remote.get('pipe.parquet')
        assert local_bytes == remote_bytes, "local/remote final files differ"
        t = pq.read_table(pa.BufferReader(remote_bytes))
        assert t.num_rows == i and \
            t.column('n').to_pylist() == list(range(i)), "final mismatch"
        assert remote.list('pipe.parquet.chunks/') == []
        assert not ckpt_versions(remote, 'pipe.parquet')
        print(f"\npipeline: {i:,} rows in {elapsed:.1f}s "
              f"({i / elapsed:,.0f} rows/s target {rate:,.0f})")
        print(f"  ships: {stats['ships']}, outage polls ridden: "
              f"{stats['outage_polls']}, remote reads: {stats['reads']}")
        print(f"  final: {len(local_bytes) / 1e6:.1f} MB, byte-identical "
              f"local == remote")
        assert not problems, problems[0]
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--kills', type=int, default=0)
    ap.add_argument('--seconds', type=float, default=0)
    ap.add_argument('--pipeline', type=float, default=0,
                    help='seconds of full writer->shipper->S3 soak')
    ap.add_argument('--rate', type=float, default=20000.0)
    ap.add_argument('--group-rows', type=int, default=2000)
    args = ap.parse_args()
    rc = 0
    if args.kills:
        rc |= kill_loop(args.kills)
    if args.seconds:
        rc |= sustained(args.seconds, args.rate, args.group_rows)
    if args.pipeline:
        rc |= pipeline(args.pipeline, args.rate, args.group_rows)
    if not args.kills and not args.seconds and not args.pipeline:
        rc |= kill_loop(10)
        rc |= sustained(15, args.rate, args.group_rows)
        rc |= pipeline(15, args.rate, args.group_rows)
    return rc


if __name__ == '__main__':
    sys.exit(main())
