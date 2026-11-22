"""Go <-> Python cross-compatibility suite for parquet_checkpoint.

Run:  python3 packages/parquet_checkpoint/run_go_compat.py

The Go host (go/pqc, cgo over the same C core) and the Python package
must produce artifacts the OTHER side can read and finalize:

  - Go writer crashes (chunks + checkpoint with a provisional tail):
    Python live-reads the checkpoint, finalizes, reads exactly
  - Python writer crashes: Go finalizes and reads exactly
  - each side reads the other's FINISHED files

Also syncs the checked-in copies of src/core/pqc.{c,h} into go/pqc/ and
fails if they drift (the Go build cache needs in-package files).
"""

import filecmp
import os
import shutil
import subprocess
import sys
import tempfile
import traceback

import pyarrow as pa
import pyarrow.parquet as pq

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path[:0] = [os.path.join(REPO, "src", "python"),
                os.path.join(REPO, "tests")]

from parquet_checkpoint import (ckpt_versions,
                                CheckpointReader, CheckpointWriter, DirSink,
                                finalize, live_table)

GO = os.path.join(REPO, 'src', 'go')

# The Go packages compile checked-in copies of the C cores
# (src/go/sync_core.py). Refresh them; the run fails at its end if one
# still differs.
sys.path.insert(0, GO)
import sync_core  # noqa: E402
for _dst in sync_core.sync():
    print(f"  copied {_dst} from src/core")

BIN = os.path.join(tempfile.gettempdir(), 'pqc-compat-bin')
r = subprocess.run(['go', 'build', '-o', BIN, './cmd/pqc-compat'],
                   cwd=GO, capture_output=True, text=True)
if r.returncode != 0:
    print(r.stdout + r.stderr)
    sys.exit(1)


def go(*args):
    r = subprocess.run([BIN, *args], capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"pqc-compat {args}: {r.stderr.strip()}")
    return r.stdout.strip()


F = pa.field
SCHEMA = pa.schema([F('n', pa.int64(), nullable=False),
                    F('x', pa.float64(), nullable=False),
                    F('s', pa.string(), nullable=False)])


def make_rows(start, count):
    return pa.table({'n': list(range(start, start + count)),
                     'x': [i * 0.5 for i in range(start, start + count)],
                     's': [f'row-{i}' for i in range(start, start + count)]},
                    schema=SCHEMA)


def check_exact(table, expect):
    assert table.num_rows == expect, (table.num_rows, expect)
    assert table.column('n').to_pylist() == list(range(expect))
    assert table.column('s').to_pylist()[-1] == f'row-{expect - 1}'


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


@test("Go crashed writer -> Python live read, finalize, exact")
def _():
    with tempfile.TemporaryDirectory() as tmp:
        go('write', tmp, 'g.parquet', '4040')      # 8x500 final + 40 tail
        sink = DirSink(tmp)
        assert ckpt_versions(sink, 'g.parquet')
        assert len(sink.list('g.parquet.chunks/')) >= 2
        r = CheckpointReader(ckpt_versions(sink, 'g.parquet')[-1][1], sink)
        assert r.num_rows == 4040
        check_exact(r.table(), 4040)               # live, tail included
        status, _ = finalize('g.parquet', sink)
        assert status == 'recovered'
        t = pq.read_table(os.path.join(tmp, 'g.parquet'))
        check_exact(t, 4040)
        assert [f for f in sink.list('') if f != 'g.parquet'] == []


@test("Python crashed writer -> Go finalize, Go exact read")
def _():
    with tempfile.TemporaryDirectory() as tmp:
        sink = DirSink(tmp)
        w = CheckpointWriter('p.parquet', SCHEMA, sink=sink,
                             chunk_bytes=32 * 1024, compression='none')
        for k in range(5):
            w.write_table(make_rows(k * 600, 600))
        w.checkpoint(tail=make_rows(3000, 25))
        del w                                       # crash
        assert go('finalize', tmp, 'p.parquet') == 'recovered'
        n, s = go('read', tmp, 'p.parquet').split()
        total = 3025
        assert (int(n), int(s)) == (total, total * (total - 1) // 2), (n, s)
        assert [f for f in sink.list('') if f != 'p.parquet'] == []


@test("Go live checkpoint read by Go; Python live checkpoint read by Go")
def _():
    with tempfile.TemporaryDirectory() as tmp:
        go('write', tmp, 'g.parquet', '1040')
        n, s = go('read', tmp, 'g.parquet').split()  # composes the ckpt
        assert (int(n), int(s)) == (1040, 1040 * 1039 // 2), (n, s)
        sink = DirSink(tmp)
        w = CheckpointWriter('p.parquet', SCHEMA, sink=sink,
                             compression='none')
        w.write_table(make_rows(0, 500))
        n, s = go('read', tmp, 'p.parquet').split()
        assert (int(n), int(s)) == (500, 500 * 499 // 2), (n, s)
        w.close()
        n, s = go('read', tmp, 'p.parquet').split()  # finished file too
        assert (int(n), int(s)) == (500, 500 * 499 // 2), (n, s)


@test("Go finished file: pyarrow reads, DuckDB queries, stats present")
def _():
    import duckdb
    with tempfile.TemporaryDirectory() as tmp:
        go('write', tmp, 'g.parquet', '2040')
        finalize('g.parquet', DirSink(tmp))
        path = os.path.join(tmp, 'g.parquet')
        t = pq.read_table(path)
        check_exact(t, 2040)
        n, mx = duckdb.sql(f"SELECT count(*), max(n) FROM "
                           f"read_parquet('{path}')").fetchall()[0]
        assert (n, mx) == (2040, 2039)
        md = pq.read_metadata(path)
        assert md.num_row_groups >= 4
        st = md.row_group(0).column(0).statistics
        assert st is not None and st.min == 0


assert not sync_core.differing(), sync_core.differing()

print(f"\n{'=' * 50}")
print(f"Results: {passed} passed, {failed} failed")
if errors:
    print(f"Failed: {', '.join(errors)}")
    sys.exit(1)
print("All tests passed!")
