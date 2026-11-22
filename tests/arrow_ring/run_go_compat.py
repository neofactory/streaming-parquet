"""Go <-> Python cross-compatibility suite for arrow_ring.

Run:  python3 packages/arrow_ring/run_go_compat.py

The ring is in-process, so the cross-language boundary is Arrow IPC:
the Go host's record sets, written as IPC, must read exactly in
pyarrow; pyarrow-written IPC must bulk-ingest into a Go ring and read
exactly through the row getters (incl. struct leaves and strings).

Also syncs the checked-in copies of core/arrow_ring.{c,h} into
go/ring/ and fails if they drift.
"""

import filecmp
import os
import shutil
import subprocess
import sys
import tempfile
import traceback

import pyarrow as pa
import pyarrow.ipc

PKG = os.path.dirname(os.path.abspath(__file__))
GO = os.path.join(PKG, 'go')

for f in ('arrow_ring.c', 'arrow_ring.h'):
    src = os.path.join(PKG, 'core', f)
    dst = os.path.join(GO, 'ring',
                       'arrow_ring_core.c' if f.endswith('.c') else f)
    if not filecmp.cmp(src, dst, shallow=False):
        shutil.copyfile(src, dst)
        print(f"  synced {dst} from core/")

BIN = os.path.join(tempfile.gettempdir(), 'ring-compat-bin')
r = subprocess.run(['go', 'build', '-o', BIN, './cmd/ring-compat'],
                   cwd=GO, capture_output=True, text=True)
if r.returncode != 0:
    print(r.stdout + r.stderr)
    sys.exit(1)


def go(*args):
    r = subprocess.run([BIN, *args], capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"ring-compat {args}: {r.stderr.strip()}")
    return r.stdout.strip()


F = pa.field
# all non-nullable: Go arrow.Field defaults to non-nullable, and the
# ring requires structs themselves non-nullable
SCHEMA = pa.schema([
    F('n', pa.int64(), nullable=False),
    F('x', pa.float64(), nullable=False),
    F('s', pa.string(), nullable=False),
    F('servo', pa.struct([F('pos', pa.int64(), nullable=False),
                          F('vel', pa.int64(), nullable=False)]),
      nullable=False),
])

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


@test("Go ring -> record set -> IPC -> pyarrow reads exactly")
def _():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, 'go.arrows')
        go('write-ipc', path, '5000')
        with pa.ipc.open_stream(path) as rd:
            t = rd.read_all()
        assert t.num_rows == 5000
        assert t.schema.equals(SCHEMA, check_metadata=False), t.schema
        assert t.column('n').to_pylist() == list(range(5000))
        assert t.column('x').to_pylist()[3] == 1.5
        assert t.column('s').to_pylist()[4999] == 'row-4999'
        servo = t.column('servo').to_pylist()
        assert servo[10] == {'pos': 100, 'vel': -10}


@test("pyarrow IPC -> Go ring AppendRecord -> exact row getters")
def _():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, 'py.arrows')
        n = 3000
        t = pa.table({
            'n': list(range(n)),
            'x': [i * 0.5 for i in range(n)],
            's': [f'row-{i}' for i in range(n)],
            'servo': [{'pos': i * 10, 'vel': -i} for i in range(n)],
        }, schema=SCHEMA)
        with pa.ipc.new_stream(path, SCHEMA) as w:
            # several batches: exercises repeated bulk ingest
            for b in t.to_batches(max_chunksize=700):
                w.write_batch(b)
        rows, s = go('read-ipc', path).split()
        assert (int(rows), int(s)) == (n, n * (n - 1) // 2), (rows, s)


@test("round trip: Go IPC -> pyarrow -> IPC -> Go, both exact")
def _():
    with tempfile.TemporaryDirectory() as tmp:
        p1 = os.path.join(tmp, 'a.arrows')
        p2 = os.path.join(tmp, 'b.arrows')
        go('write-ipc', p1, '1200')
        with pa.ipc.open_stream(p1) as rd:
            t = rd.read_all()
        with pa.ipc.new_stream(p2, t.schema) as w:
            for b in t.to_batches():
                w.write_batch(b)
        rows, s = go('read-ipc', p2).split()
        assert (int(rows), int(s)) == (1200, 1200 * 1199 // 2), (rows, s)


for f in ('arrow_ring.c', 'arrow_ring.h'):
    src = os.path.join(PKG, 'core', f)
    dst = os.path.join(GO, 'ring',
                       'arrow_ring_core.c' if f.endswith('.c') else f)
    assert filecmp.cmp(src, dst, shallow=False), f"{dst} drifted from core/"

print(f"\n{'=' * 50}")
print(f"Results: {passed} passed, {failed} failed")
if errors:
    print(f"Failed: {', '.join(errors)}")
    sys.exit(1)
print("All tests passed!")
