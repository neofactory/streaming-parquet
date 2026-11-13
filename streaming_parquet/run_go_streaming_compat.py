"""Go <-> Python cross-compatibility suite for streaming_parquet.

Run:  python3 packages/streaming_parquet/run_go_streaming_compat.py

Full-stack, hermetic: a scratch file root carries the tickets
(<root>/streams/<name>.json); real Flight carries the batches; both
directions cross the
language boundary:

  - Go publisher (stream-compat serve) -> Python attach() reads the
    live edge exactly
  - Python publisher -> Go attach reads exactly AND ARCHIVES to
    parquet while attached (the symmetric-archival proof); the file
    it finalizes is then read back here with pyarrow, exactly
"""

import atexit
import shutil
import os
import subprocess
import sys
import tempfile
import time
import traceback

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PKG = os.path.dirname(os.path.abspath(__file__))
GO = os.path.join(PKG, 'go')

# ---- hermetic discovery root ------------------------------------------------
ROOT = tempfile.mkdtemp(prefix='sp-compat-root-')
atexit.register(lambda: shutil.rmtree(ROOT, ignore_errors=True))

import streaming_parquet as sp

# The Go package compiles checked-in copies of the stream core (the Go
# build cache needs in-package files). Refresh them; the last test
# fails if they differ.
import filecmp
CORE_COPIES = [(os.path.join(PKG, 'core', 'sp.c'), os.path.join(GO, 'streaming', 'sp_core.c')),
               (os.path.join(PKG, 'core', 'sp.h'), os.path.join(GO, 'streaming', 'sp.h'))]
for src, dst in CORE_COPIES:
    if not os.path.exists(dst) or not filecmp.cmp(src, dst, shallow=False):
        shutil.copyfile(src, dst)

BIN = os.path.join(tempfile.gettempdir(), 'stream-compat-bin')
r = subprocess.run(['go', 'build', '-o', BIN, './cmd/stream-compat'],
                   cwd=GO, capture_output=True, text=True)
if r.returncode != 0:
    print(r.stdout + r.stderr)
    sys.exit(1)

F = pa.field
SCHEMA = pa.schema([F('n', pa.int64(), nullable=False),
                    F('x', pa.float64(), nullable=False),
                    F('s', pa.string(), nullable=False)])

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


@test("Go publisher -> Python attach: live edge exact, root discovery")
def _():
    child = subprocess.Popen(
        [BIN, 'serve', ROOT, 'x/gopub', '3000', '4'],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        a = sp.connect('x/gopub', store=ROOT)
        deadline = time.time() + 20
        while time.time() < deadline:
            if a.state == 'streaming' and a._newest_seq >= 0:
                break
            time.sleep(0.05)
        # wait for the stream's tail
        while time.time() < deadline:
            row = None
            try:
                row = a._read_row(a._newest_seq)
            except Exception:
                pass
            if row is not None and row.n == 2999:
                break
            time.sleep(0.05)
        assert row is not None and row.n == 2999, \
            f"tail row {None if row is None else row.n}"
        # contiguity + content across the received window
        prev = None
        for seq in range(a._oldest_seq, a._newest_seq + 1):
            r = a._read_row(seq)
            if prev is not None:
                assert r.n == prev + 1, f"gap at {seq}: {prev}->{r.n}"
            prev = r.n
        assert a._read_row(a._newest_seq).s == 'row-2999'
        a.close()
    finally:
        child.wait(timeout=30)
        assert child.returncode == 0, child.stderr.read()


@test("Python publisher -> Go attach + Go archives parquet while attached")
def _():
    with tempfile.TemporaryDirectory() as tmp:
        s = sp.create_stream('x/pypub', SCHEMA, store=ROOT, memory=sp.MemoryOptions(lookback=8000))
        child = subprocess.Popen(
            [BIN, 'attach', ROOT, 'x/pypub', '2500', tmp, 'XLNG'],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            time.sleep(2.0)          # discovery + connect + arm parquet
            i = 0
            deadline = time.time() + 30
            while i < 3000 and time.time() < deadline:
                for _ in range(100):
                    s.append({'n': i, 'x': i * 0.5, 's': f'row-{i}'})
                    i += 1
                time.sleep(0.02)
                if child.poll() is not None:
                    break
            out, err = child.communicate(timeout=40)
            assert child.returncode == 0, err
            parts = out.split()
            first, last = int(parts[0]), int(parts[1])
            assert last >= 2499, (first, last)
            files = [p[len('file='):] for p in parts if p.startswith('file=')]
            assert files, f"Go attach produced no parquet file: {out!r}"
            t = pq.read_table(files[0])
            n = t.column('n').to_pylist()
            assert len(n) >= 1000, f"only {len(n)} rows archived"
            assert n == list(range(n[0], n[0] + len(n))), "gap in archive"
            assert t.column('s').to_pylist()[-1] == f'row-{n[-1]}'
            assert 'XLNG' in files[0]
        finally:
            if child.poll() is None:
                child.kill()
        s.close()



RICH = pa.schema([F('n', pa.int64(), nullable=False),
                  F('f', pa.float64(), nullable=True),
                  F('s', pa.string(), nullable=True),
                  F('t', pa.timestamp('us'), nullable=False),
                  F('tags', pa.list_(pa.int64()), nullable=False)])


def rich_row(n):
    import math
    f = None if n % 5 == 0 else (math.nan if n % 7 == 3 else n * 0.5)
    s = None if n % 4 == 0 else f"u\u2713{n}"
    return {'n': n, 'f': f, 's': s, 't': n * 1_000_000,
            'tags': [n] * (n % 3)}


@test("type matrix Go -> Python: nulls, NaN, unicode, ts, lists over the wire")
def _():
    import math
    child = subprocess.Popen(
        [BIN, 'serve-rich', ROOT, 'x/rich-go', '1500', '5'],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        a = sp.connect('x/rich-go', store=ROOT)
        deadline = time.time() + 25
        while time.time() < deadline:
            if a.state == 'streaming' and a._newest_seq >= 1499:
                break
            time.sleep(0.05)
        assert a._newest_seq >= 1499, (a.state, a._newest_seq)
        with a._ring.pin(0, a._ring.newest_seq + 1) as rs:   # every row
            t = rs.table
            lo = t.column('n').to_pylist()[0]
            for i, n in enumerate(t.column('n').to_pylist()):
                exp = rich_row(n)
                f = t.column('f')[i].as_py()
                if exp['f'] is None:
                    assert f is None, (n, f)
                elif isinstance(exp['f'], float) and math.isnan(exp['f']):
                    assert f is not None and math.isnan(f), (n, f)
                else:
                    assert f == exp['f'], (n, f)
                assert t.column('s')[i].as_py() == exp['s'], n
                assert t.column('t')[i].value == exp['t'], n
                assert t.column('tags')[i].as_py() == exp['tags'], n
        a.close()
    finally:
        child.wait(timeout=30)
        assert child.returncode == 0, child.stderr.read()


@test("type matrix Python -> Go: the same values verified by Go getters")
def _():
    s = sp.create_stream('x/rich-py', RICH, store=ROOT,
                                memory=sp.MemoryOptions(lookback=20000))
    child = subprocess.Popen(
        [BIN, 'attach-rich', ROOT, 'x/rich-py', '1200'],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        time.sleep(2.0)
        rows = [rich_row(n) for n in range(1500)]
        t = pa.table({k: [r[k] for r in rows] for k in
                      ('n', 'f', 's', 't', 'tags')}, schema=RICH)
        i = 0
        deadline = time.time() + 30
        while i < 1500 and time.time() < deadline:
            s.append(t.slice(i, 100).to_batches()[0])
            i += 100
            time.sleep(0.05)
            if child.poll() is not None:
                break
        out, err = child.communicate(timeout=40)
        assert child.returncode == 0, err
        lo, hi, nulls, nans = map(int, out.split())
        assert hi >= 1199, (lo, hi)
        n_range = range(lo, hi + 1)
        assert nulls == sum(1 for n in n_range if n % 5 == 0), (nulls,)
        assert nans == sum(1 for n in n_range
                           if n % 5 != 0 and n % 7 == 3), (nans,)
    finally:
        if child.poll() is None:
            child.kill()
    s.close()

@test("a Go writer's ticket is the new ticket: the core reads it with its members")
def _():
    from streaming_parquet import _core
    child = subprocess.Popen(
        [BIN, 'serve', ROOT, 'x/goticket', '200', '3'],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        path = os.path.join(ROOT, 'tickets', 'x', 'goticket.json')
        deadline = time.time() + 20
        while not os.path.exists(path) and time.time() < deadline:
            time.sleep(0.05)
        text = open(path).read()
        t = _core.ticket_from_text(text)
        assert t["name"] == 'x/goticket' and t["port"] > 0 and t["run"], t
        assert t["beat_seconds"] == 60 and t["stale_after"] == 180, t
        assert t["revision"] == 1, t
        assert t["files"] == [] and t["unfinished"] == [], t
        assert t["stored_row"] is None, t      # Go does not say yet
        # and it is the text the core writes: Python's core writes the same
        assert _core.ticket_text(t) == text, (_core.ticket_text(t), text)
        assert not _core.ticket_is_stale(t, t["ts"] + 179, 30.0)
        assert _core.ticket_is_stale(t, t["ts"] + 180, 30.0)
    finally:
        child.wait(timeout=30)
        assert child.returncode == 0, child.stderr.read()


@test("the Go package's copies of the stream core are the core")
def _():
    for src, dst in CORE_COPIES:
        assert filecmp.cmp(src, dst, shallow=False), f"{dst} differs from core/"


sp.close()
print(f"\n{'=' * 50}")
print(f"Results: {passed} passed, {failed} failed")
if errors:
    print(f"Failed: {', '.join(errors)}")
    sys.exit(1)
print("All tests passed!")
