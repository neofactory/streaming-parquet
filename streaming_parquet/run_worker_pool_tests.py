"""Regression: promoted worker tasks must release detached Arrow ring pins.

Run: python3 streaming_parquet/run_worker_pool_tests.py
Use --worker-module PATH to check another version of _worker_pool.py.
Only the worker module is loaded; no streaming service or network is started.
"""

import argparse
import gc
import importlib.util
import logging
from pathlib import Path
import sys
import threading
import time
import traceback
import weakref

import pyarrow as pa

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from arrow_ring import PinStalledError, Ring


def load_worker_module(path):
    spec = importlib.util.spec_from_file_location("worker_pool_regression", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    # Expected exception cases should not configure telemetry's logger/files.
    module._diag = logging.getLogger("worker_pool_regression")
    module._diag.addHandler(logging.NullHandler())
    module._diag.propagate = False
    return module


def drain(pool):
    deadline = time.monotonic() + 3.0
    while not pool.drain(timeout=0.01):
        assert time.monotonic() < deadline, "worker pool failed to drain"


def enqueue_payload(pool, table, capture, observations, fail):
    """Return without retaining the task or its captured table in the caller."""
    def consume(payload):
        observations.append(payload.column("n").to_pylist())
        if fail:
            raise RuntimeError("intentional task/callback failure")

    if capture == "args":
        pool.submit("flight", "pinned", consume, table)
    elif capture == "default":
        pool.submit("flight", "pinned", lambda payload=table: consume(payload))
    elif capture == "closure":
        pool.submit("flight", "pinned", lambda: consume(table))
    elif capture == "on_done":
        pool.submit("flight", "pinned", lambda: None,
                    on_done=lambda payload=table: consume(payload))
    else:
        raise AssertionError(capture)


def promoted_task_releases_pin(module, workers, capture, fail=False):
    ring = Ring(pa.schema([pa.field("n", pa.int64(), nullable=False)]),
                capacity=16)
    for value in range(16):
        ring.get_mutable_row().n = value
    ring.commit()
    relation = ring.get_static_relation()
    assert (relation.lo, relation.hi) == (0, 16)
    table = relation.detach()
    table_ref = weakref.ref(table)
    del relation

    pool = module.WorkerPool(num_workers=workers)
    started = threading.Event()
    release = threading.Event()
    blocker_errors = []
    observations = []
    barrier = threading.Barrier(workers + 1, timeout=3.0)
    ordinary_workers = []

    def blocker():
        started.set()
        if not release.wait(3.0):
            blocker_errors.append("blocker timed out")

    def ordinary():
        ordinary_workers.append(threading.get_ident())
        barrier.wait()

    try:
        pool.submit("flight", "pinned", blocker)
        assert started.wait(3.0), "first task did not start"
        # The slot is busy until release, so this task MUST be promoted.
        enqueue_payload(pool, table, capture, observations, fail)
        del table
        try:
            ring.get_mutable_row()
        except PinStalledError:
            pass
        else:
            raise AssertionError("queued task did not retain its ring pin")

        release.set()
        drain(pool)
        assert not blocker_errors, blocker_errors
        assert observations == [list(range(16))], observations
        gc.collect()
        released_after_drain = table_ref() is None

        # Every worker now executes ordinary, independently dispatchable work.
        # Such work must not be needed to release a completed task, and it
        # does not overwrite the old next_task local in the broken version.
        for index in range(workers):
            pool.submit("flight", f"ordinary-{index}", ordinary)
        barrier.wait()
        drain(pool)
        assert len(set(ordinary_workers)) == workers, ordinary_workers

        gc.collect()
        try:
            row = ring.get_mutable_row()
        except PinStalledError as error:
            raise AssertionError(
                "completed promoted task still pins the ring after drain "
                "and ordinary subsequent work (before shutdown)"
            ) from error
        row.n = 16
        ring.commit()
        assert ring.read_row(16).n == 16
        assert released_after_drain, "drained task retained its captured table"
    finally:
        release.set()
        barrier.abort()
        pool.shutdown(timeout=1.0)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker-module", type=Path,
                        default=Path(__file__).with_name("_worker_pool.py"))
    args = parser.parse_args()
    module = load_worker_module(args.worker_module)
    cases = [(workers, capture, False)
             for workers in (1, 4)
             for capture in ("args", "default", "closure", "on_done")]
    cases.extend([(1, "args", True), (4, "on_done", True)])
    failed = 0
    for workers, capture, fail in cases:
        label = f"workers={workers} capture={capture} exception={fail}"
        try:
            promoted_task_releases_pin(module, workers, capture, fail)
        except Exception:
            failed += 1
            print(f"FAIL {label}", flush=True)
            traceback.print_exc()
        else:
            print(f"PASS {label}", flush=True)
    print(f"{len(cases) - failed}/{len(cases)} worker pin regressions passed",
          flush=True)
    return int(bool(failed))


if __name__ == "__main__":
    raise SystemExit(main())
