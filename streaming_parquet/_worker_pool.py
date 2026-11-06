"""Process-wide worker pool for telemetry streams.

Workers are interchangeable threads that execute Flight push tasks
(small, frequent, latency-sensitive), Parquet write tasks (large,
infrequent, throughput-bound), or ship tasks (the S3 mirror: bounded by
network timeouts, never by the local disk). Per-stream serialization
keeps Flight batches in order, prevents concurrent parquet writes to the
same file, and keeps one mirror pass at a time, while still allowing the
three kinds for the same stream to run in parallel on different worker
threads.
"""

import logging
import queue
import threading

_diag = logging.getLogger("telemetry.diag")


class _StreamSlots:
    """Per-stream serialization state: the set of task kinds in flight
    ('flight', 'parquet', 'ship'). At most one task of each kind may be
    in flight for a given stream at a time. Held under the pool's lock."""

    __slots__ = ("busy",)

    def __init__(self):
        self.busy = set()


class Task:
    __slots__ = ("kind", "stream_name", "fn", "args", "on_done")

    def __init__(self, kind, stream_name, fn, args, on_done):
        self.kind = kind                 # "flight" | "parquet" | "ship"
        self.stream_name = stream_name
        self.fn = fn                     # callable doing the work
        self.args = args                 # tuple of args for fn
        self.on_done = on_done           # callable() invoked after fn returns

    def run(self):
        try:
            self.fn(*self.args)
        except Exception as e:
            _diag.error(f"[pool] task {self.kind} for {self.stream_name} failed: {e}")
            import traceback
            _diag.error(traceback.format_exc())
        finally:
            try:
                if self.on_done is not None:
                    self.on_done()
            except Exception as e:
                _diag.error(f"[pool] on_done for {self.kind}/{self.stream_name} failed: {e}")


class WorkerPool:
    """Shared pool with per-stream task-type serialization.

    Submission is non-blocking: tasks for streams whose slot is busy are
    parked on a per-stream pending queue and dispatched when the slot
    becomes free. This keeps Flight batches in FIFO order per stream
    while allowing parallel work across streams (and across task types
    within one stream).
    """

    def __init__(self, num_workers=4):
        self._num_workers = num_workers
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._stop = False
        self._streams = {}                       # name -> _StreamSlots
        self._pending = {}                       # (kind, stream_name) -> [Task, ...]
        self._ready = []                         # [Task, ...] dispatchable now
        self._workers = []
        for _ in range(num_workers):
            t = threading.Thread(target=self._worker_loop, daemon=True)
            t.start()
            self._workers.append(t)

    def submit(self, kind, stream_name, fn, *args, on_done=None):
        """Submit a task. Returns immediately. Non-blocking even if the
        stream's slot is busy (task parks on the per-stream pending list)."""
        self.submit_task(Task(kind, stream_name, fn, args, on_done))

    def submit_task(self, task):
        """Submit a prebuilt task: a stream that has at most one task of
        a kind outstanding (the Flight push) builds it once and resubmits
        the same object, so a commit allocates nothing."""
        kind, stream_name = task.kind, task.stream_name
        with self._cond:
            slots = self._streams.get(stream_name)
            if slots is None:
                slots = _StreamSlots()
                self._streams[stream_name] = slots
            if kind in slots.busy:
                self._pending.setdefault((kind, stream_name), []).append(task)
            else:
                slots.busy.add(kind)
                self._ready.append(task)
                self._cond.notify()

    def _worker_loop(self):
        while True:
            with self._cond:
                while not self._ready and not self._stop:
                    self._cond.wait()
                if self._stop and not self._ready:
                    return
                task = self._ready.pop(0)
            task.run()
            # Drop the closure NOW: an idle worker's local would
            # otherwise reference the finished task (and any detached
            # record-set pin riding its args) until the next task
            # arrives -- on a quiet stream, forever. Found by the
            # both-layers jam test: publish's 2s-lease pin outlived
            # its lease with nothing draining it.
            kind, stream_name = task.kind, task.stream_name
            task = None
            # Mark slot free; promote a parked task if any.
            with self._cond:
                slots = self._streams.get(stream_name)
                queue_key = (kind, stream_name)
                parked = self._pending.get(queue_key)
                if parked:
                    # Do not retain the promoted task in a worker local:
                    # its buffers must be freed when its execution ends.
                    self._ready.append(parked.pop(0))
                    if not parked:
                        del self._pending[queue_key]
                    self._cond.notify()
                else:
                    if slots is not None:
                        slots.busy.discard(kind)

    def drain(self, timeout=None):
        """Block until no tasks are queued or running for any stream.
        Returns True if drained, False if timeout elapsed."""
        with self._cond:
            deadline = None
            import time as _time
            if timeout is not None:
                deadline = _time.monotonic() + timeout
            while True:
                # Drained when no ready tasks AND no slot is busy AND no parked tasks
                ready = bool(self._ready)
                busy = any(s.busy for s in self._streams.values())
                pending = bool(self._pending)
                if not ready and not busy and not pending:
                    return True
                if deadline is None:
                    self._cond.wait(timeout=0.1)
                else:
                    remaining = deadline - _time.monotonic()
                    if remaining <= 0:
                        return False
                    self._cond.wait(timeout=remaining)

    def shutdown(self, timeout=2.0):
        """Drain pending work, signal workers to stop, join."""
        self.drain(timeout=timeout)
        with self._cond:
            self._stop = True
            self._cond.notify_all()
        for t in self._workers:
            t.join(timeout=timeout)


_pool_lock = threading.Lock()
_pool = None


def get_pool(num_workers=4):
    global _pool
    with _pool_lock:
        if _pool is None:
            _pool = WorkerPool(num_workers=num_workers)
        return _pool


def shutdown_pool():
    global _pool
    with _pool_lock:
        if _pool is not None:
            _pool.shutdown()
            _pool = None
