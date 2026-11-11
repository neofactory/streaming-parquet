"""Stream: a local publishing stream -- an arrow_ring Ring with a name,
a ticket heartbeat in the discovery root, Flight fan-out, and parquet
archival.

The writer is only for very fast, low-latency operations. It fills rows
with get_mutable_row, from_wire or append. commit() triggers worker
threads inside the library to forward the data, write the files, and
do everything slow; cut() triggers them too. A reader, in any thread,
calls one of the reader verbs (the Stream reads the same way an
Attached does), and it all just works.

How commit triggers the work (_publish): compare the committed count
with three cursors and submit at most one task per kind to the worker
pool -- the Flight push, the parquet write or tail checkpoint, the
mirror pass. The push task sends everything from the Flight cursor to
the committed count at the moment it runs, and commit submits it only
when none is pending, so a slow sink gets fewer, larger batches and a
fast sink gets a row per push, with nothing configured. The time-driven
work on a quiet stream (the tail checkpoint, the mirror retry, the
ticket beat) runs on the stream's heartbeat thread, which wakes on the
shortest of its cadences. Slow operations never run on a caller's
thread.
"""

import threading
import time

import pyarrow as pa

import parquet_checkpoint

from ._diag import _diag, console_print
from ._options import MemoryOptions, ParquetOptions, ArchiveState, MirrorState, MemoryState
from ._stream_data import _StreamData
from ._worker_pool import get_pool
from . import _tickets

import socket


def _get_local_ip():
    """Best-effort local IP for the ticket (no traffic sent)."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
        finally:
            s.close()
    except Exception:
        return "127.0.0.1"


# cut(label=KEEP_LABEL): the next file keeps the current label.
KEEP_LABEL = object()

#: A file is cut when its row groups reach this many bytes, whenever the
#: host cut it last. Every decision of the writer is taken by time or
#: by bytes, whichever comes first; the host cuts by time (cut()), and
#: a fast stream reaches this first. The reason is the footer: it has
#: an entry for each row group and each column, and it is written at
#: every checkpoint. At 10,000 rows a second a file left to grow for 3
#: hours has 879 row groups and a footer of 4 MB.
FILE_CUT_BYTES = 256_000_000


class Stream:
    """A local publishing stream. Create via streaming_parquet.create_stream()."""

    #: the heartbeat thread never sleeps longer than this between its
    #: time-due checks, whatever the cadences say
    HEARTBEAT_MAX_WAIT_S = 60.0

    def __init__(self, name, schema, flight_server, port, run,
                 memory=None, parquet=None, store=None, writer=None,
                 owner=None):
        """`run`: this writer's start time (_clock.run_start()), the same
        value the writer records in its files. `writer`: the
        ParquetWriter the caller built for this stream's store and
        spool, or None for a stream that keeps no files. `owner`: the
        held ownership (_owner.claim), released at close."""
        memory = memory or MemoryOptions()
        self.name = name
        self.schema = schema
        self._memory = memory
        self._archive = parquet
        self._store = store
        row_group_rows = parquet.row_group_rows if parquet else None
        ring_rows, row_group_rows = _ring_sizes(name, memory.lookback,
                                                row_group_rows)
        self._data = _StreamData(name, schema, capacity=ring_rows,
                                 segment_bytes=memory.segment_bytes)
        self._ring = self._data.ring
        self._flight_server = flight_server
        self._port = port
        self._ticket_root = store
        # The run: this writer's start time, one per publisher
        # incarnation. A subscriber that sees it change knows the seq
        # axis restarted (publisher respawn) and resyncs instead of
        # livelocking on the regression; the files record it as sp_run.
        self._run = run
        self._owner = owner
        # One lock over the cursors and flags that commit's _publish,
        # the push task, start_parquet and stop_parquet all touch: the
        # writer's thread, the pool and a host thread may call them.
        self._lock = threading.Lock()

        a = parquet or ParquetOptions()
        self._parquet = writer              # None: no files, ever
        self._recording = writer is not None
        # The archive's standing: the first copy's last failure, and rows
        # the archive never got (recycled before their flush). A failing
        # first copy means rows are not durable anywhere but the ring.
        self._archive_failing = False
        self._archive_error = None
        self._archive_lost_rows = 0
        # The S3 mirror: every ship_seconds, while a file is live or
        # pending, one mirror pass goes to the ship lane.
        self._remote = writer._remote if writer is not None else None
        self._ship_seconds = a.ship_seconds
        self._ship_mono = None          # last mirror pass submitted
        self._ship_queued = False       # one pass queued at a time
        self._ship_failing = False      # for transition logging
        self._ship_error = None         # the last failure's text

        # The archive cursor (seq space). Flight readers keep their own:
        # they pull (see _flight_server).
        self._pq_cursor = 0
        self._pq_durable = 0            # rows covered by the last flush
                                        # or tail checkpoint
        self._pq_open_mono = None       # when the pending region started
        self._row_group_rows = row_group_rows
        self._row_group_seconds = a.row_group_seconds
        # Tail-checkpoint cadence: between row-group flushes, the
        # pending region rides the checkpoint as a provisional row
        # group. Crash loss = checkpoint_seconds, independent of the
        # row-group cadence. None = durability per flush only.
        self._checkpoint_seconds = a.checkpoint_seconds
        self._ckpt_mono = None          # last durability point

        # The ticket's beat: the longest gap between ticket writes, the
        # cadence at which the store's data changes -- the ship interval
        # for a bucket behind a spool, else the checkpoint interval (the
        # ship interval when there is none). The heartbeat writes the
        # ticket when the stored state changes, and at least once a beat.
        if self._remote is None and a.checkpoint_seconds is not None:
            self._beat_seconds = a.checkpoint_seconds
        else:
            self._beat_seconds = a.ship_seconds
        # When a reader may call this writer dead: the one rule (three
        # beats, at least 30 seconds), written into the ticket.
        self._stale_after = parquet_checkpoint.dead_after(self._beat_seconds)

        self._pool = get_pool()
        self._closed = False
        self._first_write_logged = False

        # The reader face: a Stream reads the same way an Attached does
        # (its archive views get their ticket from this writer, in
        # memory, and are set up once the ticket is: _view_source).
        from ._attach import Attached
        self._face = Attached(data=self._data, feeder=None, on_close=None,
                              archive_dir=self._archive_location,
                              unarchived=self._rows_unarchived)

        # The heartbeat thread: the ticket beat, and the time-due work on
        # a stream committing nothing (the tail checkpoint, the mirror
        # retry). Always runs; the ticket part only with a root.
        if self._ticket_root is not None:
            console_print(f"[streaming-parquet] Ticket heartbeat starting: "
                          f"{self._ticket_root}/tickets/{self.name} "
                          f"(on each change of the stored data, at least "
                          f"every {self._beat_seconds:g}s; stale after "
                          f"{self._stale_after:g}s; host={_get_local_ip()}, "
                          f"port={self._port})")
        else:
            console_print("[streaming-parquet] No store given "
                          "(store= / $STREAMING_PARQUET_STORE) -- no ticket; "
                          "the stream is local + direct-ticket only")
        # The ticket: written by _write_ticket only, under its own lock,
        # right after each upload and catalog upload and at least every
        # beat. `revision` counts this run's ticket writes; every Flight
        # batch carries it, so a live reader knows at once to re-read.
        self._ticket_lock = threading.Lock()
        self._revision_cond = threading.Condition()
        self._ticket_revision = 0
        self._ticket_last_state = None
        self._ticket_next_mono = 0.0
        self._ticket_failing = False
        self._ticket_deleted = False
        if self._parquet is not None:
            self._parquet.on_ticket = self._write_ticket
        self._face._source = self._view_source()
        self._heartbeat_stop = threading.Event()
        self._heartbeat = threading.Thread(target=self._heartbeat_loop,
                                           daemon=True)
        self._heartbeat.start()

    @property
    def _ticket(self):
        """The ticket: who writes the stream, how often the store's data
        changes and when a reader should call the writer dead, what the
        store holds of this run, and the files there still unfinished:
        each as the path of its newest checkpoint version (an object
        written once and never changed; a superseded one stays
        keep_seconds, 10 minutes, so a reader holding a slightly old
        ticket can still open it). `files` says of each unfinished file
        where it sits on the stream's row numbers (first_row), how many
        of its rows are in finished row groups (final_rows) and how many
        it has (rows): what a view needs to change from the store to
        its ring at a row the ring still holds, with no download.
        `unfinished` is the same paths alone."""
        from ._clock import now as _now
        t = self._ticket_state()
        t["revision"] = self._ticket_revision
        t["ts"] = _now()
        return t

    @property
    def ticket_revision(self):
        return self._ticket_revision

    def wait_ticket_revision(self, seen, timeout):
        """Until this writer's ticket revision differs from `seen`
        (returns it) or `timeout` seconds pass (returns None) -- the
        writer's own views follow its ticket this way, in memory."""
        with self._revision_cond:
            if self._revision_cond.wait_for(
                    lambda: self._ticket_revision != seen or self._closed,
                    timeout):
                return self._ticket_revision
        return None

    def _view_source(self):
        """The TicketSource for views over this stream in this process:
        the ticket as this writer holds it, its revisions, its run and
        its ring. None without a ticket (no store)."""
        if self._ticket_root is None:
            return None
        from ._views import TicketSource
        return TicketSource(
            self.name, self._store,
            ticket=lambda: self._ticket if self._ticket_revision else None,
            revisions=self, run=lambda: self._run,
            memory=self._data.iter_chunks, window=self._data.window)

    def _write_ticket(self):
        """Write the ticket if what it says changed or a beat passed.
        True when the store's ticket says the current state (written
        now, or already). Every write raises the revision and tells
        connected readers (an empty batch carrying it)."""
        if self._ticket_root is None:
            return True
        from ._clock import now as _now
        with self._ticket_lock:
            if self._ticket_deleted:    # closed: a late task writes nothing
                return False
            state = self._ticket_state()
            if (state == self._ticket_last_state
                    and time.monotonic() < self._ticket_next_mono):
                return True
            ticket = dict(state, revision=self._ticket_revision + 1,
                          ts=_now())
            try:
                _tickets.put_ticket(self._ticket_root, self.name, ticket)
            except Exception as e:
                if not self._ticket_failing:
                    self._ticket_failing = True
                    console_print(f"[streaming-parquet] {self.name}: ticket "
                                  f"write failing: {e}")
                    _diag.error(f"[{self.name}] ticket write failing: {e}")
                return False
            first = self._ticket_revision == 0
            with self._revision_cond:
                self._ticket_revision += 1
                self._revision_cond.notify_all()
            self._ticket_last_state = state
            self._ticket_next_mono = time.monotonic() + self._beat_seconds
            if self._ticket_failing:
                self._ticket_failing = False
                _diag.info(f"[{self.name}] ticket write recovered")
        if first:
            console_print(f"[streaming-parquet] First ticket published: "
                          f"{self._ticket_root}/tickets/{self.name} -> "
                          f"{ticket}")
        # A waiting reader answers at once with the new revision.
        self._flight_server.notify(self.name)
        return True

    def _ticket_state(self):
        """The ticket without its write time: what a change is judged by."""
        p = self._parquet
        if p is not None:
            stored_row, stored_at, files = p.store_files()
            where = p.store_location()
            files = [dict(f, checkpoint=f"{where}{f['checkpoint']}")
                     for f in files]
            catalog_etag = p.catalog.etag if p.catalog is not None else None
            keep_seconds = p.superseded_keep_s
        else:
            stored_row, stored_at, files = -1, None, []
            catalog_etag = None
            keep_seconds = None
        # `unfinished`, the checkpoint paths alone, is what a reader from
        # before `files` knows
        unfinished = [f["checkpoint"] for f in files]
        return {"name": self.name, "host": _get_local_ip(),
                "port": self._port, "run": self._run,
                "beat_seconds": self._beat_seconds,
                "stale_after": self._stale_after,
                "checkpoint_seconds": self._checkpoint_seconds,
                "ship_seconds": (self._ship_seconds
                                 if self._remote is not None else None),
                "stored_row": stored_row, "stored_at": stored_at,
                "unfinished": unfinished, "files": files,
                "keep_seconds": keep_seconds,
                "catalog_etag": catalog_etag}

    # --- writing: the fast path -----------------------------------------------

    def get_mutable_row(self, block_s=0.0):
        """Allocate the next slot; the ring vocabulary all the way up.
        On this origin ring, PinStalledError is fatal territory."""
        return self._ring.get_mutable_row(block_s=block_s)

    def commit(self):
        """Freeze allocated rows: visible to readers at once. Then the
        publish step (memory stores and queue puts): the Flight push, the
        parquet write when due, the mirror pass when due are submitted to
        the worker pool. Nothing slow runs here."""
        count = self._ring.commit()
        if count > 0:
            if not self._first_write_logged:
                _diag.info(f"[{self.name}] FIRST WRITE: {count} rows "
                           f"(newest_seq={self._ring.newest_seq})")
                self._first_write_logged = True
            self._publish()
        return count

    def append(self, rows):
        """Many rows at once: a pyarrow RecordBatch or Table (one C
        memcpy set per batch), or a dict for one flat row. Committed as
        a unit, then published like commit()."""
        if isinstance(rows, pa.Table):
            n = 0
            for batch in rows.to_batches():
                self._ring.append_batch(batch)
                n += batch.num_rows
        elif isinstance(rows, pa.RecordBatch):
            self._ring.append_batch(rows)
            n = rows.num_rows
        elif isinstance(rows, dict):
            row = self._ring.get_mutable_row()
            for key, value in rows.items():
                setattr(row, key, value)
            n = self._ring.commit()
        else:
            raise TypeError(f"append: a RecordBatch, Table or dict, not "
                            f"{type(rows).__name__}")
        if n > 0:
            if not self._first_write_logged:
                _diag.info(f"[{self.name}] FIRST WRITE: {n} rows (append)")
                self._first_write_logged = True
            self._publish()
        return n

    def wire_map(self, entries):
        return self._ring.wire_map(entries)

    def from_wire(self, wmap, frame):
        return self._ring.from_wire(wmap, frame)

    # --- the publish step: what commit triggers ---------------------------------

    def _publish(self):
        """Compare counts; submit at most one task per kind. Pointer work
        and queue puts, microseconds. Called by commit, append, cut, and
        the heartbeat thread for the time-due part. Flight readers pull
        (see _flight_server): a commit only wakes the ones waiting."""
        from ._clock import now as _now
        committed = self._ring.newest_seq + 1
        if self._flight_server is not None:
            self._flight_server.notify(self.name)
        with self._lock:
            archive = (self._recording and committed > self._pq_cursor
                       and (self._pq_due(committed)
                            or self._tail_due(committed)))
            if self._parquet is not None and self._recording \
                    and committed > self._pq_cursor and self._pq_open_mono is None:
                # The pending region begins: this commit's row is the
                # first row of the next file, and the file is named by
                # this moment. For a continuous stream that is within a
                # frame of the cut; for one whose archive follows the data
                # (a probe's voltages) it is the session's first sample.
                self._pq_open_mono = time.monotonic()
                self._parquet.set_next_open(_now())
            ship = self._ship_due()
            if ship:
                self._ship_queued = True
                self._ship_mono = time.monotonic()
        if archive:
            self._submit_archive(committed)
        if ship:
            self._pool.submit("ship", self.name, self._do_ship)

    def _submit_archive(self, committed):
        """Pin the pending region, slice it, submit the write or the
        tail checkpoint. Under the lock: the cursors move here."""
        rel = self._data.pin(self._pq_cursor, committed)
        table = rel.detach()
        with self._lock:
            if not self._recording or committed <= self._pq_cursor:
                return
            if self._pq_due(committed):
                self._flush_parquet_region(rel, table, committed)
            elif self._tail_due(committed):
                self._checkpoint_tail(rel, table, committed)

    # --- the archive's standing ---------------------------------------------

    def _archive_fail(self, text):
        self._archive_error = text
        if not self._archive_failing:
            _diag.error(f"[{self.name}] archive failing: {text}")
            self._archive_failing = True

    def _archive_ok(self):
        if self._archive_failing:
            _diag.info(f"[{self.name}] archive recovered")
            self._archive_failing = False
            self._archive_error = None

    def archive_state(self):
        """The first copy's standing, for a daemon's display and status.
        A failing first copy means rows are durable nowhere but the ring."""
        if self._parquet is None:
            return ArchiveState(failing=False, error=None, lost_rows=0,
                                location=None, file=None)
        return ArchiveState(failing=self._archive_failing,
                            error=self._archive_error,
                            lost_rows=self._archive_lost_rows,
                            location=self._parquet.get_dir(),
                            file=self._parquet.get_open_file(),
                            catalog_error=(self._parquet.catalog.error
                                           if self._parquet.catalog else None))

    def _archive_location(self):
        return self._parquet.get_dir() if self._parquet is not None else None

    # --- the S3 mirror ------------------------------------------------------

    def _ship_due(self):
        if self._remote is None or self._parquet is None or self._ship_queued:
            return False
        if not self._parquet.get_in_flight() and self._parquet._remote_swept:
            return False
        return (self._ship_mono is None
                or time.monotonic() - self._ship_mono >= self._ship_seconds)

    def _submit_ship(self, on_done=None):
        with self._lock:
            self._ship_queued = True
            self._ship_mono = time.monotonic()
        self._pool.submit("ship", self.name, self._do_ship, on_done=on_done)

    def _do_ship(self):
        with self._lock:
            self._ship_queued = False
        try:
            self._parquet.ship()
        except Exception as e:
            self._ship_error = str(e)
            if not self._ship_failing:
                _diag.warning(f"[{self.name}] remote mirror failing: {e}")
                self._ship_failing = True
            return
        if self._ship_failing:
            _diag.info(f"[{self.name}] remote mirror recovered")
            self._ship_failing = False
            self._ship_error = None
        self._write_ticket()            # right after the upload

    def mirror_state(self):
        """The S3 mirror's standing: None without a remote."""
        if self._remote is None:
            return None
        return MirrorState(failing=self._ship_failing, error=self._ship_error,
                           pending=self._parquet.get_pending())

    # --- memory -----------------------------------------------------------

    def _rows_unarchived(self):
        """Committed rows not yet on disk: everything past the last
        durability point. On a stream that never archives, everything."""
        return max(0, self._ring.newest_seq + 1 - self._pq_durable)

    def memory_state(self):
        return self._face.memory_state()

    # --- the parquet region ---------------------------------------------------

    def _pq_due(self, committed):
        if not self._recording:
            return False
        pending = committed - self._pq_cursor
        if pending <= 0:
            return False
        if pending >= self._row_group_rows:
            return True
        return (self._row_group_seconds is not None
                and self._pq_open_mono is not None
                and time.monotonic() - self._pq_open_mono
                >= self._row_group_seconds)

    def _flush_parquet_region(self, rel, table, committed):
        """Submit the pending region [pq_cursor, committed) as one row
        group. `table` is the detached table covering [rel.lo, rel.hi).
        Caller holds the lock."""
        if rel.lo > self._pq_cursor:
            _diag.error(
                f"[{self.name}] ARCHIVE LOSS: rows [{self._pq_cursor}, "
                f"{rel.lo}) recycled before parquet flush")
            self._archive_lost_rows += rel.lo - self._pq_cursor
            self._pq_cursor = rel.lo
        region = table.slice(self._pq_cursor - rel.lo,
                             committed - self._pq_cursor)
        if region.num_rows == 0:
            return
        parquet = self._parquet

        def do_write(t=region, fs=self._pq_cursor):
            try:
                parquet.write_table(t, first_seq=fs)
                self._archive_ok()
            except Exception as e:
                self._archive_fail(f"parquet write failed: {e}")
                return
            size = parquet.file_bytes()
            if size >= FILE_CUT_BYTES:
                # the file is large enough: the next row group begins a
                # new one, with the same label
                try:
                    parquet.rotate()
                    _diag.info(f"[{self.name}] cut: the file reached "
                               f"{size:,} bytes")
                except Exception as e:
                    self._archive_fail(f"cut at {size} bytes failed: {e}")
                    return
            if self._remote is None:    # the first copy is the store
                self._write_ticket()
        self._pool.submit("parquet", self.name, do_write)
        self._pq_cursor = committed
        self._pq_durable = committed
        self._pq_open_mono = None
        self._ckpt_mono = time.monotonic()  # write_table implies checkpoint

    def _tail_due(self, committed):
        """A tail checkpoint is due: the cadence elapsed AND committed
        rows are past the durable mark. Every committed row already in
        the last checkpoint means nothing to claim, so an idle stream
        costs no encode and no upload; the rows before an idle spell
        still get their claim once, on the first check past the
        cadence, with or without new rows."""
        return (self._recording and committed > self._pq_durable
                and self._ckpt_due())

    def _ckpt_due(self):
        if self._checkpoint_seconds is None:
            return False
        base = self._ckpt_mono if self._ckpt_mono is not None \
            else self._pq_open_mono
        return (base is not None
                and time.monotonic() - base >= self._checkpoint_seconds)

    def _checkpoint_tail(self, rel, table, committed):
        """Durability point between row-group flushes: submit the
        pending region [pq_cursor, committed) as a provisional tail.
        The cursor does NOT advance -- the next flush writes the same
        rows as a final row group, superseding the tail. Caller holds
        the lock."""
        if rel.lo > self._pq_cursor:
            return                      # loss already handled at flush
        region = table.slice(self._pq_cursor - rel.lo,
                             committed - self._pq_cursor)
        if region.num_rows == 0:
            return
        parquet = self._parquet

        def do_ckpt(t=region, fs=self._pq_cursor):
            try:
                parquet.checkpoint(t, first_seq=fs)
                self._archive_ok()
            except Exception as e:
                self._archive_fail(f"tail checkpoint failed: {e}")
                return
            if self._remote is None:    # the first copy is the store
                self._write_ticket()
        self._pool.submit("parquet", self.name, do_ckpt)
        self._pq_durable = committed
        self._ckpt_mono = time.monotonic()

    # --- the archive's boundaries -----------------------------------------------

    def cut(self, label=KEEP_LABEL):
        """End the current file at the last committed row; the next
        committed row begins a new file, named by the time that row is
        committed and carrying `label`: omitted keeps the current label, None
        clears it, a string sets it. No committed row is skipped. The
        boundary call for a continuously recording host (a program
        start: cut(label=program); its end: cut(label=None)). Triggers
        the pool like commit does; returns at once."""
        self._publish()
        if not self._recording or self._parquet is None:
            return
        committed = self._ring.newest_seq + 1
        rel = self._data.pin(self._pq_cursor, committed)
        table = rel.detach()
        with self._lock:
            if committed > self._pq_cursor:
                self._flush_parquet_region(rel, table, committed)
        self._pool.submit("parquet", self.name, self._do_cut, label)

    def start_parquet(self, label=None):
        """Begin archiving: the next file opens with `label` in its
        name and covers rows committed from THIS moment. The stream
        stays live throughout."""
        if self._parquet is None:
            raise ValueError(f"stream {self.name!r} has no archive: created "
                             f"with parquet=None")
        with self._lock:
            self._pq_cursor = self._ring.newest_seq + 1
            self._pq_durable = self._pq_cursor
            self._pq_open_mono = None
        self._pool.submit("parquet", self.name, self._do_start_parquet, label)

    def stop_parquet(self):
        """End archiving: flush the pending region, close the file.
        Rows still flow to readers and to memory."""
        if self._parquet is None:
            return
        with self._lock:
            recording = self._recording
            self._recording = False
        if recording:
            committed = self._ring.newest_seq + 1
            rel = self._data.pin(self._pq_cursor, committed)
            table = rel.detach()
            with self._lock:
                if committed > self._pq_cursor:
                    self._recording = True   # the flush needs it on
                    self._flush_parquet_region(rel, table, committed)
                    self._recording = False
        self._pool.submit("parquet", self.name, self._do_stop_parquet)

    # --- reading: a Stream reads the same way an Attached does -------------

    def newest_row(self):
        return self._face.newest_row()

    def cursor(self, since=None, columns=None):
        return self._face.cursor(since=since, columns=columns)

    def arrow_table(self, columns=None, rows=None, since=None):
        return self._face.arrow_table(columns=columns, rows=rows, since=since)

    def duckdb_live(self, con, name=None):
        return self._face.duckdb_live(con, name=name)

    def duckdb_archive(self, con, name=None, store=None):
        return self._face.duckdb_archive(con, name=name, store=store)

    def duckdb_all(self, con, name=None, store=None):
        return self._face.duckdb_all(con, name=name, store=store)

    # --- Lifecycle -----------------------------------------------------------

    def close(self):
        """Final publish + cut, drain tasks, close parquet, stop the
        heartbeat, unregister, clear the retained ticket."""
        if self._closed:
            return
        self._closed = True
        # Every step of close is timed and reported in ONE line at the
        # end, so a slow shutdown names its step (2026-10-23: a 55 KB
        # file took 16 s to finish at close and nothing said where).
        t_close = time.monotonic()
        steps = []

        def step(label, t0):
            steps.append(f"{label} {time.monotonic() - t0:.2f}s")

        t0 = time.monotonic()
        self._publish()
        step("publish", t0)
        if self._recording and self._parquet is not None:
            t0 = time.monotonic()
            self.cut()
            step("cut", t0)
        self._data.mark_closed()

        # Drain this stream's parquet queue: a sentinel task + event.
        done = threading.Event()
        parquet = self._parquet

        def do_close():
            try:
                if parquet is not None:
                    parquet.close()
            except Exception as e:
                self._archive_fail(f"parquet.close failed: {e}")
            finally:
                done.set()
        t0 = time.monotonic()
        self._pool.submit("parquet", self.name, do_close)
        if not done.wait(timeout=5.0):
            _diag.warning(f"[{self.name}] close: parquet queue didn't "
                          f"drain in 5s")
            step("parquet-close TIMEOUT", t0)
        else:
            step("parquet-close", t0)

        # The closing mirror pass: with the link up, the last file's
        # remote copy is assembled before we return. With it down, the
        # file stays pending locally and the next start's boot sweep
        # finishes it.
        if self._remote is not None:
            shipped = threading.Event()
            t0 = time.monotonic()
            self._submit_ship(on_done=shipped.set)
            for _ in range(3):               # 3 s, a progress line a second
                if shipped.wait(timeout=1.0):
                    break
                doing = self._parquet.shipping_now()
                if doing:
                    console_print(f"[streaming-parquet] {self.name} close: "
                                  f"{doing}, {time.monotonic() - t0:.0f} s "
                                  f"so far")
            if not shipped.is_set():
                doing = self._parquet.shipping_now() or "the mirror pass"
                msg = (f"[{self.name}] close: still {doing} after "
                       f"{time.monotonic() - t0:.0f} s; the file is complete "
                       f"in the local spool ({self._parquet.get_dir()}) and "
                       f"the next start finishes the upload")
                _diag.warning(msg)
                console_print(f"[streaming-parquet] {msg}")
                step("ship-wait TIMEOUT", t0)
            else:
                step("ship", t0)

        # The catalog lists the last file before the stream goes, when it
        # can in a few seconds; otherwise its pass stops between two files
        # (the process must not exit inside DuckDB: _catalog) and the
        # next start's reconcile does the rest.
        if self._parquet is not None and self._parquet.catalog is not None:
            t0 = time.monotonic()
            cat = self._parquet.catalog
            cat.poke()
            done = False
            for _ in range(5):               # 5 s, a progress line a second
                if cat.wait(1.0):
                    done = True
                    break
                n, total = cat.progress
                if total > 1:
                    console_print(f"[streaming-parquet] {self.name} close: "
                                  f"catalog pass running, {n} of {total} "
                                  f"file(s) registered")
            if done:
                step("catalog", t0)
            else:
                n, total = cat.progress
                console_print(f"[streaming-parquet] {self.name} close: "
                              f"stopping the catalog pass after its current "
                              f"file ({n} of {total} registered)")
                cat.stop()
                if cat.wait(10.0):
                    step("catalog STOPPED", t0)
                else:
                    _diag.error(f"[{self.name}] close: the catalog pass did "
                                f"not stop within 10 s (one registration "
                                f"hung?); the process exiting now may abort "
                                f"inside DuckDB")
                    step("catalog STOP TIMEOUT", t0)

        t0 = time.monotonic()
        self._heartbeat_stop.set()
        if self._heartbeat is not None:
            self._heartbeat.join(timeout=2.0)
        step("heartbeat", t0)

        if self._flight_server is not None:
            t0 = time.monotonic()
            try:
                self._flight_server.unregister_stream(self.name)
            except Exception:
                pass
            step("unregister", t0)

        # Unregister from the module so the name is reusable (a closed
        # stream lingering in the registry made re-creation raise
        # "already exists" forever).
        from . import _forget_stream
        _forget_stream(self)

        if self._ticket_root is not None:
            t0 = time.monotonic()
            try:
                with self._ticket_lock:     # no write after this one
                    self._ticket_deleted = True
                    _tickets.delete_ticket(self._ticket_root, self.name)
            except Exception as e:
                _diag.error(f"[{self.name}] ticket delete failed on "
                            f"close: {e}")
            step("ticket", t0)
        if self._owner is not None:
            self._owner.release()        # the next writer may start now
        _diag.info(f"[{self.name}] closed in "
                   f"{time.monotonic() - t_close:.2f}s: " + ", ".join(steps))

    # --- Internal: parquet task implementations ----------------------------

    def _do_start_parquet(self, label):
        try:
            self._parquet.rotate()
            self._parquet.set_label(label)
            with self._lock:
                self._recording = True
            _diag.info(f"[{self.name}] start_parquet label={label!r}")
        except Exception as e:
            self._archive_fail(f"start_parquet failed: {e}")

    def _do_stop_parquet(self):
        try:
            self._parquet.close()
            _diag.info(f"[{self.name}] stop_parquet")
        except Exception as e:
            self._archive_fail(f"stop_parquet failed: {e}")

    def _do_cut(self, label):
        try:
            self._parquet.rotate()
            if label is not KEEP_LABEL:
                self._parquet.set_label(label)
            _diag.info(f"[{self.name}] cut"
                       + ("" if label is KEEP_LABEL else f" label={label!r}"))
        except Exception as e:
            self._archive_fail(f"cut failed: {e}")

    # --- Internal: heartbeat --------------------------------------------------

    def _heartbeat_wait(self):
        """The shortest cadence among the stream's time-driven jobs."""
        waits = [self.HEARTBEAT_MAX_WAIT_S]
        if self._ticket_root is not None:
            waits.append(self._beat_seconds)
        if self._parquet is not None:
            if self._checkpoint_seconds is not None:
                waits.append(self._checkpoint_seconds)
            if self._row_group_seconds is not None:
                waits.append(self._row_group_seconds)
        if self._remote is not None:
            waits.append(self._ship_seconds)
        return max(0.01, min(waits) / 2.0)

    def _heartbeat_loop(self):
        stop = self._heartbeat_stop
        while not stop.is_set():
            # the beat: the ticket at least every beat (the uploads write
            # it as they land; this covers a stream with nothing new)
            self._write_ticket()
            # The time-due part of the publish step: a quiet stream's
            # tail checkpoint, the mirror retry. Cheap when nothing is due.
            try:
                self._publish()
            except Exception as e:
                _diag.error(f"[{self.name}] heartbeat publish failed: {e}")
            # The catalog's owed pass: the reconcile at start, a retry
            # after a failure (a finished file pokes it at once).
            if self._parquet is not None and self._parquet.catalog is not None:
                self._parquet.catalog.poke()
            # Wait on the event, not a polled sleep: a close() wakes it
            # at once.
            stop.wait(self._heartbeat_wait())


#: DuckDB's row group, 60 of its 2,048-row vectors: DuckDB, the
#: archive's main reader, reads row groups in parallel.
DEFAULT_ROW_GROUP_ROWS = 122_880


def _ring_sizes(name, ring_rows, row_group_rows):
    """Settle a stream's ring rows and row group rows (the Go host's
    ringSizes). The ring holds two row groups, and each part of it has
    a job:

        the newest half      the unfinished row group: its rows stay in
                             the ring until the group is written
        the next quarter     the store's lag: a view reads from the
                             ring every row the store does not have yet
        the oldest quarter   the time a running query has: a view does
                             not start in it (_views.RING_MARGIN_SHARE)

    Given neither size, DuckDB's row group; given one, the other
    follows; given both, a ring under two row groups is refused."""
    if ring_rows is None and row_group_rows is None:
        return 2 * DEFAULT_ROW_GROUP_ROWS, DEFAULT_ROW_GROUP_ROWS
    if ring_rows is None:
        return 2 * row_group_rows, row_group_rows
    if row_group_rows is None:
        return ring_rows, max(1, ring_rows // 2)
    if ring_rows < 2 * row_group_rows:
        raise ValueError(
            f"stream {name}: a ring of {ring_rows} rows holds less than two "
            f"row groups of {row_group_rows}: rows would be reused before "
            f"their row group is written")
    return ring_rows, row_group_rows
