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

from ._diag import _diag
from ._options import MemoryOptions, ParquetOptions, ArchiveState, MirrorState, MemoryState
from ._stream_data import _StreamData
from ._worker_pool import get_pool
from . import _core, _tickets

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
                 owner=None, make_writer=None, nowhere=None, publish=True,
                 claim=None):
        """`run`: this writer's start time (_clock.run_start()), the same
        value the writer records in its files. `writer`: the
        ParquetWriter the caller built for this stream's store and
        spool, or None for a stream created with no parquet options.
        `make_writer(options)`: builds that writer later, for the first
        start_parquet of such a stream; `nowhere`: why it cannot (no
        store and no spool), or None. `owner`: the held ownership
        (_owner.claim), released at close; None on an internal stream
        that writes no parquet, and `claim()` takes it when the stream
        is published or starts a file. `publish`: other processes can
        find and read the stream; create_stream has registered it with
        the Flight server."""
        memory = memory or MemoryOptions()
        self.name = name
        self.schema = schema
        self._memory = memory
        self._archive = parquet
        self._store = store
        row_group_rows = parquet.row_group_rows if parquet else None
        ring_rows, row_group_rows = _ring_sizes(name, memory.lookback,
                                                row_group_rows)
        self._ring_rows = ring_rows
        self._make_writer = make_writer
        self._nowhere = nowhere
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
        self._claim = claim
        self._own_lock = threading.Lock()
        self._published = publish
        # One lock over the cursors and flags that commit's _publish,
        # the push task, start_parquet and stop_parquet all touch: the
        # writer's thread, the pool and a host thread may call them.
        self._lock = threading.Lock()

        a = parquet or ParquetOptions()
        self._parquet = writer              # None: no file was ever started
        # The archive's standing: the first copy's last failure, and rows
        # the archive never got (recycled before their flush). A failing
        # first copy means rows are not durable anywhere but the ring.
        self._archive_failing = False
        self._archive_error = None
        # The S3 mirror: every ship_seconds, while a file is live or
        # pending, one mirror pass goes to the ship lane.
        self._remote = writer._remote if writer is not None else None
        self._ship_seconds = a.ship_seconds
        self._ship_mono = None          # last mirror pass submitted
        self._ship_queued = False       # one pass queued at a time
        self._ship_failing = False      # for transition logging
        self._ship_error = None         # the last failure's text

        # The write plan: which committed rows go to the files, and
        # when. Its rules and state are the stream core's (src/core/sp.h,
        # THE WRITE PLAN); this class pins the rows and does the writes.
        # Tail-checkpoint cadence: between row groups, the pending rows
        # ride the checkpoint as a provisional row group. Crash loss =
        # checkpoint_seconds, independent of the row-group cadence.
        # None = durability per row group only. self._lock covers it.
        self._row_group_rows = row_group_rows
        self._row_group_seconds = a.row_group_seconds
        self._checkpoint_seconds = a.checkpoint_seconds
        self._plan = _core.Writes(row_group_rows, a.row_group_seconds,
                                  a.checkpoint_seconds)
        if writer is not None:
            self._plan.start(0)
        # the time the pending rows began, for a writer not built yet
        # to name its first file by
        self._next_open = None

        self._set_beat()

        self._pool = get_pool()
        self._closed = False
        self._first_write_logged = False

        # The reader face: a Stream reads the same way an Attached does
        # (its archive views get their ticket from this writer, in
        # memory, and are set up once the ticket is: _view_source).
        from ._attach import Attached
        self._face = Attached(data=self._data, feeder=None, on_close=None,
                              archive_dir=self._archive_location,
                              unarchived=self._rows_unarchived,
                              run=lambda: self._run)

        # The heartbeat thread: the ticket beat, and the time-due work on
        # a stream committing nothing (the tail checkpoint, the mirror
        # retry). Always runs; the ticket part only with a root.
        if not publish:
            _diag.info(f"[{self.name}] internal stream -- no ticket, not "
                       f"served to other processes")
        elif self._ticket_root is not None:
            _diag.info(f"[{self.name}] ticket heartbeat starting: "
                       f"{self._ticket_root}/tickets/{self.name} "
                       f"(on each change of the stored data, at least "
                       f"every {self._beat_seconds:g}s; stale after "
                       f"{self._stale_after:g}s; host={_get_local_ip()}, "
                       f"port={self._port})")
        else:
            _diag.info(f"[{self.name}] no store given "
                       f"(store= / $STREAMING_PARQUET_STORE) -- no ticket; "
                       f"the stream is local + direct-ticket only")
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

    def _set_beat(self):
        """The ticket's beat: the longest gap between ticket writes, the
        cadence at which the store's data changes -- the ship interval
        for a bucket behind a spool, else the checkpoint interval (the
        ship interval when there is none). The heartbeat writes the
        ticket when the stored state changes, and at least once a beat.
        With it, when a reader may call this writer dead: the one rule
        (three beats, at least 30 seconds), written into the ticket."""
        if self._remote is None and self._checkpoint_seconds is not None:
            self._beat_seconds = self._checkpoint_seconds
        else:
            self._beat_seconds = self._ship_seconds
        self._stale_after = parquet_checkpoint.dead_after(self._beat_seconds)

    @property
    def _recording(self):
        return self._plan.on

    @property
    def _pq_cursor(self):
        """The first row not yet planned as a row group."""
        return self._plan.cursor

    @property
    def _archive_lost_rows(self):
        return self._plan.lost

    def _set_parquet_options(self, options):
        """The stream's parquet options from the next file on. The pool's
        parquet lane calls it, between two files. The ring was sized at
        creation: start_parquet refused a row group it cannot hold."""
        with self._lock:
            self._archive = options
            self._row_group_rows = _ring_sizes(
                self.name, self._ring_rows, options.row_group_rows)[1]
            self._row_group_seconds = options.row_group_seconds
            self._checkpoint_seconds = options.checkpoint_seconds
            self._ship_seconds = options.ship_seconds
            self._plan.set_options(self._row_group_rows,
                                   self._row_group_seconds,
                                   self._checkpoint_seconds)
            self._set_beat()
        self._parquet.set_compression(**(options.compression or {}))
        if self._parquet.catalog is not None:
            self._parquet.catalog.set_cadence(options.ship_seconds,
                                              options.catalog_copy_seconds)

    @property
    def parquet_options(self):
        """The ParquetOptions this stream writes its files with: given at
        creation or to a start_parquet. None when it was given none and
        no file was ever started."""
        return self._archive

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

    def wait_ticket_revision(self, seen, timeout, stopped=None):
        """Until this writer's ticket revision differs from `seen`
        (returns it) or `timeout` seconds pass (returns None) -- the
        writer's own views follow its ticket this way, in memory.
        `stopped`: the waiter's own "stop now"; it ends the wait when
        the waiter notifies _revision_cond."""
        with self._revision_cond:
            if self._revision_cond.wait_for(
                    lambda: (self._ticket_revision != seen or self._closed
                             or (stopped is not None and stopped())),
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
            # An internal stream keeps its ticket in memory only: its
            # own views follow it there, and no other process is told.
            published = self._published
            try:
                if published:
                    _tickets.put_ticket(self._ticket_root, self.name, ticket)
            except Exception as e:
                if not self._ticket_failing:
                    self._ticket_failing = True
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
        if first and published:
            _diag.info(f"[{self.name}] first ticket published: "
                       f"{self._ticket_root}/tickets/{self.name} -> "
                       f"{ticket}")
        # A waiting reader answers at once with the new revision.
        self._flight_server.notify(self.name)
        return True

    def _ticket_state(self):
        """The ticket without its write time: what a change is judged by."""
        p = self._parquet
        if p is not None:
            stored_row, stored_at, files, catalog_etag = p.store_ticket()
            where = p.store_location()
            files = [dict(f, checkpoint=f"{where}{f['checkpoint']}")
                     for f in files]
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

    # --- published or internal ------------------------------------------------

    def _own(self):
        """Hold the stream's writer lock: taken the first time the
        stream is published or starts a file, held until close. Raises,
        naming the holder, when another writer has it."""
        with self._own_lock:
            if self._owner is None and self._claim is not None:
                self._owner = self._claim()

    @property
    def published(self):
        """True when other processes can find and read the stream."""
        return self._published

    def publish(self):
        """Let other processes find the stream (its ticket in the store)
        and read it (the Flight server). Takes the stream's writer lock;
        raises, naming the holder, when another writer has it."""
        if self._closed:
            raise ValueError(f"stream {self.name!r} is closed")
        self._own()
        with self._ticket_lock:
            if self._published:
                return
            self._flight_server.register_stream(self.name, self.schema, self)
            self._published = True
            self._ticket_last_state = None      # the next write goes to the store
        _diag.info(f"[{self.name}] published")
        self._write_ticket()

    def unpublish(self):
        """Make the stream internal: its ticket is deleted and the Flight
        server stops serving it. A connected reader sees what it sees
        when a writer closes. Code in this process still reads it."""
        with self._ticket_lock:
            if not self._published:
                return
            self._published = False
            if self._ticket_root is not None:
                try:
                    _tickets.delete_ticket(self._ticket_root, self.name)
                except Exception as e:
                    _diag.error(f"[{self.name}] ticket delete failed on "
                                f"unpublish: {e}")
        self._flight_server.unregister_stream(self.name)
        _diag.info(f"[{self.name}] unpublished")

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
            kind, begins = self._plan.commit(committed, time.monotonic())
            archive = kind != _core.NOTHING
            if begins:
                # The pending rows begin: this commit's row is the
                # first row of the next file, and the file is named by
                # this moment. For a continuous stream that is within a
                # frame of the cut; for one whose archive follows the data
                # (a probe's voltages) it is the session's first sample.
                self._next_open = _now()
                if self._parquet is not None:
                    self._parquet.set_next_open(self._next_open)
                    self._next_open = None
            ship = self._ship_due()
            if ship:
                self._ship_queued = True
                self._ship_mono = time.monotonic()
        if archive:
            self._submit_archive(committed)
        if ship:
            self._pool.submit("ship", self.name, self._do_ship)

    def _submit_archive(self, committed):
        """Pin the pending rows and submit the write the plan names:
        a row group or a tail checkpoint. Under the lock: the plan
        moves here."""
        rel = self._data.pin(self._pq_cursor, committed)
        table = rel.detach()
        with self._lock:
            self._write_planned(rel, table, committed, force=False)

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
        return max(0, self._ring.newest_seq + 1 - self._plan.planned)

    def memory_state(self):
        return self._face.memory_state()

    # --- the parquet region ---------------------------------------------------

    def _write_planned(self, rel, table, committed, force):
        """Ask the plan for the write over the pinned rows [rel.lo,
        committed) and submit it. `table` is the detached table of the
        pin. force: a row group whatever is due (a cut, a start's mark,
        a stop). Caller holds the lock."""
        before = self._plan.cursor
        kind, lo, hi, lost = self._plan.plan(force, rel.lo, committed,
                                             time.monotonic())
        if lost:
            _diag.error(
                f"[{self.name}] ARCHIVE LOSS: rows [{before}, "
                f"{before + lost}) recycled before parquet flush")
        if kind == _core.NOTHING:
            return
        region = table.slice(lo - rel.lo, hi - lo)
        if kind == _core.ROW_GROUP:
            self._submit_row_group(region, lo)
        else:
            self._submit_tail(region, lo)

    def _submit_row_group(self, region, first_seq):
        """The rows as one final row group, on the parquet lane."""
        def do_write(t=region, fs=first_seq):
            # read when it runs: a first start_parquet builds the writer
            # on this lane, ahead of this task
            parquet = self._parquet
            if parquet is None:         # that start failed, and said so
                return
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

    def _submit_tail(self, region, first_seq):
        """Durability point between row groups: the pending rows as a
        provisional tail. The next row group writes the same rows as a
        final row group, superseding the tail."""
        def do_ckpt(t=region, fs=first_seq):
            parquet = self._parquet
            if parquet is None:
                return
            try:
                parquet.checkpoint(t, first_seq=fs)
                self._archive_ok()
            except Exception as e:
                self._archive_fail(f"tail checkpoint failed: {e}")
                return
            if self._remote is None:    # the first copy is the store
                self._write_ticket()
        self._pool.submit("parquet", self.name, do_ckpt)

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
        if not self._recording:
            return
        committed = self._ring.newest_seq + 1
        rel = self._data.pin(self._pq_cursor, committed)
        table = rel.detach()
        with self._lock:
            self._write_planned(rel, table, committed, force=True)
        self._pool.submit("parquet", self.name, self._do_cut, label)

    def start_parquet(self, label=None, options=None):
        """Start a new file now. It carries `label` in its name and
        holds the rows committed from THIS moment: the call is a mark in
        the stream. Rows committed before it are not put into the new
        file. If a file is open, it ends at the mark, with every row
        committed before the mark in it. If no file was open (the
        stream was created with parquet=None, or stopped), the rows
        committed while it was off are in no file.

        options: a ParquetOptions for the files from this one on. It
            replaces the stream's options whole. None = the options the
            stream has, or the defaults when it never had any. A row
            group the ring cannot hold is refused: the ring was sized at
            creation.

        The stream stays live throughout. Returns at once: the pool
        ends the old file and builds what the new one needs."""
        if options is not None and not isinstance(options, ParquetOptions):
            raise TypeError(f"start_parquet: options must be a "
                            f"ParquetOptions, got {type(options).__name__}")
        if options is None:
            options = self._archive or ParquetOptions()
        _ring_sizes(self.name, self._ring_rows, options.row_group_rows)
        if self._parquet is None and self._nowhere is not None:
            raise ValueError(self._nowhere)
        self._own()                     # one writer of this stream's files
        self._publish()
        committed = self._ring.newest_seq + 1
        rel = table = None
        if self._recording:
            rel = self._data.pin(self._pq_cursor, committed)
            table = rel.detach()
        # One hold of the lock for the mark and the task: no write of a
        # row after the mark can reach the lane ahead of the task that
        # ends the old file and makes the new one ready.
        with self._lock:
            if self._recording and rel is not None:
                # the open file ends at the mark: what cut does
                self._write_planned(rel, table, committed, force=True)
            else:
                self._plan.start(self._ring.newest_seq + 1)
            self._pool.submit("parquet", self.name, self._do_start_parquet,
                              label, options)

    def stop_parquet(self):
        """End the open file, with every row committed up to now in it.
        Rows still flow to readers and to memory. The stream keeps its
        options for the next start_parquet."""
        if not self._recording:
            return
        committed = self._ring.newest_seq + 1
        rel = self._data.pin(self._pq_cursor, committed)
        table = rel.detach()
        with self._lock:
            if not self._recording:     # another thread's stop came first
                return
            self._write_planned(rel, table, committed, force=True)
            self._plan.stop()
            self._pool.submit("parquet", self.name, self._do_stop_parquet)

    # --- reading: a Stream reads the same way an Attached does -------------

    def newest_row(self):
        return self._face.newest_row()

    def cursor(self, since=None, columns=None):
        return self._face.cursor(since=since, columns=columns)

    def arrow_table(self, columns=None, rows=None, since=None):
        return self._face.arrow_table(columns=columns, rows=rows, since=since)

    def duckdb_live(self, con, name=None, row_ids=False):
        return self._face.duckdb_live(con, name=name, row_ids=row_ids)

    def duckdb_archive(self, con, name=None, store=None, row_ids=False):
        return self._face.duckdb_archive(con, name=name, store=store,
                                         row_ids=row_ids)

    def duckdb_all(self, con, name=None, store=None, row_ids=False):
        return self._face.duckdb_all(con, name=name, store=store,
                                     row_ids=row_ids)

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
        if self._recording:
            t0 = time.monotonic()
            self.cut()
            step("cut", t0)
        self._data.mark_closed()

        # Drain this stream's parquet queue: a sentinel task + event.
        done = threading.Event()

        def do_close():
            try:
                if self._parquet is not None:
                    self._parquet.close()
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
                    _diag.info(f"[{self.name}] close: {doing}, "
                               f"{time.monotonic() - t0:.0f} s so far")
            if not shipped.is_set():
                doing = self._parquet.shipping_now() or "the mirror pass"
                msg = (f"[{self.name}] close: still {doing} after "
                       f"{time.monotonic() - t0:.0f} s; the file is complete "
                       f"in the local spool ({self._parquet.get_dir()}) and "
                       f"the next start finishes the upload")
                _diag.warning(msg)
                step("ship-wait TIMEOUT", t0)
            else:
                step("ship", t0)

        # The catalog lists the last file before the stream goes, when it
        # can in a few seconds; otherwise its pass stops between two files
        # and the
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
                    _diag.info(f"[{self.name}] close: catalog pass "
                               f"running, {n} of {total} file(s) listed")
            if done:
                step("catalog", t0)
            else:
                n, total = cat.progress
                _diag.info(f"[{self.name}] close: stopping the catalog "
                           f"pass after its current file ({n} of {total} "
                           f"listed)")
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
                    if self._published:
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

    def _do_start_parquet(self, label, options):
        try:
            if self._parquet is None:
                # the first start of a stream created with no options
                writer = self._make_writer(options)
                writer.on_ticket = self._write_ticket
                with self._lock:
                    self._parquet = writer
                    self._remote = writer._remote
                    if self._next_open is not None:
                        # rows committed before the writer was built
                        # begin its first file
                        writer.set_next_open(self._next_open)
                        self._next_open = None
            else:
                self._parquet.rotate()
            self._set_parquet_options(options)
            self._parquet.set_label(label)
            _diag.info(f"[{self.name}] start_parquet label={label!r} "
                       f"{options}")
        except Exception as e:
            self._archive_fail(f"start_parquet failed: {e}")
            with self._lock:
                self._plan.stop()
            return
        self._write_ticket()            # the beat may have changed

    def _do_stop_parquet(self):
        try:
            if self._parquet is not None:
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
                             not start in it (SP_VIEW_RING_MARGIN_SHARE, src/core/sp.h)

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
