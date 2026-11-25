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
from ._options import MemoryOptions, ParquetOptions, MirrorState
from ._stream_data import _StreamData
from ._worker_pool import get_pool
from ._files import (Files, KEEP_LABEL, DEFAULT_ROW_GROUP_ROWS,  # noqa: F401
                     _ring_sizes)
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
        self._store = store
        row_group_rows = parquet.row_group_rows if parquet else None
        ring_rows, row_group_rows = _ring_sizes(name, memory.rows,
                                                row_group_rows)
        self._ring_rows = ring_rows
        self._data = _StreamData(name, schema, capacity=ring_rows,
                                 segment_bytes=memory.segment_bytes)
        self._ring = self._data.ring
        # The writer of this ring is the program's own thread, which
        # must do no output: the ring keeps the lines its core makes
        # for the log, and the heartbeat thread logs them.
        self._ring.hold_lines = True
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
        # The S3 mirror: every ship_seconds, while a file is live or
        # pending, one mirror pass goes to the ship lane.
        self._remote = writer._remote if writer is not None else None
        self._ship_seconds = a.ship_seconds
        self._ship_mono = None          # last mirror pass submitted
        self._ship_queued = False       # one pass queued at a time
        self._ship_failing = False      # for transition logging
        self._ship_error = None         # the last failure's text

        # The files: the write plan, the pins and the writes (_files).
        # What is the stream's own comes in as functions: its writer
        # lock, its ticket after a write, its mirror and its beat.
        self._files = Files(
            name, self._data, ring_rows, self._lock, options=parquet,
            writer=writer, make_writer=make_writer, nowhere=nowhere,
            own=self._own, publish=self._publish,
            writer_made=self._writer_made, options_set=self._options_set,
            wrote=self._wrote, started=self._write_ticket)

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
        # set to end the heartbeat's wait early: by close(), and by a
        # commit that leaves a line of the ring's core to log
        self._heartbeat_wake = threading.Event()
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

    # --- the files' state, as the rest of the stream reads it -------------

    @property
    def _parquet(self):
        """The ParquetWriter; None while no file was ever started."""
        return self._files.writer

    @property
    def _recording(self):
        return self._files.recording

    @property
    def _checkpoint_seconds(self):
        return self._files.checkpoint_seconds

    @property
    def _row_group_seconds(self):
        return self._files.row_group_seconds

    def _writer_made(self, writer):
        """The first writer, built by a start_parquet of a stream
        created with none. Under the lock."""
        writer.on_ticket = self._write_ticket
        self._remote = writer._remote

    def _options_set(self, options):
        """New parquet options: the mirror's cadence and the ticket's
        beat follow. Under the lock."""
        self._ship_seconds = options.ship_seconds
        self._set_beat()

    def _wrote(self):
        """A write landed in the first copy. Without a mirror the first
        copy is the store: the ticket says so now."""
        if self._remote is None:
            self._write_ticket()

    @property
    def parquet_options(self):
        """The ParquetOptions this stream writes its files with: given at
        creation or to a start_parquet. None when it was given none and
        no file was ever started."""
        return self._files.options

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
        committed = self._ring.newest_seq + 1
        if self._ring.lines_held:
            self._heartbeat_wake.set()   # the heartbeat thread logs them
        if self._flight_server is not None:
            self._flight_server.notify(self.name)
        with self._lock:
            archive = self._files.committed(committed)
            ship = self._ship_due()
            if ship:
                self._ship_queued = True
                self._ship_mono = time.monotonic()
        if archive:
            self._files.submit(committed)
        if ship:
            self._pool.submit("ship", self.name, self._do_ship)

    # --- the archive's standing ---------------------------------------------

    def archive_state(self):
        """The first copy's standing, for a daemon's display and status.
        A failing first copy means rows are durable nowhere but the ring."""
        return self._files.archive_state()

    def get_durable_row_number(self):
        """The number of the newest row that is in a written piece of a
        file or in a checkpoint, and -1 when no row is there yet. With
        a spool that is the copy on the local disk: mirror_state() says
        what the bucket has. A program that must acknowledge its input
        only when the row is safe (a queue consumer) notes
        get_newest_row_number() after its commit, and waits until this
        number reaches it. The number moves when the library's worker
        has written, a moment after the commit; a write fault shows in
        archive_state(). A row committed while parquet was off is in no
        file: the number passes it when a later row is written."""
        return self._files.durable_row_number()

    def get_newest_row_number(self):
        """The number of the newest committed row, and -1 when there is
        none: after a commit(), the number of the last row that commit
        held. The first row a writer commits is 0."""
        return self._ring.newest_seq

    def _archive_location(self):
        return self._files.location()

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
        return self._files.rows_unarchived()

    def memory_state(self):
        return self._face.memory_state()

    # --- the archive's boundaries -----------------------------------------------

    def cut(self, label=KEEP_LABEL):
        """End the current file at the last committed row; the next
        committed row begins a new file, named by the time that row is
        committed and carrying `label`: omitted keeps the current label, None
        clears it, a string sets it. No committed row is skipped. The
        boundary call for a continuously recording host (a program
        start: cut(label=program); its end: cut(label=None)). Triggers
        the pool like commit does; returns at once."""
        self._files.cut(label)

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
        self._files.start(label, options)

    def stop_parquet(self):
        """End the open file, with every row committed up to now in it.
        Rows still flow to readers and to memory. The stream keeps its
        options for the next start_parquet."""
        self._files.stop()

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

        # Drain this stream's parquet queue, and close the file.
        t0 = time.monotonic()
        if not self._files.finish(timeout=5.0):
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
        self._heartbeat_wake.set()
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
        self._ring.log_lines()           # the heartbeat has stopped
        _diag.info(f"[{self.name}] closed in "
                   f"{time.monotonic() - t_close:.2f}s: " + ", ".join(steps))

    # --- Internal: heartbeat --------------------------------------------------

    def _heartbeat_wait(self):
        """The shortest cadence among the stream's time-driven jobs."""
        waits = [self.HEARTBEAT_MAX_WAIT_S]
        if self._ticket_root is not None:
            waits.append(self._beat_seconds)
        waits.extend(self._files.cadences())
        if self._remote is not None:
            waits.append(self._ship_seconds)
        return max(0.01, min(waits) / 2.0)

    def _heartbeat_loop(self):
        stop = self._heartbeat_stop
        while not stop.is_set():
            # the lines the ring's core made on the writer's thread
            self._ring.log_lines()
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
            # at once, and so does a commit with a line to log.
            self._heartbeat_wake.wait(self._heartbeat_wait())
            self._heartbeat_wake.clear()
