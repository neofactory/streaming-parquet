"""Files: the rows in memory become parquet files.

One piece, used by a Stream (the files of the stream it writes) and by
a reader that writes files (a copy of the rows it receives). It holds
the write plan, pins the rows a write needs, and gives each write to
the worker pool's parquet lane; nothing slow runs on the caller's
thread. The rules of the plan (which rows, when, a row group or a tail
checkpoint) are the stream core's (src/core/sp.h, THE WRITE PLAN).

What differs between the two users comes in as functions: the lock a
writer of files holds, what follows a write (a stream writes its
ticket), what a new ParquetWriter or new options mean to the user.
"""

import threading

from ._diag import _diag
from ._options import ParquetOptions, ArchiveState
from ._worker_pool import get_pool
from . import _core


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


def _nothing(*_args):
    return None


class Files:
    """The files of one ring's rows.

    name:        the stream's name (the log lines and the pool's lane).
    data:        the _StreamData whose rows are written.
    ring_rows:   the ring's capacity, settled by _ring_sizes.
    lock:        the user's lock: it covers the plan here and whatever
                 the user decides together with it.
    options:     the ParquetOptions the files are written with; None =
                 none given yet (a start gives them, or the defaults).
    writer:      the ParquetWriter, built by the user; with one the
                 files are on from the first row. None = no file was
                 ever started.
    make_writer: (options) -> a started ParquetWriter, for the first
                 start of a user created with none. `nowhere`: why it
                 cannot (text), or None.
    own:         () -> hold the lock of the one writer of these files;
                 raises, naming the holder, when another has it.
    publish:     () -> the user's step after a commit (a stream also
                 tells its readers and its mirror); called where a
                 boundary call must first see every committed row.
    writer_made: (writer) -> the first writer was built. Under `lock`.
    options_set: (options) -> the options changed. Under `lock`.
    wrote:       () -> a write landed in the first copy.
    started:     () -> a start is done, on the parquet lane.
    """

    def __init__(self, name, data, ring_rows, lock, *, options=None,
                 writer=None, make_writer=None, nowhere=None, own=None,
                 publish=None, writer_made=None, options_set=None,
                 wrote=None, started=None):
        self.name = name
        self._data = data
        self._ring = data.ring
        self._ring_rows = ring_rows
        self._lock = lock
        self.options = options
        self.writer = writer                # None: no file was ever started
        self._make_writer = make_writer
        self._nowhere = nowhere
        self._own = own or _nothing
        self._publish = publish or self.step
        self._writer_made = writer_made or _nothing
        self._options_set = options_set or _nothing
        self._wrote = wrote or _nothing
        self._started = started or _nothing
        self._durable_row = -1              # durable_row_number
        # The first copy's last failure. A failing first copy means rows
        # are not durable anywhere but the ring.
        self.failing = False
        self.error = None

        # The write plan: which committed rows go to the files, and
        # when. Tail-checkpoint cadence: between row groups, the pending
        # rows ride the checkpoint as a provisional row group. Crash
        # loss = checkpoint_seconds, independent of the row-group
        # cadence. None = durability per row group only. `lock` covers it.
        a = options or ParquetOptions()
        self.row_group_rows = _ring_sizes(name, ring_rows,
                                          a.row_group_rows)[1]
        self.row_group_seconds = a.row_group_seconds
        self.checkpoint_seconds = a.checkpoint_seconds
        self.plan = _core.Writes(self.row_group_rows, a.row_group_seconds,
                                 a.checkpoint_seconds)
        if writer is not None:
            self.plan.start(0)
        # the time the pending rows began, for a writer not built yet
        # to name its first file by
        self._next_open = None
        self._pool = get_pool()

    # --- what the plan says ---------------------------------------------------

    @property
    def recording(self):
        return self.plan.on

    @property
    def cursor(self):
        """The first row not yet planned as a row group."""
        return self.plan.cursor

    @property
    def lost_rows(self):
        return self.plan.lost

    def rows_unarchived(self):
        """Committed rows not yet on disk: everything past the last
        durability point. With no file ever started, everything."""
        return max(0, self._ring.newest_seq + 1 - self.plan.planned)

    def cadences(self):
        """The seconds of the time-driven writes, for the thread that
        wakes for them."""
        if self.writer is None:
            return []
        return [s for s in (self.checkpoint_seconds, self.row_group_seconds)
                if s is not None]

    def location(self):
        return self.writer.get_dir() if self.writer is not None else None

    def archive_state(self):
        """The first copy's standing. A failing first copy means rows are
        durable nowhere but the ring."""
        if self.writer is None:
            return ArchiveState(failing=False, error=None, lost_rows=0,
                                location=None, file=None)
        return ArchiveState(failing=self.failing,
                            error=self.error,
                            lost_rows=self.lost_rows,
                            location=self.writer.get_dir(),
                            file=self.writer.get_open_file(),
                            catalog_error=(self.writer.catalog.error
                                           if self.writer.catalog else None))

    def durable_row_number(self):
        """The newest row in a written piece of a file or in a
        checkpoint of the first copy; -1 for none."""
        w = self.writer
        if w is not None and w.durable_row > self._durable_row:
            self._durable_row = w.durable_row
        return self._durable_row

    def fail(self, text):
        self.error = text
        if not self.failing:
            _diag.error(f"[{self.name}] archive failing: {text}")
            self.failing = True

    def ok(self):
        if self.failing:
            _diag.info(f"[{self.name}] archive recovered")
            self.failing = False
            self.error = None

    def set_options(self, options):
        """The options from the next file on. The pool's parquet lane
        calls it, between two files. The ring was sized at creation: a
        start refused a row group it cannot hold."""
        with self._lock:
            self.options = options
            self.row_group_rows = _ring_sizes(
                self.name, self._ring_rows, options.row_group_rows)[1]
            self.row_group_seconds = options.row_group_seconds
            self.checkpoint_seconds = options.checkpoint_seconds
            self.plan.set_options(self.row_group_rows,
                                  self.row_group_seconds,
                                  self.checkpoint_seconds)
            self._options_set(options)
        self.writer.set_compression(**(options.compression or {}))
        if self.writer.catalog is not None:
            self.writer.catalog.set_cadence(options.ship_seconds,
                                            options.catalog_copy_seconds)

    # --- after a commit ---------------------------------------------------------

    def committed(self, committed):
        """The plan's look at a commit: True when a write is due (give
        it to submit(), outside the lock). The caller holds the lock."""
        from ._clock import now as _now
        kind, begins = self.plan.commit(committed)
        if begins:
            # The pending rows begin: this commit's row is the first
            # row of the next file, and the file is named by this
            # moment. For a continuous stream that is within a frame of
            # the cut; for one whose archive follows the data (a
            # probe's voltages) it is the session's first sample.
            self._next_open = _now()
            if self.writer is not None:
                self.writer.set_next_open(self._next_open)
                self._next_open = None
        return kind != _core.NOTHING

    def submit(self, committed):
        """Pin the pending rows and submit the write the plan names:
        a row group or a tail checkpoint. Under the lock: the plan
        moves here."""
        rel = self._data.pin(self.cursor, committed)
        table = rel.detach()
        with self._lock:
            self._write_planned(rel, table, committed, force=False)

    def step(self):
        """Everything of the two above, for a user with nothing else to
        decide at a commit; also the time-due look of a quiet ring (a
        tail checkpoint). Cheap when nothing is due."""
        committed = self._ring.newest_seq + 1
        with self._lock:
            due = self.committed(committed)
        if due:
            self.submit(committed)

    def _write_planned(self, rel, table, committed, force):
        """Ask the plan for the write over the pinned rows [rel.lo,
        committed) and submit it. `table` is the detached table of the
        pin. force: a row group whatever is due (a cut, a start's mark,
        a stop). Caller holds the lock."""
        before = self.plan.cursor
        kind, lo, hi, lost = self.plan.plan(force, rel.lo, committed)
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
            # read when it runs: a first start builds the writer on
            # this lane, ahead of this task
            writer = self.writer
            if writer is None:          # that start failed, and said so
                return
            try:
                writer.write_table(t, first_seq=fs)
                self.ok()
            except Exception as e:
                self.fail(f"parquet write failed: {e}")
                return
            size = writer.file_bytes()
            if size >= FILE_CUT_BYTES:
                # the file is large enough: the next row group begins a
                # new one, with the same label
                try:
                    writer.rotate()
                    _diag.info(f"[{self.name}] cut: the file reached "
                               f"{size:,} bytes")
                except Exception as e:
                    self.fail(f"cut at {size} bytes failed: {e}")
                    return
            self._wrote()
        self._pool.submit("parquet", self.name, do_write)

    def _submit_tail(self, region, first_seq):
        """Durability point between row groups: the pending rows as a
        provisional tail. The next row group writes the same rows as a
        final row group, superseding the tail."""
        def do_ckpt(t=region, fs=first_seq):
            writer = self.writer
            if writer is None:
                return
            try:
                writer.checkpoint(t, first_seq=fs)
                self.ok()
            except Exception as e:
                self.fail(f"tail checkpoint failed: {e}")
                return
            self._wrote()
        self._pool.submit("parquet", self.name, do_ckpt)

    # --- the boundaries ---------------------------------------------------------

    def cut(self, label=KEEP_LABEL):
        """End the current file at the last committed row; the next
        committed row begins a new file. Returns at once."""
        self._publish()
        if not self.recording:
            return
        committed = self._ring.newest_seq + 1
        rel = self._data.pin(self.cursor, committed)
        table = rel.detach()
        with self._lock:
            self._write_planned(rel, table, committed, force=True)
        self._pool.submit("parquet", self.name, self._do_cut, label)

    def start(self, label=None, options=None, mark=None):
        """Start a new file now: a mark in the rows. `mark`: the row
        number the mark is at, for a ring whose first row is still to
        come (a reader's); None = after the newest committed row."""
        if options is not None and not isinstance(options, ParquetOptions):
            raise TypeError(f"start_parquet: options must be a "
                            f"ParquetOptions, got {type(options).__name__}")
        if options is None:
            options = self.options or ParquetOptions()
        _ring_sizes(self.name, self._ring_rows, options.row_group_rows)
        if self.writer is None and self._nowhere is not None:
            raise ValueError(self._nowhere)
        self._own()                     # one writer of these files
        self._publish()
        committed = self._ring.newest_seq + 1
        rel = table = None
        if self.recording:
            rel = self._data.pin(self.cursor, committed)
            table = rel.detach()
        # One hold of the lock for the mark and the task: no write of a
        # row after the mark can reach the lane ahead of the task that
        # ends the old file and makes the new one ready.
        with self._lock:
            if self.recording and rel is not None:
                # the open file ends at the mark: what cut does
                self._write_planned(rel, table, committed, force=True)
            else:
                self.plan.start(self._ring.newest_seq + 1 if mark is None
                                else mark)
            self._pool.submit("parquet", self.name, self._do_start,
                              label, options)

    def stop(self):
        """End the open file, with every row committed up to now in it.
        The options stay for the next start."""
        if not self.recording:
            return
        committed = self._ring.newest_seq + 1
        rel = self._data.pin(self.cursor, committed)
        table = rel.detach()
        with self._lock:
            if not self.recording:      # another thread's stop came first
                return
            self._write_planned(rel, table, committed, force=True)
            self.plan.stop()
            self._pool.submit("parquet", self.name, self._do_stop)

    def finish(self, timeout=5.0):
        """The end: the lane writes what it was given, and the writer
        closes its file. True when that was done within `timeout`."""
        done = threading.Event()

        def do_close():
            try:
                if self.writer is not None:
                    self.writer.close()
            except Exception as e:
                self.fail(f"parquet.close failed: {e}")
            finally:
                done.set()
        self._pool.submit("parquet", self.name, do_close)
        return done.wait(timeout=timeout)

    # --- the parquet lane's tasks ----------------------------------------------

    def _do_start(self, label, options):
        try:
            if self.writer is None:
                # the first start of a user created with no writer
                writer = self._make_writer(options)
                with self._lock:
                    self.writer = writer
                    self._writer_made(writer)
                    if self._next_open is not None:
                        # rows committed before the writer was built
                        # begin its first file
                        writer.set_next_open(self._next_open)
                        self._next_open = None
            else:
                self.writer.rotate()
            self.set_options(options)
            self.writer.set_label(label)
            _diag.info(f"[{self.name}] start_parquet label={label!r} "
                       f"{options}")
        except Exception as e:
            self.fail(f"start_parquet failed: {e}")
            with self._lock:
                self.plan.stop()
            return
        self._started()

    def _do_stop(self):
        try:
            if self.writer is not None:
                self.writer.close()
            _diag.info(f"[{self.name}] stop_parquet")
        except Exception as e:
            self.fail(f"stop_parquet failed: {e}")

    def _do_cut(self, label):
        try:
            self.writer.rotate()
            if label is not KEEP_LABEL:
                self.writer.set_label(label)
            _diag.info(f"[{self.name}] cut"
                       + ("" if label is KEEP_LABEL else f" label={label!r}"))
        except Exception as e:
            self.fail(f"cut failed: {e}")
