"""Parquet file writer with rotation support, on parquet_checkpoint.

Each file is written as chunks + a checkpoint (crash-durable,
queryable while open via `name.parquet.ckpt`); rotation and close
finalize it into an ordinary .parquet file. At construction the writer
finalizes any checkpoint a crashed prior run left under ITS OWN
stream-name prefix (never other streams' live files in a shared dir).

With a `remote` sink (S3), every file is also mirrored by a
parquet_checkpoint.Shipper: the local directory stays the writer's
own tight-cadence copy, and ship() copies chunks and the newest
checkpoint outward at whatever cadence the link allows. A file
finalizes in one order: final checkpoint locally, shipped, local
assembly, remote assembly. While S3 is unreachable a finished file
stays in its local checkpoint form (readable, crash-durable), the
writer moves on to the next file, and every later ship() retries the
pending finalize. The boot sweep goes through the same path.
"""

import os
import threading
from datetime import datetime, timedelta

from ._clock import now as _wall_time

import pyarrow as pa

import parquet_checkpoint

import time

from ._diag import _diag, get_verbosity


def default_encodings(schema):
    """{column path: encoding} for every integer and float leaf of the
    schema, the writer's defaults: DELTA_BINARY_PACKED on integers (a
    counter that steps by one packs to a fraction of a bit, a quiet
    column to almost nothing) and BYTE_STREAM_SPLIT on float32/float64
    (the slowly-varying high bytes group together and compress). With
    zstd 9 and dictionary off, measured on a June 2026 CNC fieldbus recording:
    12.8 bytes per row against 28.9 with pyarrow's defaults. Struct
    leaves are `parent.child`, list elements `name.list.element`."""
    out = {}

    def walk(path, t):
        if pa.types.is_struct(t):
            for f in t:
                walk(f"{path}.{f.name}", f.type)
        elif (pa.types.is_list(t) or pa.types.is_large_list(t)
              or pa.types.is_fixed_size_list(t)):
            walk(f"{path}.list.element", t.value_type)
        elif pa.types.is_integer(t):
            out[path] = "DELTA_BINARY_PACKED"
        elif pa.types.is_float32(t) or pa.types.is_float64(t):
            out[path] = "BYTE_STREAM_SPLIT"

    for f in schema:
        walk(f.name, f.type)
    return out


class ParquetWriter:
    """Writes RecordBatches to Parquet files with rotation support.

    Files land in: <base_dir>/streams/<name>/YYYYMMDD-HHMMSS[-<label>].parquet
    (one directory per stream, the same shape under a bucket root), the
    optional label set via set_label() before the next _open().
    While a file is open, its exact committed prefix is readable by any
    process via the `.ckpt` object (parquet_checkpoint.live_table).
    """

    DEFAULT_ROTATION_INTERVAL = timedelta(hours=6)
    #: A mirror pass at or over this many seconds is logged with its
    #: per-file, per-phase breakdown at normal verbosity.
    SHIP_SLOW_S = 1.0

    def __init__(self, stream_name, schema, run, base_dir=None, rotation_interval=None,
                 compression="zstd", compression_level=None,
                 sink=None, remote=None,
                 use_dictionary=None, column_encoding=None):
        """
        run: the writing stream's run (its start time, _clock.run_start());
            every file records it as sp_run beside sp_base_seq, so a
            reader can tell which run's row numbers a file's are.
        sink: where the first copy lives -- a parquet_checkpoint sink
            (DirSink, S3Sink); None = a DirSink under <base_dir>/parquet.
            The same meaning as the Go host's ArchiveOptions.Sink: the one
            copy, whose failure is the archive failing.
        remote: a parquet_checkpoint sink (S3Sink) that mirrors the first
            copy name for name through a Shipper; None = no mirror. Its
            failure delays the mirror and loses nothing.
        compression / compression_level: passed straight to pyarrow.
            Default zstd; level 9 when the codec is zstd and no level is
            given (another codec gets its own default level).
        use_dictionary / column_encoding: pyarrow's own per-column
            controls (use_dictionary: True, False or the list of column
            names to dictionary-encode; column_encoding: {column path:
            "DELTA_BINARY_PACKED" | "BYTE_STREAM_SPLIT" | ...}). Laid
            over the defaults (default_encodings): a column named here
            takes the encoding given; a dictionary column drops its
            default encoding (pyarrow takes one or the other). Default:
            dictionary off.
        """
        self._stream_name = stream_name
        self._schema = schema
        self._run = run
        self._compression = compression
        self._compression_level = compression_level
        self._use_dictionary = use_dictionary
        self._column_encoding = column_encoding
        self._rotation_interval = rotation_interval or self.DEFAULT_ROTATION_INTERVAL
        # The layout, the same under a directory and under a bucket:
        # <root>/streams/<name>/<files>, the ticket at <root>/tickets/<name>.json.
        stream_dir = os.path.join("streams", stream_name)
        if sink is None:
            if base_dir is None:
                base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            self._dir = os.path.join(base_dir, stream_dir)
            os.makedirs(self._dir, exist_ok=True)
            sink = parquet_checkpoint.DirSink(self._dir)
        else:
            self._dir = None            # the first copy is not a local directory
            sink = sink.sub(stream_dir)
        self._sink = sink
        self._remote = remote.sub(stream_dir) if remote is not None else None
        self._ships = {}                # file name -> Shipper (live + pending)
        # What the store holds of this run, for the ticket: written on
        # the pool's lanes, read by the stream's heartbeat thread.
        self._store_lock = threading.Lock()
        self._bases = {}                # file name -> first row number (this run's files)
        self._in_store = {}             # file -> its newest checkpoint version in the store, unfinished there
        self._stored_row = -1           # newest row of this run in the store
        self._stored_at = None          # when it got there (library clock)
        self.catalog = None             # _catalog.Catalog, set by create_stream
        # Finished files whose checkpoints readers may still be reading:
        # file -> the sink holding them. A finished file stays in the
        # ticket's unfinished list until a published catalog lists it;
        # then the ticket drops it, and only then are its checkpoint
        # versions and chunks deleted (cataloged()).
        self._awaiting = {}
        self.on_ticket = None           # the stream's "write the ticket now"
        # Files the ticket has dropped: file -> (sink, when). Their
        # checkpoints are superseded then; like any superseded version
        # they are deleted SUPERSEDED_KEEP_S later, in an upload step
        # (discard_due), so a reader told of them still finds them.
        self._discard_at = {}
        self.superseded_keep_s = parquet_checkpoint.SUPERSEDED_KEEP_S
        self._remote_swept = False      # the remote half of the boot sweep
                                        # runs on the first mirror pass
        self._writer = None
        self._file_path = None
        self._file_opened_at = None
        self._file_opened_tw = None     # TW epoch s of the current file's open
        self._next_label = None         # filename suffix for next _open()
        # The next file is named by the commit time of its first row
        # (the stream sets it as the pending region begins) -- not by
        # the time of the first write into it, which the checkpoint
        # cadence can put up to one interval later, and not by the cut,
        # which for an archive that follows the data is the END of the
        # previous episode.
        self._next_open_tw = _wall_time()

    def start(self):
        """The boot sweep, once the catalog is attached (create_stream)."""
        self._boot_sweep()

    def _keeps_checkpoints(self):
        """Readers of the store are told of unfinished files; a finished
        file's checkpoints stay until the catalog lists it."""
        return self.catalog is not None

    def _boot_sweep(self):
        """Finalize every checkpoint a prior run left behind, whatever
        its age: create_stream owns the stream before it builds this
        writer (_owner: the lock file and the ticket's host), so no other
        process writes here and every checkpoint is a leftover. A file
        whose final copy exists beside its checkpoints finished before
        the crash; its checkpoints were kept for readers."""
        for final, versions in sorted(
                parquet_checkpoint.unfinished_files(self._sink).items()):
            if self._sink.exists(final):
                if self._remote is None and self._keeps_checkpoints():
                    self._await_catalog(final, self._sink, versions[-1][1])
                else:                   # a spool copy: no reader is told of it
                    parquet_checkpoint.discard_checkpoints(final, self._sink)
                continue
            if self._remote is not None:
                # pending: finalizes once its checkpoint is shipped
                self._ships[final] = parquet_checkpoint.Shipper(
                    final, self._sink, self._remote)
                _diag.info(f"[{self._stream_name}] boot sweep: "
                           f"{self._where(final)} pending "
                           f"its remote copy")
                continue
            status, where = parquet_checkpoint.finalize(final, self._sink)
            _diag.info(f"[{self._stream_name}] boot sweep: {status} "
                       f"{self._where(where)}")

    def get_opened_tw(self):
        """TW epoch s the current file was opened, or None when none is open."""
        return self._file_opened_tw

    def _where(self, name):
        return os.path.join(self._dir, name) if self._dir is not None \
            else f"{self._sink.uri}{name}"

    def get_dir(self):
        """The archive location the first copy lands in: the local
        directory, or the sink's URI when the first copy is elsewhere
        (the timeline relations read cold files only from a local
        directory today)."""
        return self._dir if self._dir is not None else self._sink.uri

    def get_open_file(self):
        """The name of the file open right now, or None when none is
        (the next write opens a fresh one)."""
        return self._writer.name if self._writer is not None else None

    def get_is_opening(self):
        """True when no file is open yet -- the next write opens a fresh one
        (the post-rotate / pre-first-write state)."""
        return self._writer is None

    def set_label(self, label):
        """Set the suffix used in the next file's name. None = no suffix.

        Takes effect on the next _open() (i.e., next write_batch after the
        current writer is closed). Stays sticky until cleared by another
        set_label call.
        """
        self._next_label = label

    def set_next_open(self, when_tw):
        """The time (TW epoch seconds) the next file is named by: the
        commit of its first row."""
        self._next_open_tw = when_tw

    def _open(self, first_seq=None):
        """Open a new Parquet file (as a checkpoint-backed live file).
        first_seq = the stream seq of the file's first row; recorded as
        sp_base_seq in the file metadata so any row's stream seq is
        base + file_row_number (the complete-relation seam). sp_run
        records the run those seqs belong to: the seq axis restarts with
        every run."""
        now = datetime.fromtimestamp(self._next_open_tw)
        stamp = now.strftime("%Y%m%d-%H%M%S")
        suffix = f"-{self._next_label}" if self._next_label else ""
        base = f"{stamp}{suffix}"
        name = f"{base}.parquet"
        n = 1
        while (self._sink.exists(name)
               or parquet_checkpoint.ckpt_versions(self._sink, name)):
            n += 1                       # same-second reopen: bump, loudly
            name = f"{base}-{n}.parquet"
        self._file_path = self._where(name)
        md = dict(self._schema.metadata or {})
        md[b'sp_run'] = self._run.encode()
        if first_seq is not None:
            md[b'sp_base_seq'] = str(int(first_seq)).encode()
            with self._store_lock:
                self._bases[name] = int(first_seq)
        schema = self._schema.with_metadata(md)
        self._writer = parquet_checkpoint.CheckpointWriter(
            name, schema, sink=self._sink,
            superseded_keep_s=self.superseded_keep_s, **self._writer_kwargs()
        )
        if self._remote is not None:
            self._ships[name] = parquet_checkpoint.Shipper(
                name, self._sink, self._remote,
                superseded_keep_s=self.superseded_keep_s)
        self._file_opened_at = now
        self._file_opened_tw = _wall_time()

    def _writer_kwargs(self):
        """pyarrow.ParquetWriter kwargs: the defaults with the stream's
        options laid over them."""
        kwargs = {"compression": self._compression}
        level = self._compression_level
        if level is None and str(self._compression).lower() == "zstd":
            level = 9
        if level is not None:
            kwargs["compression_level"] = level
        dictionary = (False if self._use_dictionary is None
                      else self._use_dictionary)
        encoding = {} if dictionary is True else default_encodings(self._schema)
        if isinstance(dictionary, (list, tuple)):
            encoding = {path: e for path, e in encoding.items()
                        if path.split(".")[0] not in dictionary}
        encoding.update(self._column_encoding or {})
        kwargs["use_dictionary"] = (list(dictionary)
                                    if isinstance(dictionary, (list, tuple))
                                    else dictionary)
        if encoding:
            kwargs["column_encoding"] = encoding
        return kwargs

    def write_table(self, table, first_seq=None):
        """Append a pyarrow Table as one row group (opens a file if
        none is open; first_seq stamps the new file's base seq). The
        publish path hands zero-copy record-set slices here. Implies a
        checkpoint: the rows are durable and readable when this
        returns."""
        if self._writer is None:
            self._open(first_seq)
        self._writer.write_table(table)
        self._stored_locally()

    def write_batch(self, batch):
        """Append a RecordBatch to the current file."""
        if self._writer is None:
            self._open()
        table = pa.Table.from_batches([batch], schema=self._schema)
        self._writer.write_table(table)

    def checkpoint(self, tail=None, first_seq=None):
        """Durability point between row-group flushes: `tail` (a Table
        of rows not yet flushed) rides the checkpoint as a provisional
        row group, superseded by the next write_table."""
        if self._writer is None:
            self._open(first_seq)
        self._writer.checkpoint(tail)
        self._stored_locally()

    def _stored_locally(self):
        """Without a mirror the first copy is the store: a checkpoint is
        the rows reaching it, and the upload step that deletes what is
        superseded."""
        if self._remote is None:
            self.discard_due()
            self._stored(self._writer.name, self._writer.rows_checkpointed,
                         self._writer.ckpt_name)

    def _stored(self, name, rows, ckpt):
        """`rows` of file `name` are in the store, unfinished there, in
        the checkpoint version `ckpt` (its object name)."""
        with self._store_lock:
            self._in_store[name] = ckpt
            base = self._bases.get(name)
            if base is not None and rows > 0 and base + rows - 1 > self._stored_row:
                self._stored_row = base + rows - 1
                self._stored_at = _wall_time()

    def _await_catalog(self, name, sink, ckpt, rows=0):
        """File `name` is finished in `sink`; its checkpoint version
        `ckpt` stays in the ticket until the catalog lists the file."""
        if ckpt is not None:
            self._stored(name, rows, ckpt)
        with self._store_lock:
            self._awaiting[name] = sink
        if self.catalog is not None:
            self.catalog.file_finished()

    def cataloged(self, names):
        """The catalog just published (or confirmed) a catalog listing
        `names`: each awaiting file among them leaves the ticket, the
        ticket is written, and only then are its checkpoints deleted.
        Called on the catalog thread."""
        with self._store_lock:
            done = [n for n in self._awaiting if n in names]
            for n in done:
                self._in_store.pop(n, None)
        if not done:
            return
        if self.on_ticket is not None and not self.on_ticket():
            return                      # ticket not written: delete nothing yet
        now = time.monotonic()
        with self._store_lock:
            for n in done:
                sink = self._awaiting.pop(n, None)
                if sink is not None:
                    self._discard_at[n] = (sink, now)
        self.discard_due()

    def discard_due(self):
        """Delete the checkpoints of files the ticket dropped at least
        superseded_keep_s ago. Called in the steps that upload (a ship
        pass; a checkpoint when the first copy is the store) and when
        the catalog drops files; nothing runs on a timer."""
        now = time.monotonic()
        with self._store_lock:
            due = [(n, sink) for n, (sink, at) in self._discard_at.items()
                   if now - at >= self.superseded_keep_s]
        for n, sink in due:
            try:
                parquet_checkpoint.discard_checkpoints(n, sink)
            except Exception as e:
                _diag.error(f"[{self._stream_name}] deleting the checkpoints "
                            f"of {n} after the ticket dropped it: {e}; the "
                            f"next upload retries")
                continue
            with self._store_lock:
                self._discard_at.pop(n, None)

    def _finished(self, name):
        """File `name` is finished in the store: an ordinary parquet file,
        for the catalog to register."""
        with self._store_lock:
            self._in_store.pop(name, None)
        if self.catalog is not None:
            self.catalog.file_finished()

    def store_state(self):
        """(stored_row, stored_at, unfinished): the newest row of this run
        in the store and when it got there (-1, None before any), and the
        object names of the newest checkpoint version of each file in the
        store unfinished."""
        with self._store_lock:
            return (self._stored_row, self._stored_at,
                    sorted(c for c in self._in_store.values() if c))

    def store_location(self):
        """The stream's folder in the store, ending in '/': the bucket's
        when mirrored, else the first copy's (a directory, or a bucket
        written directly)."""
        if self._remote is not None:
            loc = self._remote.uri
        elif self._dir is not None:
            loc = self._dir
        else:
            loc = self._sink.uri
        return loc if loc.endswith("/") else loc + "/"

    def _finish_writer(self):
        """End the current file. Local only: assemble it now. With a
        remote: write its final checkpoint and leave it pending; ship()
        assembles both copies once that checkpoint is on the remote."""
        if self._remote is None:
            name = self._writer.name
            keep = self._keeps_checkpoints()
            self._writer.close(keep_checkpoints=keep)
            if keep:
                self._await_catalog(name, self._sink, self._writer.ckpt_name,
                                    self._writer.rows_checkpointed)
            else:
                self._finished(name)
        else:
            self._writer.checkpoint()
        self._writer = None
        self._file_path = None
        self._file_opened_at = None
        self._file_opened_tw = None

    def rotate(self):
        """Force rotation: finalize the current file and open a new one
        on next write."""
        if self._writer is not None:
            self._finish_writer()

    def close(self):
        """Finalize the current file (with a remote: leave it pending
        for the stream's closing mirror pass)."""
        if self._writer is not None:
            self._finish_writer()

    def ship(self):
        """One mirror pass over every file in flight: the live file
        ships its newest checkpoint; a pending file (ended locally,
        not yet assembled) finishes once its final checkpoint is on
        the remote: local assembly, then remote assembly. Raises on
        any remote failure; the host retries on its next pass."""
        if self._remote is None:
            return
        # Each pass is timed per file and per phase. A pass slower than
        # SHIP_SLOW_S is reported with its breakdown at normal
        # verbosity; at verbosity 2 every pass is (the S3 sink then
        # also logs each call, see parquet_checkpoint._sinks.S3Sink).
        t_pass = time.monotonic()
        parts = []
        if not self._remote_swept:
            t0 = time.monotonic()
            self._sweep_remote()
            self._remote_swept = True
            parts.append(f"sweep {time.monotonic() - t0:.2f}s")
        live = self._writer.name if self._writer is not None else None
        for name, ship in list(self._ships.items()):
            t0 = time.monotonic()
            status = ship.poll()
            state = status['state']
            detail = f"{name} poll {time.monotonic() - t0:.2f}s({state}"
            if status.get('chunks'):
                detail += f", {status['chunks']} chunk(s)"
            detail += ")"
            if state == 'shipped':
                self._stored(name, status['watermark'], status['object'])
            if name == live:
                parts.append(detail)
                continue
            keep = self._keeps_checkpoints()
            if state in ('shipped', 'synced'):
                t0 = time.monotonic()
                parquet_checkpoint.finalize(name, self._sink)
                detail += f" local-finalize {time.monotonic() - t0:.2f}s"
            # 'local-finalized', or 'idle' (nothing local; remote only)
            t0 = time.monotonic()
            ship.close(keep_checkpoints=keep)
            detail += f" remote-finalize {time.monotonic() - t0:.2f}s"
            del self._ships[name]
            if keep:
                last = ship._last_object or (
                    parquet_checkpoint.ckpt_versions(self._remote, name)
                    or [(0, None)])[-1][1]
                self._await_catalog(name, self._remote, last)
            else:
                self._finished(name)
            parts.append(detail)
            _diag.info(f"[{self._stream_name}] remote finalized {name}")
        self.discard_due()              # the upload step deletes what is superseded
        elapsed = time.monotonic() - t_pass
        if parts and (elapsed >= self.SHIP_SLOW_S or get_verbosity() >= 2):
            _diag.info(f"[{self._stream_name}] ship pass {elapsed:.2f}s"
                       f"{' SLOW' if elapsed >= self.SHIP_SLOW_S else ''}: "
                       + "; ".join(parts))

    def _sweep_remote(self):
        """The remote half of the boot sweep, on the ship lane so that an
        unreachable S3 at start delays the mirror and never the stream:
        a checkpoint left on the remote whose local file already
        finalized (crash between the two assemblies) gets a Shipper that
        finishes it from the remote's own artifacts."""
        for final, versions in sorted(
                parquet_checkpoint.unfinished_files(self._remote).items()):
            if self._remote.exists(final):
                # finished before the crash; checkpoints kept for readers
                if self._keeps_checkpoints():
                    self._await_catalog(final, self._remote, versions[-1][1])
                else:
                    parquet_checkpoint.discard_checkpoints(final, self._remote)
                continue
            if final not in self._ships:
                self._ships[final] = parquet_checkpoint.Shipper(
                    final, self._sink, self._remote)
                # its checkpoint is already in the store
                self._stored(final, 0, versions[-1][1])
                _diag.info(f"[{self._stream_name}] remote sweep: "
                           f"{final} pending its remote assembly")

    def shipping_now(self):
        """What the mirror is doing on the remote right now ("uploading
        checkpoint ... (3.4 MB)"), or None."""
        for ship in list(self._ships.values()):
            doing = ship.doing
            if doing:
                return doing
        return None

    def get_pending(self):
        """Names of files ended locally whose remote copy is not yet
        assembled (empty with no remote)."""
        live = self._writer.name if self._writer is not None else None
        return [n for n in self._ships if n != live]

    def get_in_flight(self):
        """Names of files with a remote copy still to tend: the live
        file and every pending one. Empty with no remote."""
        return list(self._ships)
