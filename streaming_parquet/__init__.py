"""streaming-parquet: live streaming plus parquet archival of Arrow
rows, on the arrow_ring core. Schemas are Arrow schemas.

The writer is only for very fast, low-latency operations. It fills rows
with get_mutable_row, from_wire or append. commit() triggers worker
threads inside the library to forward the data, write the files, and
do everything slow; cut() triggers them too. A reader, in any thread,
calls one of the reader verbs, and it all just works.

    import streaming_parquet as sp

    STORE = "s3://my-bucket"   # a whole bucket, or a whole directory

    # ---- writing ------------------------------------------------------
    s = sp.create_stream(name, arrow_schema, store=STORE,   # spool= for the local copy's place
            memory=sp.Memory(lookback=rows),
            archive=sp.Archive(checkpoint_seconds=15, ship_seconds=15, parquet={...}))   # None = memory only
    row = s.get_mutable_row(); row.x = 1.0; s.commit()      # one row
    s.append(table_or_batch_or_dict)                        # many rows
    m = s.wire_map(entries); s.from_wire(m, frame); s.commit()
    s.cut(label=...)                     # end the file; next file carries label
    s.start_parquet(label), s.stop_parquet()   # archive on and off, stream live
    s.archive_state(), s.mirror_state(), s.memory_state()
    s.close()

    # ---- reading (a Stream reads the same way) -----------------------
    a = sp.connect(name, store=STORE, memory=sp.Memory(lookback=rows))
    a.newest_row()                       # one row: row.tv, row.x ...
    for row in a.cursor(): ...           # the rows this reader has not seen
    a.cursor().arrow_table()             #   the same rows as one table
    a.arrow_table(columns, rows, since)  # memory as one Arrow table
    a.duckdb_live(con)                   # memory as a DuckDB view, current
    a.duckdb_archive(con)                # the files as a DuckDB view
    a.duckdb_all(con)                    # memory plus the files, exact
    a.name, a.schema, a.state, a.standing(), a.memory_state(), a.close()

    # ---- module ---------------------------------------------------------
    sp.list_streams(store=STORE)
    sp.create_playback_stream(name, parquet_path)
    sp.cut(label), sp.start_parquet(label), sp.stop_parquet()   # every local stream
    sp.close()

Not public: sequence numbers, the ring, record batches. Those are how
it works, not what it offers.
"""

import logging
import os
import threading

import pyarrow as pa

from ._diag import suppress_console, set_verbosity  # noqa: F401 -- public API
from ._clock import set_clock  # noqa: F401 -- public API
from ._clock import run_start
from . import _catalog
from . import _owner
from . import _stall
from . import _tickets
from ._tickets import DEFAULT_PUBLIC_STORE, DEFAULT_SPOOL  # noqa: F401 -- public API
from ._parquet_writer import ParquetWriter
from ._options import (Memory, Archive, ArchiveState, MirrorState,  # noqa: F401
                       MemoryState, View)
from ._stream import Stream, DEFAULT_ROW_GROUP_ROWS, KEEP_LABEL  # noqa: F401 -- public API
from ._attach import Attached, Cursor, _Feeder  # noqa: F401
from ._flight_server import start_flight_server

from arrow_ring import StaleReadError, PinStalledError  # noqa: F401 -- public

_diag = logging.getLogger("telemetry.diag")

# Module-level state
_lock = threading.Lock()
_flight_server = None
_flight_port = None
_streams = {}        # name -> Stream (local publishers)
_attached = {}       # id -> Attached (read faces)
_feeders = {}        # id -> _Feeder (one per remote attach)


def list_streams(store=None):
    """Names of the streams with a ticket under the store (the argument,
    else $STREAMING_PARQUET_STORE, else the public demo bucket).
    Liveness is per-ticket; a listed name may be a dead publisher's
    stale ticket."""
    return _tickets.list_streams(store if store is not None
                                 else _tickets.resolve_store())


def _ensure_flight_server():
    """Start the Flight server on first call. Returns (server, port)."""
    global _flight_server, _flight_port
    if _flight_server is None:
        _flight_server, _flight_port = start_flight_server()
    return _flight_server, _flight_port


def create_stream(name, schema, store=None, spool=DEFAULT_SPOOL,
                  memory=None, archive=None):
    """Create a stream this process writes.

    name:    the stream's whole path under the store
             ("plant/line-1/press/fieldbus"): its files
             at <store>/streams/<name>/, its ticket at
             <store>/tickets/<name>.json, its Flight descriptor.
    schema:  pyarrow.Schema (nested structs, strings, lists -- the
             arrow_ring README has the type surface).
    store:   a whole bucket ("s3://my-bucket") or a whole
             directory ("/data/streams"); a prefix inside a bucket is
             refused. None (or $STREAMING_PARQUET_STORE unset) = no
             ticket, no bucket: the stream is local + direct-ticket only,
             and its files, if any, go to the spool.
    spool:   the local write-behind copy a bucket store writes through:
             on disk, surviving a reboot, drained to the bucket on the
             ship cadence, growing during an outage. Default
             /var/tmp/streaming-parquet/<user>/. None = no local copy,
             the bucket is the first copy. Ignored for a directory
             store, which is written straight into.
    memory:  Memory(lookback, maximum, rate_hint, segment_bytes). None =
             the defaults: a ring of two parquet row groups.
    archive: Archive(checkpoint_seconds, row_group_rows,
             row_group_seconds, ship_seconds, parquet): the tuning.
             None = memory only, no files.

    Returns a Stream. commit() on it triggers the library's worker
    threads; nothing slow runs on the caller's thread.
    """
    if not isinstance(schema, pa.Schema):
        raise TypeError(f"create_stream: schema must be a pyarrow.Schema, "
                        f"got {type(schema).__name__}")
    if memory is not None and not isinstance(memory, Memory):
        raise TypeError(f"create_stream: memory must be a Memory, got "
                        f"{type(memory).__name__}")
    if archive is not None and not isinstance(archive, Archive):
        raise TypeError(f"create_stream: archive must be an Archive, got "
                        f"{type(archive).__name__}")
    if store is None:
        store = _tickets.resolve_store(for_write=True)
    if store is not None:
        _tickets.parse_store(store)      # a whole bucket or directory, or refused now

    run = run_start()                    # the ticket's run, every file's sp_run
    _stall.start()                       # reports process stalls in the log
    with _lock:
        if name in _streams:
            raise ValueError(f"Stream '{name}' already exists")
        server, port = _ensure_flight_server()
    # Own the stream before the writer exists: the writer's start sweep
    # finishes every leftover checkpoint, which is safe only for the owner.
    kind = None if store is None else _tickets.parse_store(store)[0]
    if kind == "dir":
        lock_dir = _tickets.parse_store(store)[1]
    else:
        lock_dir = spool if spool is not None else DEFAULT_SPOOL
    from ._stream import _get_local_ip
    owner = _owner.claim(name, lock_dir, store, _get_local_ip(), port, run)
    try:
        writer = None
        if archive is not None:
            writer = _writer_for(name, schema, store, spool, archive, run)
            if kind in ("s3", "dir"):
                # the catalog of the stream's finished files, in the store
                folder = _tickets.archive_dir(store, name)
                folder = folder if folder.endswith("/") else folder + "/"
                writer.catalog = _catalog.Catalog(
                    name, folder,
                    os.path.join(lock_dir, "streams", name, "catalog.work.ducklake"),
                    retry_s=archive.ship_seconds,
                    copy_s=archive.catalog_copy_seconds)
                writer.catalog.on_listed = writer.cataloged
            writer.start()
        with _lock:
            if name in _streams:
                raise ValueError(f"Stream '{name}' already exists")
            server.register_stream(name, schema, None)
            stream = Stream(name, schema, server, port, run, memory=memory,
                            archive=archive, store=store, writer=writer,
                            owner=owner)
            _streams[name] = stream
    except BaseException:
        owner.release()
        raise
    return stream


def _writer_for(name, schema, store, spool, archive, run):
    """The ParquetWriter for a stream's store and spool.

    bucket store, spool given: first copy in the spool, mirrored to the
        bucket name for name (the box's disk buffers a WAN outage)
    bucket store, spool None: the bucket is the first copy
    directory store: written straight into it; the spool is unused
    no store: the spool is the only copy
    """
    import parquet_checkpoint
    opts = dict(archive.parquet or {})
    kind, where = (None, None) if store is None else _tickets.parse_store(store)
    if kind == "dir":
        return ParquetWriter(name, schema, run, base_dir=where, **opts)
    if kind == "s3":
        bucket_sink = parquet_checkpoint.S3Sink(where)
    elif kind == "sink":
        bucket_sink = where
    else:
        bucket_sink = None
    if bucket_sink is None:
        if spool is None:
            raise ValueError(f"create_stream({name!r}): no store and no "
                             f"spool: nowhere to write the archive")
        return ParquetWriter(name, schema, run, base_dir=spool, **opts)
    if spool is None:
        return ParquetWriter(name, schema, run, sink=bucket_sink, **opts)
    return ParquetWriter(name, schema, run, base_dir=spool, remote=bucket_sink,
                         **opts)


def create_playback_stream(name, parquet_path, *,
                           timestamp_column='tv',
                           latency_s=0.0,
                           memory=None,
                           clock=None,
                           store=None):
    """Register a local Stream backed by a recorded parquet. Pre-loads
    EVERY row into the ring at startup (bulk append -- the C memcpy
    ingest), then sets a playback clock so readers only see rows whose
    `row[timestamp_column] <= clock() - latency_s`.

    Reader-side time gating handles all visibility -- the writer is
    dead-simple: one bulk load, then done. As the clock advances, more
    rows become visible automatically.

    memory defaults to a ring the size of the parquet so the full
    recording fits (the right default for replay). The schema is the
    parquet's schema -- any type the ring supports plays back.

    Use case: replay-mode daemons that need a recorded stream to appear
    live: a replay registers
    create_playback_stream("probe/voltages", "...parquet",
    timestamp_column="arrival", store=<the run's directory>) and the
    daemon's connect() with the same store just works.
    """
    import pyarrow.parquet as pq

    table = pq.read_table(parquet_path)
    if memory is None:
        memory = Memory(lookback=max(1, table.num_rows))

    stream = create_stream(name, table.schema, store=store, memory=memory,
                           archive=None)

    # Mark as playback BEFORE the load so no reader ever sees unclamped
    # rows. clock defaults to the wall clock; pass a replay-aware
    # clock for replay use.
    from ._clock import now as _lib_now
    base_clock = clock if clock is not None else _lib_now
    if latency_s:
        clock_fn = lambda: base_clock() - latency_s
    else:
        clock_fn = base_clock
    stream._data.set_playback_clock(timestamp_column, clock_fn)

    if table.num_rows > 0:
        stream.append(table)
    return stream


def connect(name=None, store=None, memory=None, columns=None, ticket=None):
    """Connect to a stream for reading (non-blocking).

    Dispatch:
      - If `name` is a stream this process writes (and `store` names no
        other store), the result is a read face over the publisher's
        own ring. Zero copies, no Flight loopback, no threads. `memory`
        is accepted and has no effect: the reader sees the publisher's
        ring, as the publisher sized it (so the same call works whether
        the stream is published here or elsewhere).
      - Otherwise a Flight feeder fills a local ring: `memory.lookback`
        sizes it (default two DuckDB row groups, 245,760 rows), `columns`
        may request a top-level field subset. The feeder polls the
        ticket at <store>/tickets/<name> (store: the argument, else
        $STREAMING_PARQUET_STORE, else the public demo bucket) -- or
        takes `ticket` directly, no discovery involved.

    `name` may be a fully-qualified stream URI
    ("s3://bucket/streams/air/dfw"): the store rides in the name.

    Returns a Stream to read. Check `.state`: "streaming" means data
    flows; "root-unreachable", "no-ticket", "waiting", "connecting",
    "reconnecting" say why not, and `.standing()` says it in words.
    """
    if name is None and ticket is None:
        raise ValueError("connect() requires name or ticket")
    if memory is not None and not isinstance(memory, Memory):
        raise TypeError(f"connect: memory must be a Memory, got "
                        f"{type(memory).__name__}")

    if name is not None and "://" in name and "/streams/" in name:
        store, name = _tickets.parse_stream_uri(name)
    if store is not None:
        _tickets.parse_store(store)      # refused now, not in the feeder thread

    # Local fast path: the stream is published here, and the caller
    # names no other store.
    if ticket is None and name is not None:
        with _lock:
            local = _streams.get(name)
        if local is not None and (store is None or store == local._store):
            if columns is not None:
                raise ValueError(
                    f"connect({name!r}): column subsets are a remote "
                    f"feature; a local connect reads the publisher's ring")
            a = Attached(data=local._data, feeder=None,
                         on_close=_forget_attached,
                         archive_dir=local._archive_location,
                         unarchived=local._rows_unarchived,
                         source=local._view_source())
            with _lock:
                _attached[id(a)] = a
            return a

    # Remote path: spin up a feeder.
    if ticket is not None:
        resolved_name = ticket["name"]
        feeder_store = None              # direct ticket: no discovery
    else:
        resolved_name = name
        feeder_store = store if store is not None else _tickets.resolve_store()

    memory = memory or Memory()
    feeder = _Feeder(resolved_name, ticket=ticket, store=feeder_store,
                     capacity=(memory.lookback if memory.lookback is not None
                               else 2 * DEFAULT_ROW_GROUP_ROWS),
                     columns=columns, segment_bytes=memory.segment_bytes)
    a = Attached(data=None, feeder=feeder, on_close=_forget_attached)
    if feeder_store is not None:
        from ._views import TicketSource

        def take(duration_s, a=a):
            return a._require().get_static_relation(duration_s)
        a._source = TicketSource(resolved_name, feeder_store, revisions=feeder,
                                 run=lambda: feeder._run, memory=take)
    with _lock:
        _feeders[id(feeder)] = feeder
        _attached[id(a)] = a
    return a


def _forget_stream(stream):
    """Remove a closed Stream from the registry (its own close calls
    this) so the name can be created again."""
    with _lock:
        if _streams.get(stream.name) is stream:
            _streams.pop(stream.name)


def _forget_attached(a, feeder=None):
    """Remove a closed Attached and its now-unused remote feeder."""
    with _lock:
        _attached.pop(id(a), None)
        if feeder is not None:
            _feeders.pop(id(feeder), None)


def _get_streams():
    """Dict of all open streams (read-only view), keyed by name."""
    with _lock:
        return dict(_streams)


def __getattr__(name):
    if name == "streams":
        return _get_streams()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def cut(label=KEEP_LABEL):
    """Cut the archive on every local stream: the current file ends at
    the last committed row, the next committed row begins a new one
    named by the time of this call. `label` names the next file's suffix
    on every stream: omitted keeps each stream's current label, None
    clears it, a string sets it. See Stream.cut."""
    with _lock:
        stream_list = list(_streams.values())
    for stream in stream_list:
        stream.cut(label)


def start_parquet(label=None):
    """Begin archiving on every local stream: each opens a fresh file
    with `label` in its name, covering rows committed from this moment.
    Readers are unaffected -- gating is the archive's only."""
    with _lock:
        stream_list = list(_streams.values())
    for stream in stream_list:
        stream.start_parquet(label=label)


def stop_parquet():
    """End archiving on every local stream. Rows still flow to readers
    and memory; only the files pause until the next start_parquet()."""
    with _lock:
        stream_list = list(_streams.values())
    for stream in stream_list:
        stream.stop_parquet()


def close():
    """Close all streams and attached readers, shut down Flight.

    Streams do a final cut on close, then subscribers get up to 2s to
    consume pending batches before the Flight server shuts down.
    """
    global _flight_server, _flight_port
    import threading
    import time as _time

    with _lock:
        attached_list = list(_attached.values())
        feeder_list = list(_feeders.values())
        stream_list = list(_streams.values())

    for a in attached_list:
        try:
            a.close()
        except Exception:
            pass

    # Close remote feeders (stops Flight receive threads). Parallel,
    # but JOINED: an unclosed Flight client leaves a gRPC finalizer
    # that blocks interpreter shutdown forever.
    closers = []
    for feeder in feeder_list:
        t = threading.Thread(target=feeder.close, daemon=True)
        t.start()
        closers.append(t)
    deadline = _time.monotonic() + 3.0
    for t in closers:
        t.join(timeout=max(0.1, deadline - _time.monotonic()))
        if t.is_alive():
            _diag.error("close: a feeder did not close within 3s "
                        "(its Flight client may pin shutdown)")

    # Close local streams concurrently (each: final cut, the closing
    # mirror pass -- a second of S3 round trips per stream -- the ticket
    # delete). Joined: the files must be finished in both places before
    # the process ends.
    def _close_stream(stream):
        try:
            stream.close()
        except Exception:
            pass
    closers = [threading.Thread(target=_close_stream, args=(s,), daemon=True)
               for s in stream_list]
    for t in closers:
        t.start()
    deadline = _time.monotonic() + 15.0
    for t in closers:
        t.join(timeout=max(0.1, deadline - _time.monotonic()))
        if t.is_alive():
            _diag.error("close: a stream did not close within 15s")

    with _lock:
        _streams.clear()
        _attached.clear()
        _feeders.clear()

        server = _flight_server

    # Let subscribers drain pending batches, then shut down -- joined
    # for the same reason as the feeders above.
    if server is not None:
        server.drain_subscribers(timeout=2.0)
        t = threading.Thread(target=server.shutdown, daemon=True)
        t.start()
        t.join(timeout=3.0)
        if t.is_alive():
            _diag.error("close: Flight server did not shut down "
                        "within 3s")

    with _lock:
        _flight_server = None
        _flight_port = None

    # Shut down the shared worker pool last (after streams have queued
    # any final close tasks).
    from ._worker_pool import shutdown_pool
    try:
        shutdown_pool()
    except Exception:
        pass


# An unclean exit (sys.exit with streams open) must still tear down:
# pyarrow/gRPC register weakref finalizers that block interpreter
# shutdown forever while a Flight server/client lives. Plain atexit is
# TOO LATE -- daemon threads can no longer run by then, so the gRPC
# teardown they must service deadlocks anyway (measured: explicit
# close exits in 2s, atexit close still hangs). threading's earlier
# exit hook (what concurrent.futures uses) runs before thread
# shutdown, while everything can still make progress.
import threading as _threading
_threading._register_atexit(close)
