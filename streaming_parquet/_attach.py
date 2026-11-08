"""Attached: the read face of a stream, local or remote.

connect() (in __init__) returns an Attached either way:

  LOCAL  -- the named stream is published in this process: the Attached
            wraps the publisher's own _StreamData. Zero copies, no
            Flight loopback, no threads. Memory is the publisher's
            business and must not be passed.
  REMOTE -- a _Feeder owns a Flight receive thread that fills a local
            _StreamData ring via append_batch (the C memcpy ingest).
            One feeder per attach; closing the Attached stops it.

The read surface is identical in both cases because both cases ARE a
ring, and a Stream reads through the same face. Consumers poll;
nothing calls them back (house polling idiom).

    a.newest_row()                       one row, or None while empty
    for row in a.cursor(): ...           the rows this reader has not seen
    a.cursor().arrow_table()             the same rows as one table
    a.arrow_table(columns, rows, since)  memory as one Arrow table
    a.duckdb_live(con)                   memory as a DuckDB view, current
    a.duckdb_archive(con)                the files as a DuckDB view
    a.duckdb_all(con)                    memory plus the files, exact
    a.memory_state()                     rows held, rows not on disk
    a.name, a.schema, a.state, a.standing(), a.close()

Feeder states (attached.state):
    "root-unreachable" -- the store can't be read (remote)
    "no-ticket"    -- store readable, no ticket there (remote)
    "waiting"      -- have a stale ticket, publisher appears dead (remote)
    "connecting"   -- attempting Flight connection (remote)
    "streaming"    -- data flowing (always, for local)
    "reconnecting" -- lost connection, retrying with backoff (remote)
    "closed"       -- shut down (or local publisher closed)
"""

import re
import threading
import time

import pyarrow as pa
import pyarrow.flight as flight

from arrow_ring import StaleReadError

from ._diag import _diag
from ._options import MemoryState, View
from ._stream_data import _StreamData
from . import _tickets

# How often the feeder re-reads the ticket while unconnected.
TICKET_POLL_INTERVAL = 1.0


def _safe(name):
    return re.sub(r'\W+', '_', name)


def _empty_table(schema, columns=None):
    if schema is None:
        return pa.table({})
    fields = [f for f in schema if columns is None or f.name in columns]
    return pa.table({f.name: pa.array([], type=f.type) for f in fields})


class Cursor:
    """A reader's position in a stream's memory: iterating it yields the
    rows committed since the last pass, as rows; arrow_table() gives
    them as one table. Starts at the newest row, or at the first row at
    or after `since=(column, value)`. Rows recycled before this reader
    saw them are counted in `lost`. On a publisher restart (a new run)
    the stream's row numbering starts over; the cursor follows today's
    rule and the attached ring is replaced underneath it."""

    def __init__(self, attached, since=None, columns=None):
        self._a = attached
        self._columns = list(columns) if columns else None
        self.lost = 0
        d = attached._resolve()
        self._data = d
        self._next = 0
        if d is not None:
            newest = d.newest_visible_seq()
            if since is None:
                self._next = newest + 1
            else:
                column, value = since
                seq = d.seq_at_or_after(column, value)
                self._next = seq if seq >= 0 else newest + 1

    def _current(self):
        d = self._a._resolve()
        if d is not self._data:
            # a fresh ring (publisher restart): start from its oldest
            self._data = d
            self._next = d.oldest_seq if d is not None else 0
        return d

    def __iter__(self):
        d = self._current()
        if d is None:
            return
        newest = d.newest_visible_seq()
        seq = self._next
        while seq <= newest:
            try:
                row = d.read_row(seq)
            except StaleReadError:
                oldest = d.oldest_seq
                if oldest > seq:
                    self.lost += oldest - seq
                    seq = oldest
                    continue
                break                   # not yet visible (playback clock)
            self._next = seq + 1
            yield row
            seq += 1

    def arrow_table(self, columns=None):
        """The unread rows as one table, and the cursor moves past them.
        Zero copy: the table pins its own rows, and no others, until it
        is dropped."""
        d = self._current()
        cols = list(columns) if columns else self._columns
        if d is None:
            return _empty_table(None)
        hi = d.newest_visible_seq() + 1
        if hi <= self._next:
            return _empty_table(d.schema, cols)
        rs = d.pin(self._next, hi)
        if rs.lo > self._next:                  # recycled before this pass
            self.lost += rs.lo - self._next
        if rs.hi <= rs.lo:
            rs.release()
            return _empty_table(d.schema, cols)
        t = rs.detach()
        self._next = rs.hi
        return t.select(cols) if cols else t


class Attached:
    """Read face over a _StreamData. See module docstring."""

    def __init__(self, data=None, feeder=None, on_close=None,
                 archive_dir=None, unarchived=None, source=None):
        self._data = data          # _StreamData (local) or None until the
        self._feeder = feeder      #   remote feeder discovers the schema
        self._source = source      # _views.TicketSource, or None (no store)
        self._on_close = on_close
        self._closed = False
        self._archive_dir_fn = archive_dir   # callable -> the archive's location
        self._unarchived_fn = unarchived     # callable -> rows not yet on disk

    # ---- identity / state ----------------------------------------------

    @property
    def name(self):
        d = self._resolve()
        return d.name if d is not None else (
            self._feeder.name if self._feeder else None)

    @property
    def schema(self):
        d = self._resolve()
        return d.schema if d is not None else None

    @property
    def state(self):
        if self._closed:
            return "closed"
        if self._feeder is not None:
            return self._feeder.state
        if self._data is None or self._data.closed:
            return "closed"
        return "streaming"

    def is_connected(self):
        return self.state == "streaming"

    def _resolve(self):
        """Remote attaches get their _StreamData from the feeder --
        re-checked every call, because a new run (publisher respawn)
        swaps in a fresh ring."""
        if self._feeder is not None:
            d = self._feeder.data
            if d is not None:
                self._data = d
        return self._data

    def _require(self):
        d = self._resolve()
        if d is None:
            raise StaleReadError(
                f"attach({self.name!r}): not yet connected (state="
                f"{self.state})")
        return d

    # ---- the read surface (identical local/remote) ------------------------

    def newest_row(self):
        """The newest visible row, or None while the stream is empty or
        not yet connected."""
        d = self._resolve()
        if d is None:
            return None
        seq = d.newest_visible_seq()
        if seq < 0:
            return None
        try:
            return d.read_row(seq)
        except StaleReadError:
            return None

    def cursor(self, since=None, columns=None):
        """The rows this reader has not seen: iterate for rows, or
        .arrow_table() for all of them as one table. since=(column,
        value) starts at the first row at or after that value."""
        return Cursor(self, since=since, columns=columns)

    def arrow_table(self, columns=None, rows=None, since=None):
        """Memory as one Arrow table, zero copy: the table pins the
        rows it holds, and no others, until it is dropped (drop it or
        copy it; a numpy view of a column keeps it).

        No selector: the rows not yet on disk. On a reader with no
        archive of its own, everything it holds.
        rows=N: the newest N rows, fewer if memory holds fewer.
        since=(column, value): from the first row at or after `value`.
        columns: a subset of the columns."""
        d = self._resolve()
        cols = list(columns) if columns else None
        if d is None:
            return _empty_table(None)
        # The selector names the seq range first; the pin covers that
        # range and nothing else.
        hi = d.newest_visible_seq() + 1
        lo = d.oldest_seq
        if since is not None:
            column, value = since
            seq = d.seq_at_or_after(column, value)
            lo = max(lo, seq) if seq >= 0 else hi
        elif rows is not None:
            lo = max(lo, hi - int(rows))
        elif self._unarchived_fn is not None:
            lo = max(lo, hi - int(self._unarchived_fn()))
        if hi <= lo:
            return _empty_table(d.schema, cols)
        rs = d.pin(lo, hi)
        if rs.hi <= rs.lo:
            rs.release()
            return _empty_table(d.schema, cols)
        t = rs.detach()
        return t.select(cols) if cols else t

    def memory_state(self):
        d = self._resolve()
        if d is None:
            return MemoryState(rows=0, rows_unarchived=0, capacity=0)
        newest = d.newest_visible_seq()
        held = 0 if newest < 0 else newest - d.oldest_seq + 1
        unarchived = int(self._unarchived_fn()) if self._unarchived_fn else 0
        return MemoryState(rows=held, rows_unarchived=min(unarchived, held),
                           capacity=d.ring.capacity)

    # ---- DuckDB views ------------------------------------------------------

    def duckdb_live(self, con, name=None):
        """Memory as a DuckDB view that stays current: register once,
        every query sees the current rows. A scan reads memory a chunk
        at a time and pins only the chunk in hand, so a query holds
        nothing of the ring between chunks, however long it runs."""
        from arrow_ring import DynamicRelation
        d = self._require()
        vname = name or f"sp_live_{_safe(d.name)}"
        con.register(vname, DynamicRelation(d))
        return View(con, vname, unregister=lambda: con.unregister(vname))

    def standing(self):
        """The conditions standing between this reader and a live
        stream, in words: [] while streaming (or local). A dashboard
        shows them; a daemon's check folds them into its own."""
        if self._feeder is None or self.state == "streaming":
            return []
        reason = self._feeder.reason
        return [reason] if reason else [f"not streaming: {self.state}"]

    def _view_source(self, store):
        """The TicketSource for this reader's archive views: its own
        (the store it connected through, or the writer's), or one over
        `store` (a direct-ticket connect names it)."""
        from ._views import TicketSource
        if store is None:
            if self._source is None:
                raise ValueError(
                    f"{self.name!r}: the archive views read the store, and "
                    f"this reader has none: connect(name, store=...), or "
                    f"pass store=")
            return self._source
        feeder = self._feeder

        def memory_chunks(lo):
            return self._require().iter_chunks(lo)
        return TicketSource(
            self.name, store, revisions=feeder,
            run=(lambda: feeder._run) if feeder is not None else None,
            memory=memory_chunks)

    def duckdb_archive(self, con, name=None, store=None):
        """The stream's files as one DuckDB view: the finished files of
        the catalog the ticket names and the unfinished files it lists,
        kept current from the ticket (_views). The connection must be
        opened with allow_unsigned_extensions. Needs no live connection:
        the files know the columns."""
        from ._views import StoreView
        src = self._view_source(store)
        return StoreView(con, src, name or f"sp_archive_{_safe(self.name)}",
                         self._resolve().schema if self._resolve() else None,
                         with_memory=False)

    def duckdb_all(self, con, name=None, store=None):
        """Memory plus the files, one DuckDB view, exact at the seam:
        the files the ticket names up to its stored_row, memory after
        it; kept current from the ticket (_views). Without the live
        connection the memory piece is left out and `standing()` says
        why; the files still answer."""
        from ._views import StoreView
        src = self._view_source(store)
        return StoreView(con, src, name or f"sp_{_safe(self.name)}",
                         self._resolve().schema if self._resolve() else None,
                         with_memory=True)

    @property
    def archive_location(self):
        """Where the stream's files live: <store>/streams/<name> under
        the store this reader connected through, or the local
        publisher's own archive directory. None when unknown (a direct
        ticket, or a local stream with no archive)."""
        if self._archive_dir_fn is not None:
            return self._archive_dir_fn()
        if self._feeder is not None:
            return self._feeder.archive_location
        return None

    # ---- private: the sequence axis (the library's row address) ----------

    @property
    def _oldest_seq(self):
        d = self._resolve()
        return d.oldest_seq if d is not None else 0

    @property
    def _newest_seq(self):
        d = self._resolve()
        return d.newest_visible_seq() if d is not None else -1

    def _read_row(self, seq):
        return self._require().read_row(seq)

    def _seq_at_or_after(self, column, value):
        d = self._resolve()
        return d.seq_at_or_after(column, value) if d is not None else -1

    @property
    def _ring(self):
        return self._require().ring

    # ---- lifecycle ---------------------------------------------------------

    def close(self):
        if self._closed:
            return
        self._closed = True
        feeder = self._feeder
        if feeder is not None:
            feeder.close()
        if self._on_close is not None:
            try:
                self._on_close(self, feeder)
            except Exception:
                pass


def _side_data(meta):
    """(first row's seq or None, ticket revision or None) from a Flight
    batch's app_metadata: 16 bytes LE (seq, revision), or 8 (seq only,
    from a publisher without revisions). Any other size is malformed:
    appending without the axis would silently misalign, so it refuses."""
    import struct as _s
    if meta is None or len(meta) == 0:
        return None, None
    raw = meta.to_pybytes()
    if len(raw) == 16:
        seq, revision = _s.unpack('<qq', raw)
    elif len(raw) == 8:
        seq, revision = _s.unpack('<q', raw)[0], None
    else:
        raise ValueError(
            f"batch app_metadata is {len(raw)} bytes (want 16: the LE "
            f"origin seq and ticket revision; or 8: the seq) -- malformed "
            f"publisher; dropping the connection")
    return (seq if seq >= 0 else None), revision


class _Feeder:
    """Owns a _StreamData and the Flight receive thread that fills it.

    One per remote attach. Lifecycle: poll the discovery root for a
    live ticket (<root>/tickets/<name>.json), connect, discover the
    schema (get_flight_info), build the ring, do_get, append every
    received batch into the ring; reconnect with exponential backoff
    on loss. A publisher restart drops the connection, and the
    reconnect re-reads the ticket -- that is how endpoint changes are
    picked up. A direct `ticket` skips discovery entirely (no root).
    """

    def __init__(self, name, ticket=None, store=None, capacity=5000,
                 columns=None, segment_bytes=None):
        self.name = name
        self._capacity = capacity
        self._columns = columns          # top-level field subset or None
        self._segment_bytes = segment_bytes
        self._ticket = ticket
        self._store = store              # None for direct-ticket connects
        self._root_error_logged = False
        self._reason = None              # why not streaming, in words
        self._run = None                 # the publisher run streamed from
        self.data = None                 # _StreamData, built on first connect
        self._state = "no-ticket" if ticket is None else "connecting"
        self._closed = False
        self._client = None
        self._reader = None
        self._lock = threading.Lock()
        # the publisher's ticket revision, from each batch's side data:
        # a change means the ticket changed, so a reader re-reads it
        self.ticket_revision = None
        self._revision_cond = threading.Condition()
        self._stop_event = threading.Event()
        self._lifecycle = threading.Thread(target=self._lifecycle_loop,
                                           daemon=True)
        self._lifecycle.start()

    def wait_ticket_revision(self, seen, timeout):
        """Until the ticket revision differs from `seen` (returns it) or
        `timeout` seconds pass (returns None)."""
        with self._revision_cond:
            if self._revision_cond.wait_for(
                    lambda: self.ticket_revision != seen or self._closed,
                    timeout):
                return self.ticket_revision
        return None

    @property
    def state(self):
        return self._state

    @property
    def archive_location(self):
        """<store>/streams/<name> under the store connected through;
        None for a direct-ticket connect."""
        return _tickets.archive_dir(self._store, self.name)

    @property
    def reason(self):
        """Why the feeder is not streaming, in words; None while it is."""
        return None if self._state == "streaming" else self._reason

    def close(self):
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._state = "closed"
            self._stop_event.set()
            reader = self._reader
            client = self._client
        if reader is not None:
            try:
                reader.cancel()
            except Exception:
                pass
        if client is not None:
            try:
                client.close()
            except Exception:
                pass
        if threading.current_thread() is not self._lifecycle:
            self._lifecycle.join(timeout=1.0)
        if self.data is not None:
            self.data.mark_closed()

    def _refresh_ticket(self):
        """Re-read the ticket from the discovery root (no-op for
        direct-ticket attaches). Returns True when the root was
        readable (or there is no root to read)."""
        if self._store is None:
            return True
        try:
            t = _tickets.get_ticket(self._store, self.name)
        except Exception as e:
            self._reason = f"store {self._store} unreadable: {e}"
            if not self._root_error_logged:
                _diag.error(f"[{self.name}] STORE UNREADABLE: "
                            f"{self._store}: {e}")
                self._root_error_logged = True
            return False
        if self._root_error_logged:
            _diag.info(f"[{self.name}] store readable again: "
                       f"{self._store}")
            self._root_error_logged = False
        self._ticket = t                 # None = publisher closed/absent
        return True

    def _lifecycle_loop(self):
        backoff = 0.5
        while not self._closed:
            root_ok = self._refresh_ticket()
            if self._closed:
                break
            if self._ticket is None:
                if root_ok:
                    self._reason = (f"no ticket for {self.name} under "
                                    f"{self._store}: no publisher is up")
                self._state = "no-ticket" if root_ok else "root-unreachable"
                self._stop_event.wait(timeout=TICKET_POLL_INTERVAL)
                continue

            ticket = self._ticket
            ts = ticket.get("ts")
            if ts is not None and (time.time() - ts) > _tickets.stale_after(ticket):
                self._reason = (f"ticket stale: the publisher at "
                                f"{ticket.get('host')}:{ticket.get('port')} last "
                                f"beat {time.time() - ts:.0f} s ago (stale after "
                                f"{_tickets.stale_after(ticket):.0f} s)")
                self._state = "waiting"
                self._stop_event.wait(timeout=TICKET_POLL_INTERVAL)
                continue

            # A new run: the publisher respawned and its seq axis
            # restarted. Resync on a FRESH ring -- appending the new
            # axis into the old ring is the livelock (the core rightly
            # refuses seq regression). The old ring is closed so
            # anything still holding it fails loudly instead of
            # reading a dead run. (The epochs step of V2B keeps the
            # reader's rows across this instead.)
            run = ticket.get("run")
            if (self.data is not None and self._run is not None
                    and run != self._run):
                _diag.error(
                    f"[{self.name}] PUBLISHER RESTARTED: the ticket at "
                    f"{ticket.get('host')}:{ticket.get('port')} names run "
                    f"{run}, this reader streamed run {self._run}; "
                    f"resyncing on a fresh ring. Rows from the previous "
                    f"run are discarded here (the archive keeps them); "
                    f"re-create any relations over this attach.")
                old = self.data
                self.data = None
                old.mark_closed()
            self._run = run

            self._state = "connecting"
            try:
                self._client = flight.connect(
                    f"grpc://{ticket['host']}:{ticket['port']}")
                descriptor = flight.FlightDescriptor.for_path(self.name)
                info = self._client.get_flight_info(descriptor)
                schema = info.schema
                if self._columns is not None:
                    schema = pa.schema(
                        [schema.field(c) for c in self._columns])
                    ticket_str = f"{self.name}:{','.join(self._columns)}"
                else:
                    ticket_str = self.name

                if self.data is None:
                    self.data = _StreamData(
                        self.name, schema, capacity=self._capacity,
                        segment_bytes=self._segment_bytes)

                self._state = "streaming"
                backoff = 0.5
                self._receive(self._client,
                              flight.Ticket(ticket_str.encode("utf-8")))
            except Exception as e:
                if self._closed:
                    break
                self._reason = (f"connect to {ticket.get('host')}:"
                                f"{ticket.get('port')} failed: {e}")
                _diag.error(f"[{self.name}] FEEDER LIFECYCLE ERROR: {e}")

            if self._closed:
                break
            self._state = "reconnecting"
            self._stop_event.wait(timeout=backoff)
            backoff = min(backoff * 2, 10.0)

        self._state = "closed"

    def _receive(self, client, flight_ticket):
        t_connect = time.monotonic()
        _diag.info(f"[{self.name}] RECV START: connecting to Flight do_get")
        batches = 0
        rows = 0
        reader = None
        try:
            reader = client.do_get(flight_ticket)
            with self._lock:
                if self._closed:
                    try:
                        reader.cancel()
                    except Exception:
                        pass
                    return
                self._reader = reader
            _diag.info(f"[{self.name}] RECV CONNECTED: do_get returned after "
                       f"{time.monotonic() - t_connect:.3f}s")
            for chunk in reader:
                if self._closed:
                    break
                batch = chunk.data
                at_seq, revision = _side_data(chunk.app_metadata)
                if revision is not None and revision != self.ticket_revision:
                    with self._revision_cond:
                        self.ticket_revision = revision
                        self._revision_cond.notify_all()
                if batch.num_rows == 0:
                    continue        # an empty batch carries a revision only
                batches += 1
                rows += batch.num_rows
                if batches == 1:
                    _diag.info(f"[{self.name}] RECV FIRST BATCH: "
                               f"{batch.num_rows} rows, "
                               f"{time.monotonic() - t_connect:.3f}s "
                               f"after connect")
                # publisher's origin seq: keep the local ring on the
                # publisher's seq axis (gap = loud jump)
                self.data.ring.append_batch(batch, at_seq=at_seq)
        except Exception as e:
            if not self._closed:
                _diag.error(f"[{self.name}] RECV ERROR: {e} "
                            f"(after {batches} batches, {rows} rows)")
        finally:
            with self._lock:
                if reader is not None and self._reader is reader:
                    self._reader = None
            _diag.info(f"[{self.name}] RECV DONE: {batches} batches, "
                       f"{rows} rows, {time.monotonic() - t_connect:.1f}s")
