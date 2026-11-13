"""The archive views: one DuckDB view over a stream, kept current from
its ticket.

    view = s.duckdb_all(con)                  # register once
    con.sql(f"SELECT ... FROM {view.name}")   # every query sees one ticket

What the view reads, all named by one ticket:

  finished files    the catalog version the ticket names (catalog_etag):
                    our code downloads exactly that version (boto3, one
                    GET conditioned on the ETag; a directory store's file
                    checked by size and time) to a local file, and DuckDB
                    attaches the local copy, reading the files' byte
                    ranges from the store
  unfinished files  the checkpoint versions the ticket lists, read
                    through the parquet_checkpoint extension
                    (read_parquet('pqc://<path>')), each up to the seam
  memory            duckdb_all only: the ring, rows after the seam
                    (same run only)

The seam is the row where the view changes from the store to the ring.
To the extent that rows are in the ring, the view uses them: the seam
is the oldest row boundary the store has that is above the ring's
oldest quarter (choose_seam). A view uses the newest three quarters of
the ring; the oldest quarter is the time a running query has before the
writer reuses the rows after its seam. With the default ring of two row
groups the ring always holds the unfinished row group, so the seam is
the end of a finished row group: the view reads finished row groups
from the store, each once, and never the rows a checkpoint adds. This
holds for a remote reader, whose ring is fed over Flight, and for the
process that writes the stream, whose ring is the source. A view with
no ring (duckdb_archive, or a reader not connected) has its seam at the
ticket's stored_row.

The view's text names all three pieces and the seam, so a query never
mixes two states. Our code replaces the view in one statement (CREATE
OR REPLACE VIEW, on its own cursor of the caller's database) when the
text must change: the seam moved (about once for each row group), the
catalog changed, or a checkpoint version the text names has been
replaced for half the time the store keeps it (the ticket's
keep_seconds; a version is deleted that long after a newer one replaces
it). A view with no ring follows every ticket. One background thread
per view decides when: a live reader learns of every ticket write from
the revision on its connection; a reader without one reads the ticket
on the schedule its write time and beat give. A catalog or checkpoint
version the store no longer holds means a newer ticket exists: the view
reads the ticket again.

The connection must load the parquet_checkpoint extension (_extension:
opened with allow_unsigned_extensions). Views are closed at exit,
before the interpreter stops their threads (a thread stopped inside
DuckDB aborts the process).
"""

import atexit
import os
import re
import shutil
import tempfile
import threading
import time

import pyarrow as pa
import pyarrow.compute as pc

import parquet_checkpoint

from . import _tickets
from ._catalog import CATALOG
from ._diag import _diag

#: A reader without a connection reads the ticket at least this often
#: when the ticket names no beat, and never more often than POLL_MIN_S.
POLL_DEFAULT_S = 15.0
POLL_MIN_S = 0.5
#: Margin after a ticket's expected next write before reading it again.
POLL_MARGIN_S = 1.0
#: Catalog copies kept attached: the current one and those a query
#: still running may hold.
KEEP_CATALOGS = 3
#: The share of the ring a view leaves alone: its oldest rows. A query
#: keeps the seam it began with, and the row after that seam must still
#: be in the ring when the query's scan reaches the ring. A seam above
#: the oldest quarter gives the query that many rows of the writer's
#: time. (Not a pin: a scan pins one chunk for under a millisecond. This
#: is the time between a query's start and its scan of the ring.)
RING_MARGIN_SHARE = 0.25
#: A view names a checkpoint version until the seam moves, or until the
#: version has been replaced for this share of the time the store keeps
#: a replaced version.
REPOINT_SHARE = 0.5


def _safe(s):
    return re.sub(r'\W+', '_', s)


class _Gone(Exception):
    """The store holds a newer version than the ticket names."""


class TicketSource:
    """Where a reader's views get the stream's ticket and memory.

    name, store:  the stream and the store it lives in.
    ticket():     the current ticket, or None (no publisher).
    revisions:    an object with wait_ticket_revision(seen, timeout)
                  and ticket_revision (a live connection, or the
                  writer itself), or None (read on a schedule).
    run():        the run the memory holds, or None.
    memory(lo):   the ring's rows from seq lo on, as (first_seq, batch)
                  pairs read a chunk at a time (Ring.iter_chunks), or
                  None.
    window():     (oldest row, newest row, capacity) of that ring, or
                  None: what a view places its seam by.
    """

    def __init__(self, name, store, ticket=None, revisions=None, run=None,
                 memory=None, window=None):
        self.name = name
        self.store = store
        self.ticket = ticket or (lambda: _tickets.get_ticket(store, name))
        self.revisions = revisions
        self.run = run or (lambda: None)
        self.memory = memory
        self.window = window
        folder = _tickets.archive_dir(store, name)
        if folder is None:
            raise ValueError(
                f"{name!r}: the archive views read the store, and this "
                f"reader has none: connect(name, store=...), or pass store=")
        self.folder = folder if folder.endswith("/") else folder + "/"


class _MemoryProxy:
    """Per query: the ring's committed rows, each with its row number in
    _seq and, in _first, the row number of the first row this read
    delivered (the ring's oldest row when the scan began). The view's
    text keeps the rows after its seam, and fails the query when _first
    is past the row after the seam: the ring no longer held the rows the
    query needed. Read a chunk at a time through Ring.iter_chunks, zero
    copy: nothing is pinned until the scan reaches memory, which in a
    query over the store may be seconds in, and then only the chunk in
    hand. (2026-10-27 a whole-window pin held for such a query stalled
    the writer; a copy patched it; the chunk read retires the copy.)
    A reader with no connection yet scans an empty memory."""

    def __init__(self, chunks, schema):
        self._chunks = chunks
        self._schema = (schema.append(pa.field('_seq', pa.int64()))
                        .append(pa.field('_first', pa.int64())))

    def __arrow_c_stream__(self, requested_schema=None):
        try:
            it = self._chunks(0)        # from the ring's oldest row
        except Exception:
            it = ()
        return pa.RecordBatchReader.from_batches(
            self._schema, _Numbered(it)).__arrow_c_stream__(requested_schema)


class _Numbered:
    """(first_seq, batch) pairs as batches with _seq and _first. A class
    with __next__, not a generator: a generator's loop variable would
    hold each batch, and its chunk's pin, until the next pull."""

    def __init__(self, pairs):
        self._pairs = iter(pairs)
        self._first = None

    def __iter__(self):
        return self

    def __next__(self):
        first, batch = next(self._pairs)
        if self._first is None:
            self._first = first
        n = batch.num_rows
        return (batch.append_column('_seq', _row_numbers(first, n))
                .append_column('_first', pa.repeat(
                    pa.scalar(self._first, pa.int64()), n)))


_counting = None


def _row_numbers(first, n):
    """The int64 array first, first + 1, ..., first + n - 1."""
    global _counting
    if _counting is None or len(_counting) < n:
        _counting = pa.array(range(max(n, 1 << 14)), type=pa.int64())
    return pc.add(_counting.slice(0, n), pa.scalar(first, pa.int64()))


def choose_seam(stored_row, files, window):
    """Where a view changes from the store to the ring.

    stored_row:  the newest row in the store (the ticket's).
    files:       the ticket's unfinished files: dicts with first_row and
                 final_rows (either may be None: not known).
    window:      (oldest row, newest row, capacity) of the ring, or None
                 (no ring: a view of the files only).

    Returns (seam, use_ring, why). The view reads the store up to row
    `seam` and, when use_ring, the ring after it. The row boundaries the
    store has are the start of each unfinished file, the end of its
    finished row groups, and stored_row. The seam is the oldest of them
    that the ring covers and that is above the ring's oldest quarter.

    why, when the choice is not that one:
      'tight'  no boundary is above the oldest quarter, but the ring
               reaches stored_row: the seam is stored_row, and a running
               query has less than a quarter of the ring.
      'short'  the ring is full and begins after stored_row + 1: it is
               too short for the store's lag. No ring piece.
      'late'   the ring is not full and begins after stored_row + 1 (a
               reader that connected after the ticket's checkpoint; a
               later ticket reaches its first row). No ring piece.
    """
    if window is None:
        return stored_row, False, None
    oldest, newest, capacity = window
    if newest < oldest:                 # the ring holds no rows yet
        return stored_row, True, None
    # the first row above the oldest quarter of a full ring: the writer
    # reuses row r once it has written row r + capacity
    above = newest + 1 - capacity + int(capacity * RING_MARGIN_SHARE)
    candidates = {stored_row}
    for f in files:
        first = f.get("first_row")
        if first is None:
            continue
        candidates.add(first - 1)
        if f.get("final_rows") is not None:
            candidates.add(first + f["final_rows"] - 1)
    usable = [c for c in candidates
              if c <= stored_row and c + 1 >= oldest and c + 1 >= above]
    if usable:
        return min(usable), True, None
    if oldest <= stored_row + 1:
        return stored_row, True, 'tight'
    full = newest - oldest + 1 >= capacity
    return stored_row, False, ('short' if full else 'late')


def _ticket_files(ticket):
    """The ticket's unfinished files, as dicts with checkpoint,
    first_row, final_rows and rows. A ticket from a writer before
    `files` lists the checkpoint paths alone: the numbers are None."""
    if not ticket:
        return []
    if ticket.get("files") is not None:
        return list(ticket["files"])
    return [{"checkpoint": c, "first_row": None, "final_rows": None,
             "rows": None} for c in ticket.get("unfinished") or ()]


def _file_of(checkpoint):
    """The path of the file a checkpoint version's path belongs to."""
    folder, _, obj = checkpoint.rpartition("/")
    got = parquet_checkpoint.parse_ckpt_object(obj)
    return (folder + "/" + got[0]) if got is not None else checkpoint


_views = set()
_views_lock = threading.Lock()


def _close_all():
    with _views_lock:
        views = list(_views)
    for v in views:
        v.close()


atexit.register(_close_all)


class StoreView:
    """One DuckDB view over a stream's store (duckdb_archive) or its
    store and memory (duckdb_all). See the module docstring."""

    def __init__(self, con, source, name, schema, with_memory):
        from . import _extension
        _extension.load(con, f"{name}: the archive view of {source.name!r}")
        self.name = name
        self._con = con
        self._cur = con.cursor()        # our statements, from our thread
        self._source = source
        self._schema = schema
        self._with_memory = with_memory and source.memory is not None
        sid = _safe(name)
        self._sid = sid
        self._mem_name = f"__sp_mem_{sid}"
        self._empty_name = f"__sp_empty_{sid}"
        if schema is not None:
            empty = pa.table({f.name: pa.array([], type=f.type) for f in schema})
            for c in (con, self._cur):
                c.register(self._empty_name, empty)
                if self._with_memory:
                    c.register(self._mem_name,
                               _MemoryProxy(source.memory, schema))
        self._tmp = tempfile.mkdtemp(prefix="sp-view-")
        self._lock = threading.Lock()
        self._k = 0
        self._catalogs = []             # [(etag, alias, path)], newest last
        self._built = None              # the facts the view's text names
        self._ticket = None
        self._seen_revision = None
        # file -> (the checkpoint version the text names, when a newer
        # one was first seen or None)
        self._named = {}
        self._named_seam = None
        self.seam = None                # the row the text changes at
        self.rebuilds = 0               # times the view's text was replaced
        self.ring_note = None           # why the ring is not used as wanted
        self.error = None               # the last refresh's failure
        self._stop = threading.Event()
        self.refresh()                  # the first build raises
        with _views_lock:
            _views.add(self)
        self._thread = threading.Thread(target=self._loop, daemon=True,
                                        name=f"view-{sid}")
        self._thread.start()

    # ---- building the view ------------------------------------------------

    def refresh(self):
        """Read the ticket and, if it changed, replace the view."""
        with self._lock:
            for _ in range(3):
                try:
                    self._refresh()
                    return
                except _Gone:
                    continue            # a newer ticket exists: read it
            self._refresh()

    def _refresh(self):
        src = self._source
        if src.revisions is not None:
            self._seen_revision = getattr(src.revisions, "ticket_revision", None)
        t = src.ticket()
        self._ticket = t
        # A live writer's ticket names the catalog version to read. No
        # ticket (a clean close deletes it), a stale one (a dead writer)
        # or one without a revision (an older writer) vouches for none:
        # read the store's current catalog.
        live = (t is not None and "revision" in t
                and time.time() - float(t.get("ts", 0)) < _tickets.stale_after(t))
        etag = t.get("catalog_etag") if live else None
        files = _ticket_files(t)
        run = t.get("run") if t else None
        stored = t.get("stored_row", -1) if t else -1
        seam, use_ring, why = stored, False, None
        if (self._with_memory and live and run is not None
                and run == src.run()):
            seam, use_ring, why = choose_seam(stored, files, self._window())
        self._note_ring(why, stored)
        keep = float((t or {}).get("keep_seconds")
                     or parquet_checkpoint.SUPERSEDED_KEEP_S)
        named = self._name_versions(files, seam, keep * REPOINT_SHARE)
        facts = (etag if live else "current", tuple(named), run, seam,
                 use_ring)
        if facts == self._built and live:
            return
        catalog = self._catalog(etag, exact=live)
        pieces = []
        if catalog is not None:
            pieces.append(f'SELECT * FROM "{catalog}".data')
        for path, last_row in named:
            quoted = "'pqc://" + path.replace("'", "''") + "'"
            if last_row is None:
                pieces.append(f"SELECT * FROM read_parquet({quoted})")
            else:
                # the file up to the seam: DuckDB opens no object of the
                # file past that row
                pieces.append(
                    f"SELECT * EXCLUDE (file_row_number) FROM "
                    f"read_parquet({quoted}, file_row_number=true) "
                    f"WHERE file_row_number <= {int(last_row)}")
        if use_ring:
            # The view reads the store up to the seam and the ring after
            # it. If the ring begins past the row after the seam, rows
            # are in neither: the query fails, it does not answer short.
            what = (f"{self.name}: the ring does not hold row "
                    f"{int(seam) + 1}; it begins at row ")
            why = (". This query read the rows up to "
                   f"{int(seam)} from the store. Either it reached the ring "
                   "after the stream had written more rows than the ring "
                   "keeps for a running query, or this reader connected a "
                   "moment ago and the store has not reached its first row "
                   "yet. Run it again; for longer queries connect with a "
                   "larger MemoryOptions(lookback=...).")
            pieces.append(
                f'SELECT * EXCLUDE (_seq, _first) FROM "{self._mem_name}" '
                f'WHERE _seq > {int(seam)} AND CASE WHEN _first > '
                f"{int(seam) + 1} THEN error('" + what.replace("'", "''")
                + "' || _first || '" + why.replace("'", "''")
                + "') ELSE true END")
        if not pieces:
            if self._schema is None:
                raise ValueError(
                    f"{self.name}: {src.name!r} has no catalog, no "
                    f"unfinished file and no memory yet; nothing to read "
                    f"the columns from")
            pieces.append(f'SELECT * FROM "{self._empty_name}"')
        if src.folder.startswith("s3://"):
            self._give_duckdb_s3(src.folder)
        sql = " UNION ALL BY NAME ".join(f"({p})" for p in pieces)
        self._cur.execute(f'CREATE OR REPLACE VIEW "{self.name}" AS {sql}')
        self._built = facts
        self.seam = seam
        self.rebuilds += 1
        self._detach_old()

    def _window(self):
        """The ring's (oldest row, newest row, capacity), or None."""
        if self._source.window is None:
            return None
        try:
            return self._source.window()
        except Exception:
            return None                 # a reader not yet connected

    def _name_versions(self, files, seam, repoint_s):
        """[(checkpoint path, last file row or None)] for the unfinished
        files the text reads: those that begin at or before the seam,
        each up to the seam (None: the whole file, its place on the row
        numbers not known). A file keeps the checkpoint version the text
        already names until the seam moves or that version has been
        replaced for repoint_s: every version holds the rows up to the
        seam it was named for."""
        now = time.monotonic()
        moved = seam != self._named_seam
        named, keeps = [], {}
        for f in files:
            first = f.get("first_row")
            if first is not None and first > seam:
                continue                # after the seam: the ring has it
            key = _file_of(f["checkpoint"])
            path, since = f["checkpoint"], None
            old = self._named.get(key)
            if old is not None and not moved and old[0] != path:
                since = old[1] if old[1] is not None else now
                if now - since < repoint_s:
                    path = old[0]       # still there: keep naming it
                else:
                    since = None
            keeps[key] = (path, since)
            named.append((path, None if first is None else seam - first))
        self._named = keeps
        self._named_seam = seam
        return named

    def _note_ring(self, why, stored):
        """Say, once for each change, why the ring is not used as
        wanted. 'late' is not said: a later ticket ends it."""
        note = None
        if why in ('short', 'tight'):
            oldest, newest, capacity = self._window() or (0, -1, 0)
            behind = newest - stored
            want = int(behind / (1 - RING_MARGIN_SHARE)) + 1
            if why == 'short':
                note = (f"the ring holds rows {oldest} to {newest} and the "
                        f"store ends at row {stored}: the ring is too short "
                        f"for the store's lag of {behind} rows, so the view "
                        f"reads the files only and is that far behind. "
                        f"Connect with MemoryOptions(lookback={want}) or more "
                        f"(now {capacity}).")
            else:
                note = (f"the store is {behind} rows behind a ring of "
                        f"{capacity}: a running query has less than a "
                        f"quarter of the ring to reach memory. Connect with "
                        f"MemoryOptions(lookback={want}) or more.")
        if (note is None) != (self.ring_note is None):
            if note is not None:
                _diag.warning(f"[{self._source.name}] view {self.name}: {note}")
            else:
                _diag.info(f"[{self._source.name}] view {self.name}: the "
                           f"ring is in use again")
        self.ring_note = note

    def _catalog(self, etag, exact):
        """The alias of the attached catalog copy for `etag` (None when
        the ticket names none). exact=False: the store's current
        catalog, whatever its version (no publisher, no ticket)."""
        if exact and etag is None:
            return None
        if exact and self._catalogs and self._catalogs[-1][0] == etag:
            return self._catalogs[-1][1]
        self._k += 1
        path = os.path.join(self._tmp, f"catalog.{self._k}.ducklake")
        got = _fetch_catalog(self._source.folder, etag if exact else None, path)
        if got is None:
            if exact:
                raise _Gone(f"catalog {etag}")
            return None                 # no catalog published yet
        if not exact and self._catalogs and self._catalogs[-1][0] == got:
            os.remove(path)
            return self._catalogs[-1][1]
        alias = f"__sp_cat_{self._sid}_{self._k}"
        self._cur.execute(f"ATTACH 'ducklake:{path}' AS \"{alias}\" (READ_ONLY)")
        self._catalogs.append((got, alias, path))
        return alias

    def _detach_old(self):
        while len(self._catalogs) > KEEP_CATALOGS:
            _etag, alias, path = self._catalogs.pop(0)
            try:
                self._cur.execute(f'DETACH "{alias}"')
                os.remove(path)
            except Exception as e:
                _diag.warning(f"[{self._source.name}] view {self.name}: "
                              f"detaching an old catalog copy: {e}")

    def _give_duckdb_s3(self, folder):
        """The S3 secret DuckDB reads the store with: boto3's current
        credentials (an assumed role included), scoped to the bucket."""
        from parquet_checkpoint import _s3
        bucket = folder[len("s3://"):].split("/", 1)[0]
        session = _s3.session()
        c = session.get_credentials()
        if c is None:
            raise RuntimeError(f"{self.name}: boto3 found no AWS "
                               f"credentials to read s3://{bucket}")
        c = c.get_frozen_credentials()
        token = f", SESSION_TOKEN '{c.token}'" if c.token else ""
        region = session.region_name or "us-east-1"
        try:
            self._cur.execute(
                f"CREATE OR REPLACE SECRET __sp_s3_{_safe(bucket)} (TYPE s3, "
                f"KEY_ID '{c.access_key}', SECRET '{c.secret_key}'{token}, "
                f"REGION '{region}', SCOPE 's3://{bucket}')")
        except Exception as e:
            msg = str(e)
            for v in (c.access_key, c.secret_key, c.token):
                if v:
                    msg = msg.replace(v, "<redacted>")
            raise RuntimeError(f"{self.name}: DuckDB refused the S3 secret "
                               f"made from boto3's credentials: {msg}") from None

    # ---- staying current ----------------------------------------------------

    def _wait_s(self):
        """Seconds until the ticket's next expected write, plus a margin."""
        t = self._ticket
        if not t:
            return POLL_DEFAULT_S
        beat = t.get("beat_seconds") or POLL_DEFAULT_S
        due = float(t.get("ts", 0)) + beat + POLL_MARGIN_S - time.time()
        return min(max(due, POLL_MIN_S), beat + POLL_MARGIN_S)

    def _loop(self):
        src = self._source
        while not self._stop.is_set():
            wait = self._wait_s()
            if src.revisions is not None:
                src.revisions.wait_ticket_revision(self._seen_revision, wait)
            else:
                self._stop.wait(wait)
            if self._stop.is_set():
                break
            try:
                self.refresh()
                if self.error is not None:
                    _diag.info(f"[{src.name}] view {self.name} current again")
                self.error = None
            except Exception as e:
                if self.error is None:
                    _diag.error(f"[{src.name}] view {self.name} could not "
                                f"follow the ticket: {e}")
                self.error = str(e)
                self._stop.wait(POLL_MIN_S)

    def close(self):
        """Stop following the ticket, drop the view and its catalog copies."""
        if self._stop.is_set():
            return
        self._stop.set()
        rev = self._source.revisions
        if rev is not None and hasattr(rev, "_revision_cond"):
            with rev._revision_cond:
                rev._revision_cond.notify_all()
        self._thread.join(timeout=10.0)
        with _views_lock:
            _views.discard(self)
        with self._lock:
            try:
                self._cur.execute(f'DROP VIEW IF EXISTS "{self.name}"')
                for _e, alias, _p in self._catalogs:
                    self._cur.execute(f'DETACH DATABASE IF EXISTS "{alias}"')
                for c in (self._con, self._cur):
                    for n in (self._mem_name, self._empty_name):
                        try:
                            c.unregister(n)
                        except Exception:
                            pass
                self._cur.close()
            except Exception as e:
                _diag.warning(f"[{self._source.name}] view {self.name} "
                              f"close: {e}")
            self._catalogs = []
        shutil.rmtree(self._tmp, ignore_errors=True)


def _fetch_catalog(folder, etag, dest):
    """Copy the catalog version `etag` (None: the current one) to
    `dest`. Returns the version copied, or None when the store holds no
    catalog or (etag given) another version."""
    if folder.startswith("s3://"):
        import botocore.exceptions
        from parquet_checkpoint import _s3
        bucket, _, prefix = folder[len("s3://"):].partition("/")
        kw = {"Bucket": bucket, "Key": prefix + CATALOG}
        if etag is not None:
            kw["IfMatch"] = etag
        try:
            r = _s3.timed("get", kw["Key"],
                          lambda: _s3.client(_s3.TRANSFER).get_object(**kw))
        except botocore.exceptions.ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in ("PreconditionFailed", "412", "NoSuchKey", "404"):
                return None
            raise
        data, got = r["Body"].read(), r["ETag"]
    else:
        path = os.path.join(folder, CATALOG)
        try:
            fd = os.open(path, os.O_RDONLY)
        except FileNotFoundError:
            return None
        try:
            st = os.fstat(fd)           # the version of the file opened
            got = f"{st.st_size}-{st.st_mtime_ns}"
            if etag is not None and got != etag:
                return None
            chunks = []
            while True:
                b = os.read(fd, 1 << 20)
                if not b:
                    break
                chunks.append(b)
            data = b"".join(chunks)
        finally:
            os.close(fd)
    tmp = dest + ".tmp"
    with open(tmp, "wb") as f:
        f.write(data)
    os.replace(tmp, dest)
    return got
