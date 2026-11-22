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
oldest quarter (the core: sp_view_seam). A view uses the newest three quarters of
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

The rules are the core's (src/core/sp.h: THE VIEW): the seam, which
checkpoint version the text names, the text itself, and when to look
again. This module reads the ticket, copies and attaches the catalog,
runs the statement and the thread.

A view lives until view.close() or the end of the process. Closing its
stream or its reader does not close it: the view goes on answering from
the store, and looks at the store on the schedule above.

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

from datetime import datetime, timezone

from . import _core, _tickets
from ._catalog import CATALOG
from ._diag import _diag

#: Catalog copies kept attached: the current one and those a query
#: still running may hold.
KEEP_CATALOGS = 3


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

    def __init__(self, chunks, schema, run=None):
        """run: a callable that answers the run the ring's rows belong
        to; given, every row also has that run's start time in the
        column sp_run_start_time (row ids, src/core/sp.h: ROW IDS)."""
        self._chunks = chunks
        self._run = run
        if run is not None:
            schema = schema.append(pa.field(_core.ROW_ID_RUN, _RUN_TYPE))
        self._schema = (schema.append(pa.field('_seq', pa.int64()))
                        .append(pa.field('_first', pa.int64())))

    def __arrow_c_stream__(self, requested_schema=None):
        try:
            it = self._chunks(0)        # from the ring's oldest row
        except Exception:
            it = ()
        started = None
        if self._run is not None:
            started = pa.scalar(_run_time(self._run()), _RUN_TYPE)
        return pa.RecordBatchReader.from_batches(
            self._schema, _Numbered(it, started)
        ).__arrow_c_stream__(requested_schema)


_RUN_TYPE = pa.timestamp("us", "UTC")


def refuse_row_id_columns(stream, schema):
    """row_ids=True on a stream that has a column of one of the two
    names is refused: the view would have two columns of that name."""
    if schema is None:
        return
    taken = [n for n in (_core.ROW_ID_RUN, _core.ROW_ID_ROW) if n in schema.names]
    if taken:
        raise ValueError(
            f"row_ids=True on the stream {stream!r}: the stream has a "
            f"column named {' and '.join(taken)} already, and row ids add "
            f"a column of that name. Make the view without row_ids, or "
            f"name the stream's column another way.")


def _run_time(run):
    """A run (the writer's start, '2026-11-21T23:58:06.066487Z') as a
    datetime; None for no run or text that is not one."""
    if not run:
        return None
    try:
        return datetime.strptime(run, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
            tzinfo=timezone.utc)
    except ValueError:
        return None


class _Numbered:
    """(first_seq, batch) pairs as batches with _seq and _first. A class
    with __next__, not a generator: a generator's loop variable would
    hold each batch, and its chunk's pin, until the next pull."""

    def __init__(self, pairs, started=None):
        self._pairs = iter(pairs)
        self._first = None
        self._started = started         # a scalar: the row id column's value

    def __iter__(self):
        return self

    def __next__(self):
        first, batch = next(self._pairs)
        if self._first is None:
            self._first = first
        n = batch.num_rows
        if self._started is not None:
            batch = batch.append_column(_core.ROW_ID_RUN,
                                        pa.repeat(self._started, n))
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

    def __init__(self, con, source, name, schema, with_memory, row_ids=False):
        from . import _extension
        if row_ids:
            refuse_row_id_columns(source.name, schema)
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
                    c.register(self._mem_name, _MemoryProxy(
                        source.memory, schema,
                        run=source.run if row_ids else None))
        self._tmp = tempfile.mkdtemp(prefix="sp-view-")
        self._lock = threading.Lock()
        self._k = 0
        self._catalogs = []             # [(etag, alias, path)], newest last
        # what the text names, the text, when to look again: the core's
        self._rules = _core.View(
            name, source.name,
            self._mem_name if self._with_memory else None,
            self._empty_name if schema is not None else None,
            row_ids=row_ids)
        self._seen_revision = None
        self.seam = None                # the row the text changes at
        self.rebuilds = 0               # times the view's text was replaced
        self.error = None               # the last refresh's failure
        self._stop = threading.Event()
        self.refresh()                  # the first build raises
        with _views_lock:
            _views.add(self)
        self._thread = threading.Thread(target=self._loop, daemon=True,
                                        name=f"view-{sid}")
        self._thread.start()

    @property
    def ring_note(self):
        """Why the ring is not used as wanted, in words; None when it is."""
        return self._rules.ring_note

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
        plan = self._rules.plan(
            t, [_file_of(f["checkpoint"]) for f in (t or {}).get("files") or ()],
            src.run(), self._window() if self._with_memory else None,
            time.time(), time.monotonic(), parquet_checkpoint.dead_after(),
            parquet_checkpoint.SUPERSEDED_KEEP_S)
        self._say()
        if not plan.build:
            return
        catalog = None
        if plan.catalog == _core.CATALOG_EXACT:
            catalog = self._catalog(plan.etag, exact=True)
        elif plan.catalog == _core.CATALOG_CURRENT:
            catalog = self._catalog(None, exact=False)
        sql = self._rules.sql(catalog)  # nothing to read: ValueError
        if src.folder.startswith("s3://"):
            self._give_duckdb_s3(src.folder)
        self._cur.execute(sql)
        self._rules.built()
        self.seam = plan.seam
        self.rebuilds += 1
        self._detach_old()

    def _say(self):
        for level, text in self._rules.messages():
            (_diag.error if level == _core.LOG_ERROR else
             _diag.warning if level == _core.LOG_WARNING else _diag.info)(text)

    def _window(self):
        """The ring's (oldest row, newest row, capacity), or None."""
        if self._source.window is None:
            return None
        try:
            return self._source.window()
        except Exception:
            return None                 # a reader not yet connected

    def _catalog(self, etag, exact):
        """The alias of the attached catalog copy for `etag` (None when
        the ticket names none). exact=False: the store's current
        catalog, whatever its version (no publisher, no ticket)."""
        if exact and etag is None:
            return None
        if exact and self._catalogs and self._catalogs[-1][0] == etag:
            return self._catalogs[-1][1]
        self._k += 1
        path = os.path.join(self._tmp, f"catalog.{self._k}.sqlite")
        got = _fetch_catalog(self._source.folder, etag if exact else None, path)
        if got is None:
            if exact:
                raise _Gone(f"catalog {etag}")
            return None                 # no catalog published yet
        if not exact and self._catalogs and self._catalogs[-1][0] == got:
            os.remove(path)
            return self._catalogs[-1][1]
        alias = f"__sp_cat_{self._sid}_{self._k}"
        self._cur.execute(f"ATTACH 'ducklake:sqlite:{path}' AS \"{alias}\" (READ_ONLY)")
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

    def _loop(self):
        src = self._source
        while not self._stop.is_set():
            with self._lock:
                wait = self._rules.wait_seconds(time.time())
            if src.revisions is not None:
                seen = self._seen_revision
                got = src.revisions.wait_ticket_revision(
                    seen, wait, self._stop.is_set)
                if got is not None and got == seen:
                    # the source closed: no revision comes from it again
                    self._stop.wait(wait)
            else:
                self._stop.wait(wait)
            if self._stop.is_set():
                break
            try:
                self.refresh()
                self.error = None
                with self._lock:
                    self._rules.followed()
                    self._say()
            except Exception as e:
                self.error = str(e)
                with self._lock:
                    wait = self._rules.failed(e)
                    self._say()
                self._stop.wait(wait)

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
