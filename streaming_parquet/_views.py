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
                    (read_parquet('pqc://<path>'))
  memory            duckdb_all only: the reader's ring, rows after the
                    ticket's stored_row (same run only)

The view's text names all three, so a query never mixes two tickets.
When the ticket changes, our code replaces the view in one statement
(CREATE OR REPLACE VIEW, on its own cursor of the caller's database).
One background thread per view decides when: a live reader learns of
every ticket write from the revision on its connection; a reader
without one reads the ticket on the schedule its write time and beat
give. A catalog or checkpoint version the store no longer holds means a
newer ticket exists: the view reads the ticket again.

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
    """

    def __init__(self, name, store, ticket=None, revisions=None, run=None,
                 memory=None):
        self.name = name
        self.store = store
        self.ticket = ticket or (lambda: _tickets.get_ticket(store, name))
        self.revisions = revisions
        self.run = run or (lambda: None)
        self.memory = memory
        folder = _tickets.archive_dir(store, name)
        if folder is None:
            raise ValueError(
                f"{name!r}: the archive views read the store, and this "
                f"reader has none: connect(name, store=...), or pass store=")
        self.folder = folder if folder.endswith("/") else folder + "/"


class _MemoryProxy:
    """Per query: the ring's committed rows after `floor()`, with their
    row numbers in _seq (the view keeps the rows after its ticket's
    stored_row; floor is the oldest such seam a view version still in
    use names). Read a chunk at a time through Ring.iter_chunks, zero
    copy: nothing is pinned until the scan reaches memory, which in a
    query over the store may be seconds in, and then only the chunk in
    hand. (2026-10-27 a whole-window pin held for such a query stalled
    the writer; a copy patched it; the chunk read retires the copy.)
    A reader with no connection yet scans an empty memory."""

    def __init__(self, chunks, schema, floor):
        self._chunks = chunks
        self._floor = floor
        self._schema = schema.append(pa.field('_seq', pa.int64()))

    def __arrow_c_stream__(self, requested_schema=None):
        try:
            it = self._chunks(self._floor() + 1)
        except Exception:
            it = ()
        # map, not a generator: a generator's loop variable would hold
        # each batch, and its chunk's pin, until the next pull.
        return pa.RecordBatchReader.from_batches(
            self._schema, map(_seq_column, it)).__arrow_c_stream__(requested_schema)


def _seq_column(pair):
    first, batch = pair
    return batch.append_column(
        '_seq', pa.array(range(first, first + batch.num_rows), type=pa.int64()))


def _with_seq(table, start):
    return table.append_column(
        '_seq', pa.array(range(start, start + table.num_rows), type=pa.int64()))


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
        self._seams = [-1]              # the seams of view versions in use
        self._empty_name = f"__sp_empty_{sid}"
        if schema is not None:
            empty = pa.table({f.name: pa.array([], type=f.type) for f in schema})
            for c in (con, self._cur):
                c.register(self._empty_name, empty)
                if self._with_memory:
                    c.register(self._mem_name,
                               _MemoryProxy(source.memory, schema,
                                            self._memory_floor))
        self._tmp = tempfile.mkdtemp(prefix="sp-view-")
        self._lock = threading.Lock()
        self._k = 0
        self._catalogs = []             # [(etag, alias, path)], newest last
        self._built = None              # the ticket facts the view names
        self._ticket = None
        self._seen_revision = None
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
        unfinished = tuple(t.get("unfinished") or ()) if t else ()
        run = t.get("run") if t else None
        seam = t.get("stored_row", -1) if t else -1
        memory_ok = (self._with_memory and live and run is not None
                     and run == src.run())
        facts = (etag if live else "current", unfinished, run, seam, memory_ok)
        if facts == self._built and live:
            return
        catalog = self._catalog(etag, exact=live)
        pieces = []
        if catalog is not None:
            pieces.append(f'SELECT * FROM "{catalog}".data')
        if unfinished:
            paths = ", ".join("'pqc://" + p.replace("'", "''") + "'"
                              for p in unfinished)
            pieces.append(f"SELECT * FROM read_parquet([{paths}], "
                          f"union_by_name=true)")
        if memory_ok:
            pieces.append(f'SELECT * EXCLUDE (_seq) FROM "{self._mem_name}" '
                          f'WHERE _seq > {int(seam)}')
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
        if memory_ok:
            # before the text naming it: a query on the new text must
            # find its rows in the memory piece
            self._seams = (self._seams + [int(seam)])[-(KEEP_CATALOGS + 1):]
        self._cur.execute(f'CREATE OR REPLACE VIEW "{self.name}" AS {sql}')
        self._built = facts
        self._detach_old()

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

    def _memory_floor(self):
        """The oldest seam a view version still in use names."""
        return min(self._seams)

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
