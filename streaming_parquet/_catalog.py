"""The catalog: one DuckLake catalog per stream, listing its finished files.

    <store>/streams/<name>/catalog.ducklake

A DuckDB database file in DuckLake's format. One table, `data`, whose
files are the stream's finished parquet files, registered in place by
their full path (DuckLake records the path as given). DuckDB reads only
the files a query needs: the catalog holds each file's row count and
each column's minimum and maximum.

The writer keeps a working copy on local disk and publishes the whole
file after registering new files. On a bucket the upload succeeds only
if the stored object's ETag is still the one this writer last saw (a
create-only upload the first time); a refusal means someone else
changed it, so the writer fetches it again, reconciles and retries. A
directory store's catalog is replaced atomically (os.replace), so a
reader holding the old file keeps reading the old file.

The working copy has 16 KB blocks, DuckDB's smallest. With DuckDB's
default 256 KB blocks the catalogs were 6.8-8.4 MB for 0.1-0.2 MB of
rows, mostly part-used and free blocks; with 16 KB blocks the same
catalogs are 0.4-0.7 MB, and every registration uploads the whole file
(2026-11-03). A catalog published with other blocks is copied into
16 KB blocks once, on its fetch, and published.

The files are the source of truth. Every pass reconciles: it lists the
stream's finished files in the store and registers each one the
catalog lacks, so a crash between a file finishing and its catalog
upload, a leftover finished at start, and the files already in a
bucket all come out the same way.

Catalog work is slow (each registration reads a file's footer from the
store; a first pass over a full bucket takes about a minute) so each
pass runs on a thread of its own, never on the worker pool that carries
Flight pushes and parquet writes. A stream has at most one pass queued
or running; different streams' passes run at the same time (one thread
for all of them made the streams' uploads wait on each other: 4 s of
one publisher's exit on 2026-11-02). A
pass with many files reports its progress every few seconds, on the
console and in the log. A stream's close stops its pass between two
files (stop()); the next start's reconcile finishes the work. The
process must never exit while the catalog thread is inside DuckDB: a
thread stopped there aborts the process ("terminate called ...
exception not rethrown", 2026-10-27 on the box).

Dated copies, catalog.<YYYYMMDD-HHMMSS>.ducklake beside it, keep the
catalog's history: once the copy interval has passed since the newest
copy, the next pass copies the published catalog, but only if it
differs from that copy (a bucket copies the object itself; nothing is
uploaded twice). The interval counts from the newest copy's own name,
so a restart makes no extra copy.

A file whose columns the table lacks gets them added first
(ALTER TABLE ADD COLUMN), so no column is ever hidden; a file missing a
column registers with nulls in it; a column whose type changed refuses
the file, loudly, in archive_state().catalog_error.
"""

import hashlib
import os
import re
import threading
import time
from datetime import datetime

from ._diag import _diag, console_print

CATALOG = "catalog.ducklake"
TABLE = "data"
_COPY = re.compile(r"^catalog\.(\d{8}-\d{6})\.ducklake$")


class Conflict(Exception):
    """The published catalog changed since this writer last saw it."""


class _Stopped(Exception):
    """The stream's close stopped the pass between two files."""

    def __init__(self, done, total):
        super().__init__(f"stopped by close after {done} of {total} file(s)")
        self.done, self.total = done, total


#: The working copy's DuckDB block size: DuckDB's smallest.
BLOCK_SIZE = 16 * 1024

#: A pass registering more files than this reports progress.
PROGRESS_FILES = 10
#: Seconds between progress reports.
PROGRESS_S = 5.0


class _S3Published:
    """The catalog object in a bucket, read and replaced by ETag."""

    def __init__(self, bucket, key):
        import botocore.exceptions
        from parquet_checkpoint import _s3
        self.bucket, self.key = bucket, key
        self._session = _s3.session()
        self._timed = _s3.timed
        self._list_keys = _s3.list_keys
        # the catalog file moves bytes; its HEAD is QUICK
        self._transfer = _s3.client(_s3.TRANSFER)
        self._quick = _s3.client(_s3.QUICK)
        self._client_error = botocore.exceptions.ClientError

    def duckdb_secret(self):
        """(sql, values): CREATE SECRET for DuckDB's S3 reads, from boto3's credentials:
        the ones this catalog's own S3 calls use. DuckDB's own credential
        chain refuses an assume-role profile (role_arn + source_profile,
        the box's): "Secret Validation Failure" (2026-10-27). boto3
        assumes the role and renews it; each pass takes the current
        set."""
        c = self._session.get_credentials()
        if c is None:
            raise RuntimeError(
                f"catalog s3://{self.bucket}/{self.key}: boto3 found no "
                f"AWS credentials for DuckDB's S3 reads")
        c = c.get_frozen_credentials()
        token = f", SESSION_TOKEN '{c.token}'" if c.token else ""
        return (f"CREATE SECRET (TYPE s3, KEY_ID '{c.access_key}', "
                f"SECRET '{c.secret_key}'{token}, "
                f"REGION '{self._quick.meta.region_name}')"), \
            [v for v in (c.access_key, c.secret_key, c.token) if v]

    def give_duckdb(self, con):
        """Run duckdb_secret() on `con`. A refusal is reported with the
        credential values cut out: DuckDB quotes the failing SQL."""
        import duckdb
        sql, values = self.duckdb_secret()
        try:
            con.execute(sql)
        except duckdb.Error as e:
            msg = str(e)
            for v in values:
                msg = msg.replace(v, "<redacted>")
            raise RuntimeError(
                f"catalog s3://{self.bucket}/{self.key}: DuckDB refused "
                f"the S3 secret made from boto3's credentials: {msg}") from None

    def get(self):
        """(bytes, etag), or None when there is no catalog yet."""
        try:
            r = self._timed("get", self.key, lambda: self._transfer.get_object(
                Bucket=self.bucket, Key=self.key))
        except self._client_error as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return None
            raise
        return r["Body"].read(), r["ETag"]

    def put(self, data, etag):
        """Replace the catalog if it is still `etag` (None: if there is
        none yet); returns the new ETag, or raises Conflict."""
        cond = {"IfMatch": etag} if etag else {"IfNoneMatch": "*"}
        try:
            r = self._timed("put", self.key, lambda: self._transfer.put_object(
                Bucket=self.bucket, Key=self.key, Body=data, **cond),
                detail=f" {len(data)}B")
        except self._client_error as e:
            if e.response["Error"]["Code"] in ("PreconditionFailed",
                                               "ConditionalRequestConflict"):
                raise Conflict(f"s3://{self.bucket}/{self.key}") from None
            raise
        return r["ETag"]

    def content_id(self, name):
        """The object `name` (in the catalog's folder)'s ETag, or None."""
        try:
            key = self._key(name)
            return self._timed("head", key, lambda: self._quick.head_object(
                Bucket=self.bucket, Key=key))["ETag"]
        except self._client_error as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return None
            raise

    def copy(self, name):
        """Copy the catalog to `name` in its folder, inside the bucket."""
        key = self._key(name)
        self._timed("copy", key, lambda: self._transfer.copy_object(
            Bucket=self.bucket, Key=key,
            CopySource={"Bucket": self.bucket, "Key": self.key}))

    def list_names(self):
        prefix = self.key[:-len(CATALOG)]
        return [k[len(prefix):] for k in
                self._list_keys(self.bucket, prefix, delimiter="/")]

    def _key(self, name):
        return self.key[:-len(CATALOG)] + name


class _DirPublished:
    """The catalog file in a directory store, replaced atomically. Its
    ETag is size and modification time: one writer per stream (_owner)
    makes a comparison enough."""

    def __init__(self, path):
        self.path = path

    def _etag(self):
        try:
            st = os.stat(self.path)
        except FileNotFoundError:
            return None
        return f"{st.st_size}-{st.st_mtime_ns}"

    def get(self):
        etag = self._etag()
        if etag is None:
            return None
        with open(self.path, "rb") as f:
            return f.read(), etag

    def put(self, data, etag):
        if self._etag() != etag:
            raise Conflict(self.path)
        tmp = self.path + ".tmp"
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)
        return self._etag()

    def content_id(self, name):
        p = os.path.join(os.path.dirname(self.path), name)
        try:
            with open(p, "rb") as f:
                return hashlib.md5(f.read()).hexdigest()
        except FileNotFoundError:
            return None

    def copy(self, name):
        import shutil
        dest = os.path.join(os.path.dirname(self.path), name)
        shutil.copyfile(self.path, dest + ".tmp")
        os.replace(dest + ".tmp", dest)

    def list_names(self):
        return os.listdir(os.path.dirname(self.path))


class Catalog:
    """One stream's catalog, as its writer maintains it.

    folder:    the stream's folder in the store, ending in '/': a local
               directory, or s3://bucket/streams/<name>/.
    work_path: the local working copy.
    retry_s:   after a failed pass, the next one waits this long.
    copy_s:    the dated copies' interval; None = no copies.
    """

    def __init__(self, name, folder, work_path, retry_s, copy_s):
        self.name = name
        self.folder = folder
        self._work = work_path
        self._retry_s = retry_s
        self._copy_s = copy_s
        self._copies_scanned = False    # the folder's dated copies looked at once
        self._newest_copy = None        # the newest dated copy's name
        self._copy_tw = None            # its time (from its name)
        self._copy_seen = None          # catalog ETag at the last copy or check
        if folder.startswith("s3://"):
            bucket, _, prefix = folder[len("s3://"):].partition("/")
            self._published = _S3Published(bucket, prefix + CATALOG)
            self._bucket, self._prefix = bucket, prefix
        else:
            self._published = _DirPublished(os.path.join(folder, CATALOG))
            self._bucket = None
        self._lock = threading.Lock()
        self._wanted = True             # a pass is owed (start: reconcile)
        self._queued = False
        self._fetched = False           # working copy == the published one
        self._failed_mono = None
        self._done = threading.Event()
        self.etag = None                # the published catalog's, after our last upload
        self.error = None               # the last pass's failure, None when fine
        self.files = 0                  # files the published catalog lists
        self.progress = (0, 0)          # (registered, to register) in this pass
        # called on the catalog thread after each pass that leaves a
        # published catalog, with the names of every file it lists
        self.on_listed = None
        self._listed = set()
        self._stop = threading.Event()  # set by the stream's close

    # ---- what the stream calls -------------------------------------------

    def set_cadence(self, retry_s, copy_s):
        """The retry wait and the dated copies' interval, from now on."""
        self._retry_s = retry_s
        self._copy_s = copy_s

    def stop(self):
        """The stream is closing: a running pass stops after its current
        file, and a queued one does not start. Nothing is published; the
        next start's reconcile does the work."""
        self._stop.set()

    def file_finished(self):
        """A file finished in the store: a pass is owed."""
        with self._lock:
            self._wanted = True
        self.poke()

    def poke(self):
        """Queue a pass on the catalog thread if one is owed and none is
        queued (after a failure, not before retry_s)."""
        with self._lock:
            if self._queued or not (self._wanted or self._copy_due()):
                return
            if (self._failed_mono is not None and
                    time.monotonic() - self._failed_mono < self._retry_s):
                return
            self._queued = True
            self._done.clear()
        self._start()

    def _start(self):
        threading.Thread(target=self.run, name=f"catalog {self.name}",
                         daemon=True).start()

    def wait(self, timeout):
        """Until no pass is queued or running and none is owed (True), or
        timeout (False)."""
        with self._lock:
            if not self._queued:
                return True
        return self._done.wait(timeout)

    def _copy_due(self):
        """The interval has passed since the newest copy and the catalog
        changed since the last copy or check (a cheap test, no I/O)."""
        if self._copy_s is None or self.etag is None:
            return False
        if self.etag == self._copy_seen:
            return False
        return (self._copy_tw is None or
                _clock_now() - self._copy_tw >= self._copy_s)

    def _copy_pass(self):
        """A dated copy of the published catalog, when due and it differs
        from the newest copy."""
        if self._copy_s is None:
            return
        if not self._copies_scanned:
            stamps = sorted(m.group(1) for m in map(
                _COPY.match, self._published.list_names()) if m)
            self._copies_scanned = True
            if stamps:
                self._newest_copy = f"catalog.{stamps[-1]}.ducklake"
                self._copy_tw = time.mktime(
                    datetime.strptime(stamps[-1], "%Y%m%d-%H%M%S").timetuple())
        if not self._copy_due():
            return
        if self._newest_copy is not None and (
                self._published.content_id(CATALOG) ==
                self._published.content_id(self._newest_copy)):
            self._copy_seen = self.etag     # unchanged since that copy
            return
        now_tw = _clock_now()
        name = f"catalog.{datetime.fromtimestamp(now_tw).strftime('%Y%m%d-%H%M%S')}.ducklake"
        self._published.copy(name)
        self._newest_copy, self._copy_tw, self._copy_seen = name, now_tw, self.etag
        _diag.info(f"[{self.name}] catalog: dated copy {self.folder}{name}")

    # ---- the pass, on the catalog thread ------------------------------------

    def run(self):
        with self._lock:
            self._wanted = False
        if self._stop.is_set():
            with self._lock:
                self._queued = False
                self._done.set()
            return
        try:
            for _ in range(3):
                try:
                    self._pass()
                    break
                except Conflict as e:
                    _diag.warning(f"[{self.name}] catalog: {e} changed since "
                                  f"this writer's last upload; fetching it "
                                  f"again and reconciling")
                    self._fetched = False
                except _Stopped as e:
                    msg = (f"[{self.name}] catalog pass {e}; the next "
                           f"start's reconcile finishes it")
                    _diag.info(msg)
                    console_print(f"[streaming-parquet] {msg}")
                    # registrations in the working copy, never published
                    self._fetched = False
                    break
            else:
                raise RuntimeError("the published catalog changed under "
                                   "three passes in a row: another writer "
                                   "is updating it")
            if self.error is not None:
                _diag.info(f"[{self.name}] catalog recovered")
            self.error = None
            self._failed_mono = None
            if self.on_listed is not None and not self._stop.is_set():
                try:
                    self.on_listed(set(self._listed))
                except Exception as e:
                    _diag.error(f"[{self.name}] after the catalog pass: {e}")
        except Exception as e:
            if self.error is None:
                _diag.error(f"[{self.name}] catalog failing: {e}")
            self.error = str(e)
            self._failed_mono = time.monotonic()
            # the working copy may hold registrations never published:
            # the next pass starts again from the published catalog
            self._fetched = False
            with self._lock:
                self._wanted = True
        finally:
            # a file that finished while this pass ran owes another pass:
            # requeue it in the same step, so a waiter (close) waits for it
            with self._lock:
                again = (self._wanted and self._failed_mono is None
                         and not self._stop.is_set())
                self._queued = again
                if not again:
                    self._done.set()
            if again:
                self._start()

    def _pass(self):
        import duckdb
        reblocked = False
        if not self._fetched:
            got = self._published.get()
            os.makedirs(os.path.dirname(self._work), exist_ok=True)
            for stale in (self._work, self._work + ".wal"):
                if os.path.exists(stale):
                    os.remove(stale)
            if got is not None:
                data, self.etag = got
                with open(self._work, "wb") as f:
                    f.write(data)
            else:
                self.etag = None
            reblocked = self._small_blocks()
            self._fetched = True
        finished = self._list_finished()
        con = duckdb.connect()
        try:
            if self._bucket is not None:
                self._published.give_duckdb(con)
            con.execute(f"ATTACH 'ducklake:{self._work}' AS lake "
                        f"(DATA_PATH '{self.folder}')")
            known = {r[0] for r in con.execute(
                "SELECT path FROM __ducklake_metadata_lake.ducklake_data_file "
                "WHERE end_snapshot IS NULL").fetchall()}
            todo = [p for p in finished if p not in known]
            self._register_all(con, todo)
            listed = len(known) + len(todo)
            names = {p.rsplit("/", 1)[-1] for p in known | set(todo)}
            # recent changes sit in the database's write-ahead log until
            # a checkpoint; the published copy is the main file alone
            con.execute("CHECKPOINT __ducklake_metadata_lake")
            con.execute("DETACH lake")
        finally:
            con.close()
        if not todo and not reblocked:
            self.files = listed
            if self.etag is not None:   # a published catalog lists them
                self._listed = names
            self._copy_pass()
            return
        if os.path.exists(self._work + ".wal"):
            raise RuntimeError(f"{self._work}.wal still exists after the "
                               f"checkpoint: the main file would miss changes; "
                               f"not publishing it")
        with open(self._work, "rb") as f:
            data = f.read()
        self.etag = self._published.put(data, self.etag)
        self.files = listed             # what the PUBLISHED catalog lists
        self._listed = names
        _diag.info(f"[{self.name}] catalog: registered {len(todo)} file(s), "
                   f"{listed} in all; published "
                   f"{self.folder}{CATALOG} ({len(data) / 1e6:.1f} MB)")
        self._copy_pass()

    def _small_blocks(self):
        """Give the working copy BLOCK_SIZE blocks: a new one is created
        with them; one with other blocks is copied into them. True when
        it was copied (the published catalog must be replaced)."""
        import duckdb
        con = duckdb.connect()
        try:
            if not os.path.exists(self._work):
                con.execute(f"ATTACH '{self._work}' AS new "
                            f"(BLOCK_SIZE {BLOCK_SIZE})")
                return False
            con.execute(f"ATTACH '{self._work}' AS old (READ_ONLY)")
            had = con.execute("SELECT block_size FROM pragma_database_size() "
                              "WHERE database_name = 'old'").fetchone()[0]
            if had == BLOCK_SIZE:
                return False
            tmp = self._work + ".blocks"
            for stale in (tmp, tmp + ".wal"):
                if os.path.exists(stale):
                    os.remove(stale)
            con.execute(f"ATTACH '{tmp}' AS new (BLOCK_SIZE {BLOCK_SIZE})")
            con.execute("COPY FROM DATABASE old TO new")
            con.execute("DETACH new")
        finally:
            con.close()
        before = os.path.getsize(self._work)
        os.replace(tmp, self._work)
        _diag.info(f"[{self.name}] catalog: copied from {had // 1024} KB "
                   f"blocks into {BLOCK_SIZE // 1024} KB blocks "
                   f"({before / 1e6:.1f} MB -> "
                   f"{os.path.getsize(self._work) / 1e6:.1f} MB); publishing it")
        return True

    def _register_all(self, con, todo):
        """Register `todo` (full paths), stopping between files when the
        stream closes; a long pass reports its progress."""
        total = len(todo)
        self.progress = (0, total)
        loud = total > PROGRESS_FILES
        t0 = last = time.monotonic()
        if loud:
            msg = (f"[{self.name}] catalog: registering {total} file(s) "
                   f"from {self.folder} (each file's footer is read)")
            _diag.info(msg)
            console_print(f"[streaming-parquet] {msg}")
        for i, path in enumerate(todo):
            if self._stop.is_set():
                raise _Stopped(i, total)
            self._register(con, path)
            self.progress = (i + 1, total)
            now = time.monotonic()
            if loud and now - last >= PROGRESS_S and i + 1 < total:
                last = now
                took = now - t0
                left = took / (i + 1) * (total - i - 1)
                msg = (f"[{self.name}] catalog: {i + 1} of {total} "
                       f"registered, {took:.0f} s so far, about "
                       f"{left:.0f} s left")
                _diag.info(msg)
                console_print(f"[streaming-parquet] {msg}")

    def _register(self, con, path):
        cols = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')"
                           ).fetchall()
        have = {r[0]: r[1] for r in con.execute(
            "SELECT column_name, data_type FROM duckdb_columns() "
            f"WHERE database_name = 'lake' AND table_name = '{TABLE}'"
        ).fetchall()}
        if not have:
            # explicit columns, not CREATE TABLE AS: that makes a
            # main/data/ folder under the data path, the stream's folder
            con.execute(f"CREATE TABLE lake.{TABLE} (" + ", ".join(
                f'"{col}" {typ}' for col, typ, *_ in cols) + ")")
        else:
            for col, typ, *_ in cols:
                if col not in have:
                    con.execute(f'ALTER TABLE lake.{TABLE} ADD COLUMN '
                                f'"{col}" {typ}')
        con.execute(f"CALL ducklake_add_data_files('lake', '{TABLE}', "
                    f"'{path}', allow_missing => true)")

    def _list_finished(self):
        """Full paths of the stream's finished files in the store, by name."""
        if self._bucket is None:
            names = [n for n in os.listdir(self.folder)
                     if n.endswith(".parquet")
                     and os.path.isfile(os.path.join(self.folder, n))]
            return [os.path.join(self.folder, n) for n in sorted(names)]
        from parquet_checkpoint import _s3
        names = [k[len(self._prefix):] for k in
                 _s3.list_keys(self._bucket, self._prefix, delimiter="/")]
        return [self.folder + n for n in sorted(names)
                if n.endswith(".parquet")]


def _clock_now():
    from ._clock import now
    return now()
