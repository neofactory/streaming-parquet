"""DuckDB checkpoint-extension test: stock read_parquet() over a LIVE
checkpoint through the registered filesystem.

Run after building:  GEN=ninja make release
                     python3 run_extension_tests.py
"""

import http.server
import os
import re
import subprocess
import sys
import tempfile
import threading

import duckdb
import pyarrow as pa

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from parquet_checkpoint import CheckpointReader, CheckpointWriter, DirSink

HERE = os.path.dirname(os.path.abspath(__file__))
EXT = os.path.join(HERE, 'build', 'release', 'extension', 'parquet_checkpoint',
                   'parquet_checkpoint.duckdb_extension')

F = pa.field
SCHEMA = pa.schema([F('n', pa.int64(), nullable=False),
                    F('x', pa.float64(), nullable=False),
                    F('s', pa.string(), nullable=False)])


def make_rows(start, count):
    return pa.table({'n': list(range(start, start + count)),
                     'x': [i * 0.5 for i in range(start, start + count)],
                     's': [f'row-{i}' for i in range(start, start + count)]},
                    schema=SCHEMA)


def many_threads_worker(queries):
    """One process: a checkpoint of 400 row groups in about 200 objects,
    read again and again on every DuckDB thread. Run apart from the
    suite because the fault it guards against (2026-11-09: the table of
    open objects had no lock, and DuckDB 2.0 reads one file handle from
    every scan thread) showed as a process that hung or died, not as an
    exception."""
    d = tempfile.mkdtemp(prefix='ckpt-ext-threads-')
    writer = CheckpointWriter('live.parquet', SCHEMA, sink=DirSink(d),
                              chunk_bytes=64 * 1024, compression='none')
    groups = 400
    for k in range(groups):
        writer.write_table(make_rows(k * 1000, 1000))
    writer.checkpoint()
    objects = len(os.listdir(os.path.join(d, 'live.parquet.chunks')))
    c = duckdb.connect(config={'allow_unsigned_extensions': 'true'})
    c.execute(f"LOAD '{EXT}'")
    threads = c.execute("SELECT current_setting('threads')").fetchall()[0][0]
    want = (groups * 1000, sum(range(groups * 1000)))
    ckpt = os.path.join(d, writer.ckpt_name)
    for i in range(queries):
        got = c.execute(f"SELECT count(*), sum(n), sum(x), count(s) "
                        f"FROM read_parquet('{ckpt}')").fetchall()[0]
        if got[:2] != want:
            print(f"query {i}: {got[:2]}, expected {want}")
            sys.exit(2)
    print(f"{queries} queries, {threads} threads, {objects} objects")


if len(sys.argv) == 3 and sys.argv[1] == '--many-threads':
    if not os.path.exists(EXT):
        sys.exit(f"extension not built at {EXT}")
    many_threads_worker(int(sys.argv[2]))
    sys.exit(0)


passed = failed = 0


def check(name, fn):
    global passed, failed
    try:
        fn()
        print(f"  PASS  {name}", flush=True)
        passed += 1
    except Exception as e:
        import traceback
        print(f"  FAIL  {name}: {e}", flush=True)
        traceback.print_exc()
        failed += 1


if not os.path.exists(EXT):
    print(f"FATAL: extension not built at {EXT} — run GEN=ninja make release")
    sys.exit(1)

con = duckdb.connect(config={'allow_unsigned_extensions': 'true'})
con.execute(f"LOAD '{EXT}'")

tmp = tempfile.mkdtemp(prefix='ckpt-ext-')
sink = DirSink(tmp)
w = CheckpointWriter('live.parquet', SCHEMA, sink=sink,
                     chunk_bytes=64 * 1024, compression='none')
for k in range(5):
    w.write_table(make_rows(k * 1000, 1000))
    w.checkpoint()


def cur():
    """The newest checkpoint version of live.parquet: each checkpoint is
    a new object."""
    return os.path.join(tmp, w.ckpt_name)


path = cur()            # the version after 5,000 rows


def live_read():
    n, mx = con.sql(
        f"SELECT count(*), max(n) FROM read_parquet('{path}')").fetchall()[0]
    assert (n, mx) == (5000, 4999), (n, mx)


def exact_vs_reader():
    t = con.sql(f"SELECT n, x, s FROM read_parquet('{path}') "
                f"ORDER BY n").to_arrow_table()
    ref = CheckpointReader(w.ckpt_name, sink).table()
    assert t.cast(ref.schema).equals(ref.sort_by('n')), "content differs"


def projection_and_filter():
    r = con.sql(f"SELECT s FROM read_parquet('{path}') "
                f"WHERE n = 4321").fetchall()[0]
    assert r == ('row-4321',), r


def grows_between_reads():
    w.write_table(make_rows(5000, 500))
    w.checkpoint()
    n, = con.sql(
        f"SELECT count(*) FROM read_parquet('{cur()}')").fetchall()[0]
    assert n == 5500, n


def older_version_reads_its_own_rows():
    # the version read first is still there (a superseded version stays
    # three) and still holds exactly its own rows: it never changes
    n, = con.sql(
        f"SELECT count(*) FROM read_parquet('{path}')").fetchall()[0]
    assert n == 5000, n


def pqc_scheme_wrapped():
    n, = con.sql(f"SELECT count(*) FROM "
                 f"read_parquet('pqc://{cur()}')").fetchall()[0]
    assert n == 5500, n


def mixed_list_closed_and_live():
    # a closed ordinary file and a live checkpoint in ONE query — the
    # union view a stream's history needs
    w2 = CheckpointWriter('old.parquet', SCHEMA, sink=sink,
                          compression='none')
    w2.write_table(make_rows(9000, 250))
    w2.close()   # finalized: ordinary parquet
    old = os.path.join(tmp, 'old.parquet')
    n, mx = con.sql(
        f"SELECT count(*), max(n) FROM "
        f"read_parquet(['{old}', '{cur()}'])").fetchall()[0]
    assert (n, mx) == (5750, 9249), (n, mx)


def finalized_still_reads():
    last = cur()
    w.close()    # live.parquet becomes an ordinary file
    p = os.path.join(tmp, 'live.parquet')
    n, = con.sql(f"SELECT count(*) FROM read_parquet('{p}')").fetchall()[0]
    assert n == 5500, n
    assert not os.path.exists(last), "finalize must delete the checkpoints"
    assert not [f for f in os.listdir(tmp) if f.endswith('.ckpt')], \
        os.listdir(tmp)


def missing_refuses_loudly():
    try:
        con.sql(f"SELECT * FROM read_parquet('{tmp}/nope.parquet.ckpt')")
        assert False, "read of a missing checkpoint succeeded"
    except duckdb.Error:
        pass


def many_threads_many_objects():
    try:
        r = subprocess.run(
            [sys.executable, os.path.abspath(__file__), '--many-threads', '200'],
            capture_output=True, text=True, timeout=300)
    except subprocess.TimeoutExpired:
        assert False, ("200 queries over 200 objects on every thread did "
                       "not finish in 300 s: the process hangs")
    assert r.returncode == 0, (
        f"the process ended with {r.returncode}: "
        f"{(r.stdout + r.stderr).strip()[-400:]}")


class RangeServer(http.server.ThreadingHTTPServer):
    """A directory over HTTP with byte ranges, and a record of every
    request: what DuckDB asks a store for, counted."""

    def __init__(self, directory):
        self.directory = directory
        self.requests = []              # (method, path, range or None)
        self.lock = threading.Lock()
        super().__init__(('127.0.0.1', 0), RangeHandler)
        threading.Thread(target=self.serve_forever, daemon=True).start()

    def url(self, name):
        return f"http://127.0.0.1:{self.server_address[1]}/{name}"

    def take(self):
        """The requests since the last take."""
        with self.lock:
            got, self.requests = self.requests, []
        return got


class RangeHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def log_message(self, *args):
        pass

    def _serve(self, body):
        rng = self.headers.get('Range')
        with self.server.lock:
            self.server.requests.append((self.command, self.path, rng))
        path = os.path.join(self.server.directory, self.path.lstrip('/'))
        if not os.path.isfile(path):
            self.send_response(404)
            self.send_header('Content-Length', '0')
            self.end_headers()
            return
        size = os.path.getsize(path)
        lo, hi = 0, size - 1
        m = re.match(r'bytes=(\d+)-(\d*)$', rng or '')
        if m:
            lo = int(m.group(1))
            hi = min(int(m.group(2)), size - 1) if m.group(2) else size - 1
            self.send_response(206)
            self.send_header('Content-Range', f'bytes {lo}-{hi}/{size}')
        else:
            self.send_response(200)
        self.send_header('Accept-Ranges', 'bytes')
        self.send_header('Content-Length', str(hi - lo + 1))
        self.send_header('Last-Modified',
                         self.date_time_string(os.path.getmtime(path)))
        self.end_headers()
        if body:
            with open(path, 'rb') as f:
                f.seek(lo)
                self.wfile.write(f.read(hi - lo + 1))

    def do_HEAD(self):
        self._serve(False)

    def do_GET(self):
        self._serve(True)


def remote_objects_are_read_once():
    # Over HTTP, as over S3: an object that never changes is fetched
    # once. A second query on the same checkpoint version asks the store
    # for nothing; a new version costs its own checkpoint object and the
    # objects it adds, never an object already read. (2026-11-09: every
    # query downloaded the checkpoint object three times and opened each
    # chunk again on each thread, because DuckDB caches nothing of a
    # pqc:// path.)
    try:
        con.execute("LOAD httpfs")
    except duckdb.Error as e:
        print(f"  SKIP  the httpfs extension does not load here ({e})")
        return
    d = tempfile.mkdtemp(prefix='ckpt-ext-http-')
    server = RangeServer(d)
    try:
        hw = CheckpointWriter('live.parquet', SCHEMA, sink=DirSink(d),
                              chunk_bytes=64 * 1024, compression='none')
        for k in range(20):
            hw.write_table(make_rows(k * 1000, 1000))
        hw.checkpoint(make_rows(20000, 300))
        v1 = hw.ckpt_name
        q = ("SELECT count(*), max(n), sum(x), count(s) "
             "FROM read_parquet('pqc://{}')")
        got = con.execute(q.format(server.url(v1))).fetchall()[0]
        assert got[:2] == (20300, 20299), got
        first = server.take()
        assert first, "the first query made no request: is the server used?"
        fetched = {path for _m, path, _r in first}

        got = con.execute(q.format(server.url(v1))).fetchall()[0]
        assert got[:2] == (20300, 20299), got
        again = server.take()
        assert not again, (
            f"the same query on the same checkpoint version made "
            f"{len(again)} request(s): {again[:6]}")

        got = con.execute(f"SELECT s FROM read_parquet('pqc://{server.url(v1)}') "
                          f"WHERE n = 12345").fetchall()
        assert got == [('row-12345',)], got
        other = server.take()
        assert not other, (
            f"another query on the same version made {len(other)} "
            f"request(s): {other[:6]}")

        for k in range(20, 26):
            hw.write_table(make_rows(k * 1000, 1000))
        hw.checkpoint(make_rows(26000, 40))
        v2 = hw.ckpt_name
        assert v2 != v1
        got = con.execute(q.format(server.url(v2))).fetchall()[0]
        assert got[:2] == (26040, 26039), got
        second = server.take()
        asked = {path for _m, path, _r in second}
        assert '/' + v2 in asked, f"the new version was not fetched: {asked}"
        repeated = asked & fetched
        assert not repeated, (
            f"the new version read again {len(repeated)} object(s) that "
            f"the cache holds: {sorted(repeated)[:6]}")

        # The extension keeps the last 8 checkpoint versions it read and
        # forgets the rest: a reader that follows a stream for days must
        # not hold every version it ever saw.
        count = "SELECT count(*) FROM read_parquet('pqc://{}')"
        versions = [v2]
        for k in range(9):
            hw.checkpoint(make_rows(26040 + k * 10, 10 * (k + 1)))
            versions.append(hw.ckpt_name)
            con.execute(count.format(server.url(hw.ckpt_name))).fetchall()
        server.take()
        con.execute(count.format(server.url(versions[-1]))).fetchall()
        assert not server.take(), "the version read last was fetched again"
        con.execute(count.format(server.url(versions[0]))).fetchall()
        asked = {path for _m, path, _r in server.take()}
        assert '/' + versions[0] in asked, (
            "a version read 9 versions ago was still kept")
    finally:
        server.shutdown()
        server.server_close()


check("live checkpoint via stock read_parquet()", live_read)
check("content exact vs CheckpointReader", exact_vs_reader)
check("projection + filter through the virtual file", projection_and_filter)
check("re-read sees checkpoint growth", grows_between_reads)
check("an older kept version still reads exactly its own rows", older_version_reads_its_own_rows)
check("pqc:// wrapped path", pqc_scheme_wrapped)
check("one query over closed file + live checkpoint", mixed_list_closed_and_live)
check("finalized file reads as plain parquet", finalized_still_reads)
check("missing checkpoint refuses loudly", missing_refuses_loudly)
check("200 queries on every thread over 200 objects", many_threads_many_objects)
check("a remote object is read once, across queries and versions",
      remote_objects_are_read_once)


# S3: the pqc://s3:// route goes through DuckDB's own filesystems
# (httpfs) with the query's opener, which carries the S3 secret. Needs a
# bucket: CKPT_TEST_STORE=s3://bucket; the objects go under
# streams/test/ckpt-extension/ and are deleted afterward.
store = os.environ.get('CKPT_TEST_STORE')
if store is None:
    print("  SKIP  S3 reads: CKPT_TEST_STORE is not set "
          "(e.g. CKPT_TEST_STORE=s3://bucket)")
else:
    from parquet_checkpoint import S3Sink
    bucket = store[len('s3://'):].strip('/')
    prefix = 'streams/test/ckpt-extension/'
    s3sink = S3Sink(bucket, prefix)
    con.execute("LOAD httpfs")
    con.execute("CREATE SECRET (TYPE s3, PROVIDER credential_chain)")
    # S3's part minimum (5 MB) is the smallest chunk: 250,000 rows make
    # at least one real chunk plus the checkpoint's own data
    w3 = CheckpointWriter('s3live.parquet', SCHEMA, sink=s3sink,
                          compression='none')
    for k in range(5):
        w3.write_table(make_rows(k * 50_000, 50_000))
    w3.checkpoint(tail=make_rows(250_000, 17))
    assert len(CheckpointReader(w3.ckpt_name, s3sink).manifest) >= 1, \
        "the S3 case wants at least one chunk"
    s3path = f"pqc://s3://{bucket}/{prefix}{w3.ckpt_name}"

    def s3_exact():
        t = con.sql(f"SELECT n, x, s FROM read_parquet('{s3path}') "
                    f"ORDER BY n").to_arrow_table()
        ref = CheckpointReader(w3.ckpt_name, s3sink).table()
        assert t.num_rows == 250_017, t.num_rows
        assert t.cast(ref.schema).equals(ref.sort_by('n')), "content differs"

    def s3_projection_and_filter():
        r = con.sql(f"SELECT s FROM read_parquet('{s3path}') "
                    f"WHERE n = 250016").fetchall()[0]
        assert r == ('row-250016',), r

    try:
        check("S3 live checkpoint exact vs CheckpointReader", s3_exact)
        check("S3 projection + filter", s3_projection_and_filter)
    finally:
        for name in s3sink.list(""):
            s3sink.delete(name)
        left = s3sink.list("")
        print(f"  S3 cleanup: {len(left)} object(s) left under "
              f"s3://{bucket}/{prefix}")

import shutil
shutil.rmtree(tmp, ignore_errors=True)
print(f"\nResults: {passed} passed, {failed} failed")
sys.exit(1 if failed else 0)
