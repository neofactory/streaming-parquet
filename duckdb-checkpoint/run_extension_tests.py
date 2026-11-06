"""DuckDB checkpoint-extension test: stock read_parquet() over a LIVE
checkpoint through the registered filesystem.

Run after building:  GEN=ninja make release
                     python3 run_extension_tests.py
"""

import os
import sys
import tempfile

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


check("live checkpoint via stock read_parquet()", live_read)
check("content exact vs CheckpointReader", exact_vs_reader)
check("projection + filter through the virtual file", projection_and_filter)
check("re-read sees checkpoint growth", grows_between_reads)
check("an older kept version still reads exactly its own rows", older_version_reads_its_own_rows)
check("pqc:// wrapped path", pqc_scheme_wrapped)
check("one query over closed file + live checkpoint", mixed_list_closed_and_live)
check("finalized file reads as plain parquet", finalized_still_reads)
check("missing checkpoint refuses loudly", missing_refuses_loudly)


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
