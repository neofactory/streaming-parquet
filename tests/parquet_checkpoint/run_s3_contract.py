"""Real-S3 contract suite for parquet_checkpoint.

Run:  PQC_TEST_BUCKET=<bucket> python3 parquet_checkpoint/run_s3_contract.py

Verifies our understanding of AWS against the real service — part
minimums, range copies, overwrite replacement — plus ONE full
writer -> reader -> finalize lifecycle through S3Sink. Thin and rare
by design (the volume tests run on DirSink/FakeS3); run before
adoption and periodically. Needs credentials for the general account
(default profile) and a test bucket named by $PQC_TEST_BUCKET. Everything
runs under a unique per-run prefix and is deleted at the end; the bucket's 14-day
expiry + 7-day MPU-abort lifecycle rules mop up anything a crash
leaves.
"""

import os
import sys
import time
import traceback

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from parquet_checkpoint import (ckpt_versions,
                                CheckpointReader, CheckpointWriter,
                                S3Sink, finalize)
from parquet_checkpoint._sinks import MIN_PART, SinkError

BUCKET = os.environ.get('PQC_TEST_BUCKET')
if not BUCKET:
    sys.exit('run_s3_contract.py: set $PQC_TEST_BUCKET to the test bucket '
             '(14-day expiry and 7-day MPU-abort lifecycle rules)')
PREFIX = f"contract/{time.strftime('%Y%m%d-%H%M%S')}-{os.getpid()}"

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
errors = []


def test(name):
    def deco(fn):
        global passed, failed
        t0 = time.monotonic()
        try:
            fn()
            print(f"  PASS  {name}  ({time.monotonic() - t0:.1f}s)",
                  flush=True)
            passed += 1
        except Exception as e:
            print(f"  FAIL  {name}: {e}", flush=True)
            traceback.print_exc()
            failed += 1
            errors.append(name)
    return deco


sink = S3Sink(BUCKET, PREFIX)
print(f"contract run: s3://{BUCKET}/{PREFIX}/", flush=True)


@test("sink ops: put / get / ranged get / size / exists / list / delete")
def _():
    body = bytes(range(256)) * 8                 # 2 KB, position-coded
    sink.put('ops/a', body)
    assert sink.exists('ops/a')
    assert sink.size('ops/a') == len(body)
    assert sink.get('ops/a') == body
    assert sink.get('ops/a', offset=300, length=512) == body[300:812]
    assert sink.get('ops/a', offset=1000) == body[1000:]
    sink.put('ops/b', b'x')
    assert sink.list('ops/') == ['ops/a', 'ops/b']
    sink.delete('ops/b')
    assert sink.list('ops/') == ['ops/a']
    assert not sink.exists('ops/b')


@test("overwrite: PUT replaces the whole object, never blends")
def _():
    sink.put('ow/k', b'A' * 4096)
    sink.put('ow/k', b'B' * 100)                 # shorter replacement
    assert sink.get('ow/k') == b'B' * 100        # exactly new, no tail


@test("AWS enforces EntityTooSmall for a non-last part under 5MB")
def _():
    # raw MPU, bypassing our guard: confirm the rule is AWS's, not ours
    from botocore.exceptions import ClientError
    sink.put('rule/small1', b'a' * 1024)
    sink.put('rule/small2', b'b' * 1024)
    s3, key = sink._transfer, sink._k('rule/dest')
    uid = s3.create_multipart_upload(Bucket=BUCKET, Key=key)['UploadId']
    try:
        parts = []
        for n, src in ((1, 'rule/small1'), (2, 'rule/small2')):
            r = s3.upload_part_copy(
                Bucket=BUCKET, Key=key, UploadId=uid, PartNumber=n,
                CopySource={'Bucket': BUCKET, 'Key': sink._k(src)})
            parts.append({'PartNumber': n,
                          'ETag': r['CopyPartResult']['ETag']})
        try:
            s3.complete_multipart_upload(
                Bucket=BUCKET, Key=key, UploadId=uid,
                MultipartUpload={'Parts': parts})
            raise AssertionError("AWS accepted a 1KB non-last part")
        except ClientError as e:
            assert e.response['Error']['Code'] == 'EntityTooSmall', e
    finally:
        s3.abort_multipart_upload(Bucket=BUCKET, Key=key, UploadId=uid)
    assert not sink.exists('rule/dest')


@test("assemble: a large object is copied; the smaller ones after it "
      "travel once, as the last part; byte-exact")
def _():
    big = os.urandom(1024) * (MIN_PART // 1024 + 1)     # just over 5MB
    small = bytes(range(256)) * 8
    other = os.urandom(3000)
    sink.put('asm/big', big)
    sink.put('asm/small', small)
    sink.put('asm/other', other)
    sink.assemble('asm/dest', [('asm/big', -1, -1),
                               ('asm/small', 100, 512)])
    assert sink.get('asm/dest') == big + small[100:612]
    # a chunk, then three smaller sources: S3 cannot join small
    # objects, so they are fetched and sent together as one part
    sink.assemble('asm/tail', [('asm/big', -1, -1), ('asm/small', -1, -1),
                               ('asm/other', -1, -1), ('asm/small', 7, 90)])
    assert sink.get('asm/tail') == big + small + other + small[7:97]
    # a small source BEFORE a large one: the part takes the large one
    # in, so that no part before the last is under the minimum
    sink.assemble('asm/first', [('asm/small', -1, -1), ('asm/big', -1, -1)])
    assert sink.get('asm/first') == small + big
    # small sources alone
    sink.assemble('asm/small2', [('asm/small', -1, -1), ('asm/other', -1, -1)])
    assert sink.get('asm/small2') == small + other


@test("lifecycle: ~2 chunks, row groups and tail objects; remote read; "
      "finalize by copies and one uploaded part; exact")
def _():
    w = CheckpointWriter('run.parquet', SCHEMA, sink=sink,
                         compression='none', checkpoint_every_write=False)
    i = 0

    def chunks():
        return [m for m in CheckpointReader(w.ckpt_name, sink).manifest
                if m[1] >= MIN_PART]
    while w.ckpt_name is None or len(chunks()) < 2:
        w.write_table(make_rows(i, 60_000))
        w.checkpoint()
        i += 60_000
        assert i <= 1_200_000, "chunks never cut"
    w.write_table(make_rows(i, 60_000))          # and one that fills none
    i += 60_000
    # checkpoints of the rows after the last row group: an object for
    # each one's new rows, beside the chunks and the row groups that
    # do not fill a chunk yet
    for tail in (25, 60, 61, 140):
        w.checkpoint(tail=make_rows(i, tail))
    total = i + 140
    r0 = CheckpointReader(w.ckpt_name, sink)
    sizes = [m[1] for m in r0.manifest]
    assert len([x for x in sizes if x >= MIN_PART]) >= 2, sizes
    assert len([x for x in sizes if x < MIN_PART]) >= 2, sizes
    # "another machine": a fresh sink, only the bucket in common
    remote = S3Sink(BUCKET, PREFIX)
    r = CheckpointReader(ckpt_versions(remote, 'run.parquet')[-1][1], remote)
    assert r.num_rows == total
    t = r.table()
    assert t.num_rows == total
    assert t.column('n').to_pylist() == list(range(total))
    # crash here (writer abandoned); anyone finalizes
    status, name = finalize('run.parquet', remote)
    assert status == 'recovered'
    final = pq.read_table(pa.BufferReader(remote.get('run.parquet')))
    assert final.num_rows == total
    assert final.column('n').to_pylist() == list(range(total))
    assert final.column('s').to_pylist()[-1] == f'row-{total - 1}'
    residue = [f for f in remote.list('run.parquet')
               if f != 'run.parquet']
    assert not residue, residue                  # chunks + ckpt swept
    status, _ = finalize('run.parquet', remote)  # idempotent
    assert status == 'done'


n_cleaned = 0
for name in sink.list(''):
    sink.delete(name)
    n_cleaned += 1
print(f"cleaned {n_cleaned} objects under {PREFIX}/")

print(f"\n{'=' * 50}")
print(f"Results: {passed} passed, {failed} failed")
if errors:
    print(f"Failed: {', '.join(errors)}")
    sys.exit(1)
print("All tests passed!")
