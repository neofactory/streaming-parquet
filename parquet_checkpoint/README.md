# parquet_checkpoint

Crash-durable, queryable-while-open parquet. One growing logical file
per stream/session instead of a file per flush; crash loss bounded to
rows since the last checkpoint (zero at any checkpoint instant — the
tail rides inside it); recovery is assembly, no data rewritten. Second
package of the streaming-parquet trio (arrow-ring / parquet-checkpoint
/ streaming-parquet); dependencies: pyarrow, and cffi with a C compiler
for the writer, whose format work is the C core's (boto3 for the real
S3 sink, duckdb in the tests). Designed 2026-08-19/22 with Paul.

## The format

Storage is behind a **sink** (a local directory or an S3 bucket —
same names, same bytes). Two kinds of artifact per live file:

| artifact | meaning |
|---|---|
| `name.parquet` | done — an ORDINARY parquet file |
| `name.parquet.chunks/000001` | a chunk: an immutable piece of the final file, cut at row-group boundaries, >= `chunk_bytes` (default 5 MB — the S3 multipart part minimum; a packaging size, never a durability boundary) |
| `name.parquet.000017.ckpt` | a checkpoint: version 17, one object, written once and never changed. Each checkpoint is a new version. A version superseded for 10 minutes (`SUPERSEDED_KEEP_S`) is deleted in the step that stores a later one, so a reader told of an older version less than that long ago still finds it, and an opened object never changes under it. Nothing runs on a timer. Recovery assembles from the newest version whose CRC holds. A `name.parquet.ckpt` without a version (written before versions) reads as version 0. |

Checkpoint layout:

```
+--------------------------------------------------------------+
| header:   magic PQC2, version, CRC-32 of everything after    |
|           the CRC field, final file name, watermark (rows    |
|           covered), data offset                              |
| manifest: ordered chunk list -- name, byte length, CRC-32    |
| data:     [magic if no chunks][buffered final row groups]    |
|           [provisional tail row group][footer]               |
+--------------------------------------------------------------+
```

The data region IS final-file bytes: finalize appends it verbatim
after the chunks (on S3, a server-side range copy of the checkpoint
object — no bytes travel to the host). The directory/prefix listing is
the state machine. Durability ordering is the contract: a chunk is
stored BEFORE any checkpoint that references it; the checkpoint
replace is atomic at both sinks, so the manifest, tail, and footer can
never disagree.

## API

Three things: a writer, a reader, and finalize. Each works alone.

```
sinks: DirSink(root)  FakeS3(part_min=5MB)  S3Sink(bucket, prefix='')
    # every S3 call of the library goes through _s3: class QUICK
    # (metadata, tickets) or TRANSFER (bytes); 1 s connect, 3 attempts
    # -- an unreachable S3 surfaces in seconds (the host retries next poll)
       put/get/size/exists/delete/list/assemble; part_min

CheckpointWriter(name, schema, sink=None, chunk_bytes=5MB,
                 checkpoint_every_write=True, **writer_options)
w.write_table(table)                  # final row groups; implies checkpoint()
w.checkpoint(tail=None)               # durability point; tail = a Table of
                                      #   rows not written anywhere yet
                                      #   (provisional: superseded by the
                                      #   next write_table)
w.rows_written  w.rows_checkpointed
w.close() -> name                     # = finalize on its own file

CheckpointReader(ckpt_name_or_bytes, sink)
r.table(columns=None) -> Table        # the live file as ordinary parquet
r.metadata -> FileMetaData
r.num_rows / r.watermark              # rows the checkpoint covers
r.manifest -> [(name, length, crc), ...]
r.verify() -> n_rows                  # O(data): chunk CRCs + page CRCs
r.assembly_plan()                     # [(name, offset, length), ...]
r.open() -> CheckpointFile            # the live file as a read-only
                                      #   seekable FILE OBJECT: hand it
                                      #   to stock parquet code
                                      #   (pq.read_table(r.open())).
                                      #   Ranged reads: column pruning
                                      #   reaches the storage. Footer
                                      #   pinned at reader construction.
                                      #   Chunks <= 8MB fetched whole
                                      #   (manifest CRC, loud); larger
                                      #   read by exact ranges (page
                                      #   checksums are the guard).

finalize(name, sink) -> (status, name)   # checkpoint -> ordinary .parquet;
                                         # idempotent; ckpt deleted last
recover_directory(sink_or_dir)           # the boot sweep

Shipper(name, local, remote)             # mirror local -> S3; pollable,
ship.poll() -> {'state': ...}            #   no threads; raises on sink
ship.synced                              #   failure (host logs + retries).
ship.close()                             # finalize the REMOTE copy from
                                         #   its own shipped artifacts
                                         #   (byte-identical to local).
                                         # Catch-up after an outage ships
                                         # missing chunks + ONLY the
                                         # latest checkpoint.

conveniences (local paths): live_table(path)  live_metadata(path)
                            live_file(path)   verify(path)
CLI: python3 -m parquet_checkpoint {status,mend,read,verify}

Go host (go/pqc, cgo over the C core; parquet-go does the encoding):
NewWriter(name, sink, chunkBytes, checkpointEveryWrite)
WriteRows[T](w, rows)  CheckpointRows[T](w, tail)  CloseTyped[T](w)
OpenReader(ckptName, sink) -> Compose()/AssemblyPlan()
Finalize(name, sink)   Sink interface + DirSink + S3Sink   ReadRows[T](file)
NewS3Sink(client, bucket, prefix)   # the caller's *s3.Client; assembly = multipart part copies
s3fake (go/s3fake): the tests' in-process S3, path-style, with the
multipart part rules (the Go side of FakeS3); sink_s3_test.go runs the
sink contract and the writer lifecycle on it

C core (core/pqc.h IS its spec; language-neutral: hosts encode
parquet and do all storage). The Python CheckpointWriter and the Go
Writer are both hosts over it, so the writer's rules exist once; the
DuckDB extension reads with it:
pqc_writer_new(final_name, chunk_bytes)
pqc_add_group(mini_parquet_bytes)     # core rebases + buffers
pqc_next_chunk / pqc_chunk_stored     # host stores; ack-or-retry
pqc_build_checkpoint(tail_mini)       # -> the PQC2 object bytes
pqc_reader_open / name / watermark / chunk / data
pqc_crc32  pqc_shift_footer
```

## The API (this README is the contract)

```python
from parquet_checkpoint import (CheckpointWriter, CheckpointReader,
                                DirSink, S3Sink, ckpt_versions, finalize,
                                live_table, recover_directory)

# write side (single writer); sink=None -> DirSink(dirname(path))
with CheckpointWriter('run.parquet', schema, sink=S3Sink('bucket'),
                      compression='zstd') as w:
    w.write_table(batch_or_table)     # final row group(s) + checkpoint
    w.checkpoint(tail=ring_rows)      # timed durability point between
                                      #   writes: unwritten rows ride
# close() -> ordinary run.parquet; chunks and checkpoint deleted

# read side, while the writer is alive (or after close — same calls)
r = CheckpointReader(ckpt_versions(sink, 'run.parquet')[-1][1], sink)
                                       # the newest version; any machine, any time
t = r.table()                          # exact prefix, ordinary parquet
t = live_table('dir/run.parquet')      # local-path convenience

# boot sweep after a crash (= assembly; anyone can run it, anywhere)
recover_directory(sink)
```

- `checkpoint()` cost: ~3 ms measured at 20k rows/s (encode the tail +
  one PUT). Checkpoint per write and crash loss = zero committed rows.
- `CheckpointWriter` refuses a name whose final file or checkpoint
  already exists (finalize first), and refuses `chunk_bytes` below the
  sink's `part_min` (finalize would be impossible on S3). Loud
  `CheckpointError` for every inconsistent state — mispaired names,
  wrong-size chunks, both-final-and-checkpoint — nothing is guessed.
- **Checksums, three layers, no rescans**: data pages carry parquet's
  own CRC-32 (`write_page_checksum` ON by default here); the
  checkpoint header carries a CRC over the whole object; the manifest
  carries a CRC per chunk, checked at read and assembly. `verify()` is
  the deliberate O(data) deep check.
- The union-view pattern is `checkpoint_reader.table() UNION ALL hot
  WHERE seq > watermark`, where the watermark IS the checkpoint
  (tested every cycle in the suite).

## DuckDB extension (duckdb-checkpoint/)

The same virtual file as a DuckDB filesystem: the `parquet_checkpoint`
extension, loaded on a connection opened with
`allow_unsigned_extensions` (DuckDB has not signed it), lets stock
`read_parquet()` read a LIVE checkpoint —

    SELECT * FROM read_parquet('dir/live.parquet.000017.ckpt');          -- local
    SELECT * FROM read_parquet('pqc://s3://b/live.parquet.000017.ckpt');  -- any
                                                       -- scheme, wrapped

Bare `.ckpt` paths without a scheme are claimed directly; scheme'd
paths are wrapped in `pqc://` so dispatch is unambiguous (the
wrapped path is opened through DuckDB's own filesystems — httpfs
etc. — so every backend DuckDB gains works here too). Closed files
and live checkpoints mix freely in one read_parquet list. The
extension compiles the same C core (one parser, fuzz-tested); build
with `GEN=ninja make release` in duckdb-checkpoint/, test with
run_extension_tests.py. A build serves one exact DuckDB version
(today 2.0.0, the `duckdb` submodule's tag); every DuckDB update needs
a rebuild for each platform.

## Mechanism (why no forks, no daemons)

Each `write_table` encodes through a stock pyarrow ParquetWriter into
memory; the mini-file's footer is REBASED to its logical position in
the final file by `_thrift_shift` (a ~250-line Thrift Compact Protocol
round-tripper that shifts the eight offset fields parquet.thrift
defines). Buffered groups cut into chunks at `chunk_bytes`; the
checkpoint's footer is the running combination of every group's
rebased metadata. Statistics, compression, and row-group pruning all
survive (tested). Finalize = one `assemble()` call: local concat, or
S3 CreateMultipartUpload + UploadPartCopy per chunk + a range copy of
the checkpoint's data region as the (exempt) last part.

## CLI

```sh
python3 -m parquet_checkpoint status <dir>   # done / live+ckpt / corrupt
python3 -m parquet_checkpoint mend <dir>     # the boot sweep, by hand
python3 -m parquet_checkpoint read <file.parquet> [-n ROWS]   # tail a live file
python3 -m parquet_checkpoint verify <file.parquet>  # O(data) CRC check
```

## Test plan

One suite, parameterized over three sinks (storage is behind the sink
interface, so the same tests run against all three):

1. Local disk (`DirSink`) — the volume tier: the subprocess SIGKILL
   gauntlet, soaks. Most test execution.
2. Fake S3, in-process (`FakeS3`) — implements S3 SEMANTICS (atomic
   overwrite, immutable objects, no append, a real multipart state
   machine with the 5 MB part rule — scaled down via `part_min` where
   tests use small chunks) with INJECTABLE FAULTS: `fail_next` (the
   operation fails doing nothing), `crash_next` (BaseException, like a
   kill). Memory-speed, so S3 bugs get the same iteration count as
   disk bugs. Hand-rolled, not moto — fault injection is the point.
3. Real S3 (`S3Sink`) — a thin contract suite (`run_s3_contract.py`,
   5 tests), run before adoption and periodically: verifies our
   understanding of AWS (part minimums — AWS returns EntityTooSmall,
   range copies byte-exact, overwrite replaces whole), plus one full
   writer -> remote reader -> finalize lifecycle. Bucket:
   `$PQC_TEST_BUCKET`; runs under a unique
   prefix and cleans up after itself. First green 2026-08-22.

Test content (all in `run_tests.py`, 14 tests):
- Crash gauntlet: SIGKILL the writer subprocess at random moments;
  finalize; assert exact content to a claim boundary, every value, no
  residue.
- Fault injection: failed chunk PUT is loud and the retry succeeds;
  failed checkpoint PUT leaves the previous checkpoint intact; crash
  mid-assembly leaves artifacts intact and finalize re-runs.
- Corruption: bit flips in chunks (manifest CRC) and checkpoints
  (header CRC), mispaired names, chunk_bytes below part_min. Every
  case a loud refusal, never a wrong answer.
- Concurrency: hammering readers against a live writer; strictly
  monotonic, exact prefixes.
- THE invariant: every checkpoint ever published is independently
  finalize-able — each saved generation, replayed into a fresh sink,
  finalizes to exactly its prefix.
- Assembly edges: the 5 MB part rule with real-size chunks; the
  range-copied final part; monotonic row-group offsets in the final
  file.
- The union-view payoff: arrow_ring hot + checkpoint reader cold,
  exact seam every cycle.
- The shipper: rides a WAN outage (local keeps writing), one catch-up
  pass ships missed chunks and only the latest checkpoint, remote
  final file byte-identical to the local one.
- C core parity (`run_c_compat.py`, 7 tests): thrift rebase byte-exact
  vs `_thrift_shift` on random schemas x120; same mini-file sequence
  through the core and the Python writer of before -> byte-identical
  chunks, CHECKPOINT OBJECTS, and final files; cross-reading both
  directions; same refusals. The package's writer became a host over
  the core on 2026-11-09: the same writes through it and through the
  writer of before (`_reference.ReferenceWriter`, kept as the oracle)
  leave the same objects byte for byte after every step, 2,509
  comparisons over schemas, compressions, tails, empty files and a
  stream's own options.
  Pure-C memory soak: `make -C core san` (ASan+UBSan) and
  `make -C core leaks` (0 leaks).

## Tests and tools

```sh
python3 packages/parquet_checkpoint/run_tests.py     # 15 tests
python3 packages/parquet_checkpoint/run_stress.py --kills 25
python3 packages/parquet_checkpoint/run_stress.py --seconds 60 --rate 20000
python3 packages/parquet_checkpoint/run_s3_contract.py   # real S3, needs creds
python3 packages/parquet_checkpoint/run_c_compat.py      # C core parity
python3 packages/parquet_checkpoint/run_go_compat.py     # Go <-> Python cross
(cd packages/parquet_checkpoint/go && go test ./pqc/)    # Go host suite
```

Run the suite + stress on BOTH Mac (APFS) and the Linux box (ext4)
before telemetry adopts the writer — DirSink is filesystem behavior;
the kill tests validate crash logic, and fsync-before-rename in
`DirSink.put` is the power-loss story (one place).

## Errata — deliberate deviations and omissions

Every place this library's output differs from what a stock parquet
writer produces, and why. Everything not listed here is vanilla
parquet: any tool reads the files.

1. **Page-index references are dropped; row-group statistics are
   kept.** Page indexes are located by absolute offsets and may
   legally live anywhere, but the spec recommends one contiguous
   block before the footer and mainstream readers shortcut-read them
   that way (parquet-go computes a single first-to-last span and
   crashes on our layout). Concatenating independently encoded
   segments interleaves their index sections, so the rebase drops the
   four ColumnChunk reference fields. Cost: pruning within a row
   group (marginal at telemetry group sizes, and S3 reads at range-
   request granularity anyway). Row-group min/max statistics -- the
   pruning that matters -- survive verbatim; proven behaviorally in
   the suite (a filter reads ONLY the group its stats admit). If
   sub-group pruning ever earns its keep, relocating indexes into a
   contiguous block at finalize is buildable.
2. **Dead bytes exist inside files, by design.** Stray segment magics
   (each mini-file's leading PAR1), dropped page-index bytes, and
   superseded provisional tails in earlier checkpoint generations are
   unreferenced bytes readers never touch. Kilobytes against
   megabytes; the price of never rewriting data.
3. **Footers are re-encoded canonically.** The rebase emits minimal
   varints and short-form field headers, so a round-tripped footer
   can differ in BYTES (never meaning) from its original encoding.
   The payoff: the C and Python implementations produce byte-
   identical artifacts, which the parity suite asserts.
4. **Page CRCs default ON** (`write_page_checksum`), unlike pyarrow's
   default. Corruption detection is worth the ~nothing it costs at
   telemetry rates.
5. **One checkpoint object, atomically replaced -- no history.**
   Redundancy comes from a second location (local + S3), not from
   keeping generations. Every generation ever published is
   independently finalize-able (tested), but only the latest exists.
6. **chunk_bytes is floored at the sink's part minimum** (5 MiB on
   S3) and the writer refuses below it at construction -- otherwise
   finalize by server-side copy would be impossible and the failure
   would surface hours later instead of on line one.
7. **The provisional tail is re-encoded at every checkpoint** and
   superseded by the next final write. Write amplification is
   bounded by one row group per checkpoint; there is deliberately no
   tail-size knob (the bound IS the host's row-group cadence -- see
   below).
8. **An all-empty checkpoint embeds a complete empty parquet file**
   as its data region, so finalize always yields a readable file.
9. **Finalize assembles once; never append-per-chunk.** Growing an
   S3 object incrementally re-copies it server-side each time
   (O(n^2)); the chunk model exists so assembly is a single pass.
10. **Bloom filter offsets are shifted if present** but neither host
    writes bloom filters by default; untested territory until one
    does.

## Tail policy (no knob)

The provisional tail is re-encoded at every checkpoint, so its cost is
bounded by its size. The bound IS the host's row-group cadence: rows
become final (and leave the tail) at every `write_table`, so the tail
never exceeds one row group. A growing tail means the host's row-group
threshold is too big — fix the cadence, don't cap the tail.

## Not yet

- Multi-file compaction/expiry policy (streaming-parquet's concern).
- Go S3 sink (DirSink only so far; the interface is the same).
- The Python reader on the C core. The writer is on it (`_core.py`);
  `CheckpointReader` and `CheckpointFile` still parse the checkpoint
  object themselves, checked against the core by run_c_compat.py and
  run_go_compat.py.
