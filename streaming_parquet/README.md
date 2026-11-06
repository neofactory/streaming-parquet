# streaming-parquet

Live telemetry streaming and parquet archival in one system. Every
committed row is queryable from the moment it is written until the end
of the archive, through one SQL view.

This package composes the other two packages of the trio:
`arrow_ring` holds the hot data in memory; `parquet_checkpoint`
defines the on-disk and S3 file format. The import name is
`streaming_parquet`. The standalone repo will be named
`streaming-parquet`. The code was reworked from telemetry v1 under the
working name telemetry_v2 and renamed on 2026-08-22.

## The model: chunks and checkpoint versions, in two places

Data lives in two kinds of artifact, in one format, wherever they are
stored:

- **Chunks**: immutable pieces of the final parquet file, cut at
  row-group boundaries, >= 5 MB (the S3 multipart part minimum; the
  packaging size, never a durability boundary). A chunk is a local
  file and/or an S3 object with the same name and bytes.
- **Checkpoint versions**: `<file>.parquet.<version>.ckpt`, each one
  object written once and never changed, holding everything a reader or
  repair needs. Each checkpoint is a new version.

```
  checkpoint version layout
  +----------------------------------------------------------+
  | header:  magic PQC2, version, CRC-32 of everything after |
  |          the CRC field, final file name, watermark (rows |
  |          covered), data offset                           |
  | manifest: ordered chunk list -- relative name, byte      |
  |          length, CRC-32 per chunk                        |
  | data:    the row groups not yet in a chunk (final), the  |
  |          newest rows as one provisional row group, and   |
  |          the footer covering chunks + data               |
  +----------------------------------------------------------+
```

The manifest defines a virtual file: chunk i covers logical bytes
[start_i, start_i + len_i), the data region follows, the footer
describes the whole. It is ordinary parquet: chunks + data region,
concatenated, is the file. Relative names make the set relocatable.

**Two copies, one format.** The same bytes are written to local disk
(the spool: fsync, tight cadence, never waits on the network) and to
the store (S3, mirrored at whatever cadence the link allows). Local
disk is WAN-outage buffering and machine-local tooling, not a separate
tier or mode.

```
                          writers (daemons)
                                 |
                                 v
  HOT     +=====================================================+
  memory  |  arrow_ring: mutable -> frozen -> recycled          |
          |  rows, cursors, tables, DuckDB views                 |
          +=====================================================+
             |  frozen spans -> chunks + checkpoint versions
             v
  DURABLE +======================+    +==========================+
          | spool (local disk)   |    | the store (S3)           |
          | chunk files          |    | chunk objects            |
          | checkpoint versions  |    | checkpoint versions      |
          +======================+    | ticket, catalog          |
                                      +==========================+
                                          |  assembly (once, anyone,
                                          v  anywhere, no deadline)
                                      run.parquet -- ordinary file
```

## Durability rules

- A PUT is atomic: a crash mid-upload leaves no object. S3 objects are
  immutable -- no append, no truncate -- so no ragged states exist on
  S3 and no truncation machinery is needed anywhere.
- Ordering, every layer: data durable first, claim second. A chunk is
  stored before any checkpoint version that references it; a version is
  stored whole before the ticket names it.
- A checkpoint version never changes once written: a reader that opened
  one reads one consistent file. A version superseded for 10 minutes
  (by the next version, or by the ticket dropping a finished file) is
  deleted in the step that uploads a later one; nothing runs on a timer.
- Crash loss: zero committed rows past the last checkpoint (the newest
  rows ride inside it). The checkpoint cadence is the loss window --
  seconds -- independent of row-group and chunk sizes.
- A live reader's memory must reach back past the ticket's stored_row
  (the store lags by at most a ship interval).

## Assembly (= repair; one mechanism)

- `CreateMultipartUpload` on the final key
- `UploadPartCopy` each chunk object, in manifest order (server-side;
  each byte copied once)
- final part: `UploadPartCopy` of the newest checkpoint version, byte
  range [data offset, end) -- the data region and footer, already in
  final form. No bytes travel to the host. The last part is exempt from
  the 5 MB minimum.
- `CompleteMultipartUpload`. The chunks and checkpoint versions stay
  while the ticket still lists the file and 10 minutes after it drops it
  (readers were told of them); then they are deleted.

Anyone with bucket credentials can run assembly, from any machine, at
any time; it holds no session state. Repair is the same: it assembles
from the newest version whose CRC holds. Do not grow the final object
incrementally (append-per-chunk re-copies the whole object server-side
each time: O(n^2)); assemble once.

## Checksums (three layers)

- Parquet data pages: CRC-32 per page, on by default
  (`write_page_checksum`), computed at write time, verified on any
  read that asks. `verify()` reads every page.
- Checkpoint versions: header CRC-32 over everything after the header.
  A corrupted version refuses loudly; repair uses the newest one whose
  CRC holds.
- Chunks: CRC-32 per chunk in the manifest, checked at read and at
  assembly.

## Lake placement (DuckLake, Iceberg, Delta)

- `.parquet` is a commitment: only finished files carry the suffix,
  and they appear atomically. Working artifacts (chunks, checkpoints)
  never match a `*.parquet` glob.
- Catalog lakes add files by commit, not discovery. Their cleanup
  (VACUUM, orphan-file removal) DELETES unregistered files inside
  table locations. Stage chunks and checkpoints OUTSIDE table roots;
  register only assembled files.
- If colocation is forced, use the `_`-prefix convention scanners
  ignore.

## Sequence identity

- Each ring keeps a monotonic seq per row. It never wraps; the ring's
  rows only set how far back is readable. Rings of different sizes share
  the same seq axis.
- Each wire batch carries two numbers in its Flight app_metadata, 16
  bytes LE: the origin seq of its first row (-1 for an empty batch), then
  the ticket revision. A ticket write with no new rows sends an empty
  batch carrying the new revision, so a live reader knows at once to
  re-read the ticket. Readers also accept the older 8-byte form (seq
  only). connect()
  starts the subscriber's ring at that number and stays on the
  PUBLISHER's seq axis. On reconnect or drop, a batch number past the
  local head is a gap: jump forward, loudly (the ring's readable
  floor rises; pre-gap reads are stale). BUILT, both languages.
- The checkpoint header carries one number: the highest seq it covers
  (the watermark W).
- Parquet stores no seq column. Each file records sp_base_seq (the
  stream seq of its first row) in its KV metadata; a row's seq =
  base + file_row_number, virtualized. BUILT, both languages.
- Each file also records sp_run, the run (the writer's start time,
  UTC to the microsecond) that wrote it; row numbers restart each run.
- The seam in duckdb_all is the ticket's stored_row: files up to it,
  memory after it, the same run only.
- write_table implies a checkpoint (the cheapest moment). A timed
  checkpoint(tail) covers the rows between writes. checkpoint_seconds
  sets crash loss; row_group_seconds sets file quality.

## The views (connect -> SQL over everything)

    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    a = streaming_parquet.connect("probe/voltages", store=STORE)
    view = a.duckdb_all(con)                 # register once
    con.sql(f"SELECT ... FROM {view.name}")  # every query sees one ticket

`duckdb_all(con)` is one DuckDB view over three pieces, all named by one
ticket: the finished files of the catalog version the ticket names
(downloaded with boto3 at exactly that ETag and attached from a local
copy; DuckDB reads the files' byte ranges from the store), the
unfinished files at the checkpoint versions the ticket lists (read
through the parquet_checkpoint extension, so the connection is opened
with `allow_unsigned_extensions`), and memory: the reader's ring rows
after the ticket's `stored_row`, same run only. `duckdb_archive(con)` is
the files alone; `duckdb_live(con)` is memory alone.

The view's text names the catalog copy, the checkpoint versions and the
seam, so a query never mixes two tickets. One background thread per view
replaces the view when the ticket changes: at once for a live reader,
which sees the revision on its connection; on the ticket's own schedule
(its write time plus its beat) for a reader without one. A catalog
version the store no longer holds means a newer ticket: the view reads
it again. Every checkpoint version, a live file's and a finished
file's, stays 10 minutes after it is superseded (by the next version, or
by the ticket dropping the file); the step that uploads a later one
deletes it, nothing on a timer. So a polling reader's view never names a
deleted one. Memory rows are copied out per query and the ring released
at once, so a query never holds the writer. Without a live writer (no
ticket, or a stale one) the views read the store's current catalog.

The archive is `<store>/streams/<name>` under the store the reader
connected through; a direct-ticket connect passes `store=`. A Stream
reads through the same face as an Attached, so the writer has its own
views with the same calls, following its ticket in memory.

## Layout (the store)

A store is a whole bucket or a whole directory, and everything in it is
one of two kinds, each under its own top-level directory, with the
stream's name as the path inside:

```
<store>/
  tickets/<name>.json                 the stream's ticket, while it is published
  streams/<name>/<stamp>[-<label>].parquet          finished files
  streams/<name>/<stamp>[-<label>].parquet.<v>.ckpt checkpoint versions (each kept 10 minutes once superseded)
  streams/<name>/<stamp>[-<label>].parquet.chunks/  chunks while open
  streams/<name>/catalog.ducklake                   the catalog of finished files
  streams/<name>/catalog.<YYYYMMDD-HHMMSS>.ducklake dated copies
```

A name may carry a hierarchy (`plant/line-1/probe/voltages`),
so a site's streams group under one prefix and access rules fall on
whole subtrees: tickets readable by every reader, streams by site.
The same shape holds under a bucket and under a directory, so the
spool mirrors the bucket name for name and a reader derives every
location from the store it connected through. A prefix inside a
bucket is refused: one shape per deployment. A stream URI is its
archive directory, `<store>/streams/<name>`, and `connect()` accepts
it in place of a name.

## Discovery (the store)

A stream's ticket is one JSON object at `<store>/tickets/<name>.json`:
who writes the stream (`name`, `host`, `port`, `run`), how often the
store's data changes (`checkpoint_seconds`, `ship_seconds`,
`beat_seconds`) and when to call the writer dead (`stale_after`), what
the store holds of this run (`stored_row`, `stored_at`), the files still
unfinished (`unfinished`: each the real path of its newest checkpoint
version), the published catalog's `catalog_etag`, a `revision` counting
this run's ticket writes, and the write time `ts`.

The writer writes it right after each upload and each catalog upload,
and at least once a beat; it deletes it on clean close. When a file
finishes, it stays in `unfinished` until a published catalog lists it;
the next ticket drops it and names that catalog, and only then are its
checkpoint versions and chunks deleted. So the ticket, the catalog it
names and the checkpoint versions it names are one consistent picture.
A live reader learns of each ticket write from the revision on its
connection; a reader without one reads the ticket on the schedule its
`ts` and `beat_seconds` give. Missing ticket = no publisher; `ts` older
than `stale_after` = dead publisher, and `standing()` says which it was.
There is no broker: discovery is the same kind of thing as the archive,
objects at a location.

The store is named on every call (`store=`), or by the
`STREAMING_PARQUET_STORE` environment variable; reads with neither fall
back to the public demo bucket (`s3://streaming-parquet`). Writers
never fall back to the public bucket: `create_stream` with no store
disables the ticket, loudly, and the stream is local + direct-ticket
only. Credentials are not the store's business: the standard AWS
chain applies; the public bucket needs none.

## Who reads what

| reader | needs | sees |
|---|---|---|
| `connect()` + `duckdb_all` on the publisher's network | the store + Flight | the catalog, the unfinished files, memory: every row, the live edge in its own ring |
| `connect()` + `duckdb_archive` anywhere | the store, read-only credentials | every row the store holds, current within a beat |
| plain DuckDB | the store | finished files, through `ATTACH 'ducklake:<store>/streams/<name>/catalog.ducklake'` |
| `live_table()` on the writer's machine | local disk | everything up to the local checkpoint |
| any parquet tool | the store | finished files |

The views read only what the ticket names, so every seam is a stored
object: a catalog version, a checkpoint version, the ticket's
stored_row. No reader lists the store.

## The catalog

One DuckLake catalog per stream, `streams/<name>/catalog.ducklake`, a
DuckDB database with one table, `data`, listing the stream's finished
files by full path with each file's row count and each column's minimum
and maximum, so DuckDB reads only the files a query needs. The writer
keeps a working copy on local disk, registers each finished file (on
a thread of the stream's own, never the worker pool; streams' passes
run at the same time) and uploads the whole file conditioned on the
ETag it last saw. The working copy has 16 KB blocks, DuckDB's
smallest: the catalog is a fraction of a MB where DuckDB's default
256 KB blocks made it several MB; a catalog published with other
blocks is copied into 16 KB blocks once and published. Every pass reconciles
against the files in the store, so a crash, a leftover or files from
before the catalog all come out the same way. A long pass reports its
progress; a stream's close stops it between two files. Dated copies,
`catalog.<YYYYMMDD-HHMMSS>.ducklake`, are made at
`Archive(catalog_copy_seconds)` (a day) only when the catalog changed.
DuckDB reads the store with boto3's credentials (an assumed role
included). The files are the source of truth; the catalog can be
rebuilt from their footers.

## Parquet encodings

The writer's defaults, the same in Python and Go, chosen for sensor
streams (counters, positions, voltages):

| Columns | Default |
|---|---|
| every column | zstd, level 9 |
| integers (struct leaves and list elements included) | DELTA_BINARY_PACKED |
| float32, float64 | BYTE_STREAM_SPLIT |
| dictionary | off, except the columns a stream names |

Measured on a June 2026 CNC fieldbus recording: 12.8 bytes per row against
28.9 with pyarrow's own defaults (snappy, dictionary on every column).
Everything here is in the parquet spec; any reader of these files must
read BYTE_STREAM_SPLIT, which DuckDB has since 2023 and pyarrow since
2.0. zstd 9 costs more CPU per written byte than snappy.

`Archive(parquet={...})` lays a stream's options over the defaults,
Python: `use_dictionary` (the columns to dictionary-encode, which then
drop their default encoding), `column_encoding` ({column path:
encoding}), `compression`, `compression_level` (9 applies to zstd only).
Go: `ArchiveOptions.Dictionary`, `Compression`.

## Ownership

One writer per stream. `create_stream` takes an exclusive lock on
`<spool or directory>/streams/<name>/writer.lock` (a second process on
the same machine is refused, naming the holder) and checks the ticket's
host (a fresh ticket from another machine is refused; a stale one is
taken over). The owner finishes every leftover checkpoint at start,
whatever its age.

## Storage: the store and the spool (Python)

A stream is created with a store, a whole bucket (`s3://name`, no
prefix) or a whole directory, whose contents are the layout. A bucket
store is written through the spool, the local write-behind copy at
`/var/tmp/streaming-parquet/<user>/` unless `spool=` says where, or
`spool=None` for the bucket as the only copy; a directory store is
written straight into. The Go host's `ArchiveOptions.Sink` is the same
first-copy idea. Severity follows the shape: the first copy failing
means rows are durable nowhere but the ring, and `archive_state()`
says so (`failing`, the last error, `lost_rows` for rows recycled
before their flush, the location, the open `file`); the mirror failing
loses nothing, and `mirror_state()` says that (`failing`, the last
error, the files pending). Both return named objects. A daemon reads
both each loop and decides what to call them; the library logs each
transition once, the archive at ERROR, the mirror at WARNING.

With a bucket store the writer never waits for the network: chunks and
the checkpoint land in the spool at the stream's own cadence, and a
Shipper per file copies them to the bucket on a ship task of the
worker pool, one pass every `ship_seconds`, submitted by `commit()`'s
publish step while any file is live or pending and by the stream's
heartbeat thread when nothing is being committed, so a session that
ended during an outage finishes as soon as the link is back.

A file finishes in one order: its final checkpoint on the first copy,
that checkpoint shipped, first-copy assembly, remote assembly
(server-side, no bytes travel). While the remote is unreachable a
finished file stays in its checkpoint form, readable and
crash-durable; the writer opens the next file as usual and every
later pass retries. A clean stop makes one closing pass and waits 3 s
for it; a file still pending after that is registered by the next
start's boot sweep, whose remote half runs on the first mirror pass,
so an unreachable remote at start delays the mirror and never the
stream. `S3Sink` fails fast (5 s connect, 15 s read, 2 attempts).

## Failure table

| failure | loss | recovery |
|---|---|---|
| process crash | 0 rows past the last local checkpoint | assembly from local chunks + checkpoint |
| disk loss | rows since the last S3 checkpoint PUT | assembly from S3, from any machine |
| WAN outage | none (local copies continue; S3 catches up) | shipper drains the backlog |
| crash during assembly | none; chunks and checkpoint are durable | re-run; a lifecycle rule deletes the dead multipart upload |
| corrupted checkpoint version | none | header CRC refuses it; repair uses the newest version that holds |
| corrupted chunk | detected | manifest CRC refuses it at read/assembly |
| data page corruption | detected | page CRCs on by default; `verify()` reads every page |

## API

The writer is only for very fast, low-latency operations. It fills rows
with `get_mutable_row`, `from_wire` or `append`. `commit` triggers worker
threads inside the library to forward the data, write the files, and do
everything slow; `cut` triggers them too. A reader, in any thread, calls
one of the reader verbs, and it all just works.

```
$STREAMING_PARQUET_DIAG_LOG    # diag log file, opened at import;
                               # default telemetry-diag.log in the cwd
STORE = "s3://my-bucket"    # a whole bucket, or a whole directory; no prefix
create_stream(name, schema, store=STORE, spool=DEFAULT_SPOOL, memory=Memory(...), archive=Archive(...))
    # name: the stream's whole path under the store (files at streams/<name>/,
    #       ticket at tickets/<name>.json)
    # spool: the local write-behind copy a bucket store writes through;
    #        default /var/tmp/streaming-parquet/<user>/, None for none
    # Memory(lookback, maximum, rate_hint, segment_bytes): rows on this core
    # Archive(checkpoint_seconds, row_group_rows, row_group_seconds, ship_seconds,
    #         catalog_copy_seconds, parquet): tuning; None = memory only
create_playback_stream(name, parquet_path, timestamp_column, latency_s, memory)
connect(name | ticket, store=STORE, memory=Memory(...), columns)
list_streams(store=STORE)
cut(label) / start_parquet(label) / stop_parquet()     # every local stream
streams ; close()

Stream (writing): get_mutable_row(block_s) commit()
                  append(batch | table | dict)
                  wire_map(entries) from_wire(wmap, frame)
                  cut(label) start_parquet(label) stop_parquet()
                  archive_state() mirror_state() memory_state() close()

reading (a stream we write and one we connect to, alike):
                  newest_row() -> Row | None
                  cursor(since=None, columns=None) -> Cursor
                      for row in cursor ; cursor.arrow_table() ; cursor.lost
                  arrow_table(columns=None, rows=None, since=None) -> pyarrow.Table
                  duckdb_live(con) duckdb_archive(con, store=) duckdb_all(con, store=) -> View
                      the archive views follow the ticket; con opened with
                      allow_unsigned_extensions (the parquet_checkpoint extension)
                  memory_state() -> MemoryState
                  .name .schema .state standing() is_connected() close()

results: ArchiveState(failing, error, lost_rows, location, file, catalog_error)
         MirrorState(failing, error, pending)   MemoryState(rows, rows_unarchived, capacity)
         View(name, close())
errors:  StaleReadError, PinStalledError
```

Not public: sequence numbers, the ring, record batches. Those are how
it works, not what it offers.

## The API (detail)

```python
import pyarrow as pa
import streaming_parquet as sp

s = sp.create_stream(name, pa.schema([...]), store="s3://my-bucket",
                     memory=sp.Memory(lookback=200_000),
                     archive=sp.Archive(checkpoint_seconds=15, ship_seconds=15))
row = s.get_mutable_row()            # the ring vocabulary
row.servo.pos = 100                  # struct leaves; strings; lists
s.commit()                           # visible at once; the pool does the rest
s.append(batch)                      # many rows, one C call, then the same
s.cut(label="O0019")                 # end the file; the next carries the label and
                                     # is named by its first row's commit time
s.start_parquet("O0019"); s.stop_parquet()   # archive on and off, stream live

a = sp.connect(name, store=...)      # local: the publisher's ring, zero
                                     # copies. Remote: a ring fed by Flight.
a.newest_row().tv                    # one row, attribute per column
for row in a.cursor(): ...           # the rows this reader has not seen
t = a.arrow_table(rows=1000)         # the newest 1000 rows, zero copy;
                                     # the table pins memory until dropped
t = a.arrow_table(since=('tv', t0))  # from a time onward
view = a.duckdb_all(con)             # register once with DuckDB;
con.sql(f"select ... from {view.name}")   # every query sees current data
```

`arrow_table()` with no selector is the rows not yet on disk. The
sequence numbers that address rows inside the library are not part of
this surface.

Deleted from v1, with no compatibility path: numpy dtypes,
`subscribe()` and the Subscriber cursor class, `connect()`,
`write_columns`, `next_row`, `Record`, `margin`, the column banks and
the CFFI transpose. Deleted from v2's first cut: `publish()` (commit
does its step), `flush_parquet` (now `cut`), `write_dict` /
`append_batch` / `append_table` (now `append`), `to_wire`, the static
timeline relation, `get_hot_relation` / `get_dynamic_relation` (now the
three `duckdb_*` views), the sequence calls and `ring` on a reader, the
`archive` field in the ticket. `start_parquet` records rows committed
after the call.

## Go host (go/streaming)

The Go host is on the OLD API (Init/Root/Attach, Publish) and has none
of the store, catalog, ticket revision or ownership work; it writes
versioned checkpoints and reads 8- or 16-byte side data. Bringing it
up, or replacing it with the C core of V2B, is an open issue. One
production service writes through it.

A complete second implementation, wire-compatible with this module
(same root tickets, same Flight contract) and symmetric by design:
there is no server role, only the two verbs, and ANY ring can archive
— an attached process may StartParquet exactly like a creating one,
because the parquet path consumes a ring and does not care whose.

```go
tel, _ := streaming.Init(streaming.Config{Root: "/data/streams"})
s, _ := tel.CreateStream("press/fieldbus", schema,
        streaming.StreamOptions{
            Archive: streaming.ArchiveOptions{BaseDir: dir,
                CheckpointSeconds: 2}})
// The ring holds two row groups: RingRows and ArchiveOptions.RowGroupRows
// default to 245,760 and DuckDB's 122,880; give one and the other
// follows; a ring under two row groups is refused.
// Archive on S3 instead: ArchiveOptions{Sink: pqc.NewS3Sink(s3client,
// bucket, prefix), CheckpointSeconds: 2} — chunks are the 5 MiB part
// minimum, the checkpoint is the durable tail between them.
// write via s.Ring() (RowBegin/Set*/Commit, wire maps);
// s.Publish() from your loop; s.StartParquet("job-19")

a, _ := tel.Attach("probe/voltages", streaming.AttachOptions{})
r, _ := a.WaitRing(5 * time.Second)   // local ring fed by Flight
a.StartParquet("cal-run")             // symmetric archival
```

`ArchiveOptions.Sink` is the root the archive lives under (a bucket,
or nil for a directory at BaseDir); the stream places its files at
`streams/<name>/` inside it (`Sink.Sub`), and the ticket's archive
field is that location (`Sink.URI`). A caller that used to pass the
stream's own prefix as the sink now passes the root.

Composes the other two Go hosts: arrow-ring/go (the ring) and
parquet-checkpoint/go (chunks + checkpoint via the shared C core;
pqarrow encodes). Roots: file:// and s3://bucket[/prefix], the same
ticket objects the Python module writes (tickets_s3.go, on the
standard AWS credential chain). Cross-language gate:
`run_go_streaming_compat.py` (hermetic file root) — Go publisher ->
Python attach() exact; Python publisher -> Go attach exact, with the
Go side archiving to a finalized parquet file while attached, read
back exactly by pyarrow.

Python does not have attached-side archival yet; porting that verb
onto `Attached` is the symmetry-parity item on the list.

## Migration charter (binding)

1. No backwards compatibility. No compatibility shims, no deprecated
   aliases, no dual code paths, no parameters kept for old callers.
   The old module serves old callers until
   each daemon migrates. Migrating a daemon means rewriting its call
   sites.
2. Rework, do not rewrite. Every change starts from the working code.
   Tuned constants, guards, sleeps, and retry shapes are lessons until
   proven otherwise. Removing one requires that the new mechanism
   demonstrably covers it, or a discussion first.
3. A bug found during rework is discussed before it is fixed. A bug
   may have a purpose.

## State

- Library main carries the store and spool, ownership, the ticket with
  its revision, the DuckLake catalog with dated copies, versioned
  checkpoints with the 10-minute rule, the parquet_checkpoint DuckDB
  extension (built for DuckDB 2.0.0) and its loader, and the archive
  views that follow the ticket.
- Suites: run_tests.py (66), run_worker_pool_tests.py, the Go
  compatibility gates, parquet_checkpoint's suites (format, C parity, Go
  parity, stress, S3 contract), arrow_ring's suites, and
  duckdb-checkpoint/run_extension_tests.py (S3 with CKPT_TEST_STORE).
- In production: a machine shop's CNC and probe daemons write
  through this library.
- Open: the Go host (above); attached-side archival in Python; DuckDB's
  signing of the extension (a community extension) to end per-platform
  builds and the unsigned flag; V2B (the SQLite catalog; the core asks,
  the host does).
