# streaming-parquet

**Stream-first Parquet: write rows as they arrive, read them while the file is still open—no brokers, no ETL.**

```python
a = sp.connect("plant/voltages", store="s3://my-bucket")
view = a.duckdb_all(con)                  # register once
con.sql(f"SELECT ... FROM {view.name}")   # archive + open file + live memory
```

Write live streams directly into big, query-ready Parquet row groups while local and remote readers access new rows in milliseconds. Query memory and archive together as one table. No more trade-offs between row group size, latency, and crash loss:

- **Big row groups** — standard `.parquet` files on disk or S3, fast to query with any tool
- **Millisecond live access, anywhere** — read zero-copy in-process or stream immediately to other processes and machines
- **Small data-loss window** — crash-durable in seconds while files are still open
- **One query** — query the coldest archive right up to the latest committed row in one DuckDB view

## How it works

- **Parquet checkpointing (`parquet_checkpoint`):** Periodically write a small checkpoint footer alongside the in-progress Parquet file. Read them together before the file is complete, or recover into a standard Parquet file.
- **Arrow ring buffer (`arrow_ring`):** Write into a zero-copy, recycling column-major buffer in memory. Committed rows are immutable while pinned.
- **Arrow Flight streaming:** Pull live batches into a local ring from any remote reader, using the store for discovery with no broker.
- **DuckDB integration:** Query live data plus the archive in one SQL view. Each query sees the latest data when it starts. During queries, DuckDB pins ring spans for under a millisecond.

`streaming_parquet` ties these pieces together over shared C cores with thin bindings in Python and Go, plus a DuckDB extension. `arrow_ring` and `parquet_checkpoint` also stand on their own.

## Quick look

```python
import pyarrow as pa
import duckdb
import streaming_parquet as sp

STORE = "/data/streams"  # or "s3://my-bucket"

schema = pa.schema([
    pa.field("tv", pa.float64(), nullable=False),
    pa.field("volts", pa.float32(), nullable=False),
])

# Writer (any process)
s = sp.create_stream(
    "plant/voltages",
    schema,
    store=STORE,
    parquet=sp.ParquetOptions(checkpoint_seconds=15),
)
row = s.get_mutable_row()
row.tv, row.volts = 1732200000.0, 24.1
s.commit()

# Reader (same process, another process, or another machine)
a = sp.connect("plant/voltages", store=STORE)
latest = a.newest_row()                                      # millisecond live read

con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
view = a.duckdb_all(con)                                     # register once
con.sql(f"SELECT avg(volts) FROM {view.name}").fetchall()    # archive + open file + memory
```

## Architecture at a glance

```text
  Writer ──► arrow_ring (hot memory) ──► Arrow Flight (pull) ──► Remote readers
                  │
                  ▼ (async, off the write thread)
        parquet_checkpoint (open file: immutable chunks + versioned .ckpt footer)
                  │
                  ▼ (assembled once on cut, close, or recovery)
         standard .parquet + DuckLake catalog ◄── DuckDB (duckdb_all view)
```

Everything durable lives under one store—either a local directory or a whole S3 bucket:

```text
<store>/
  tickets/<name>.json                  # publisher discovery + current state
  streams/<name>/<stamp>.parquet       # finished standard Parquet files
  streams/<name>/<stamp>.<v>.ckpt      # versioned checkpoints (open files)
  streams/<name>/<stamp>.chunks/       # immutable pieces of open files
  streams/<name>/catalog.sqlite        # per-stream catalog of finished files
```

With an S3 store, writes land on a local write-behind **spool** first and mirror asynchronously to S3—so writers keep running through WAN outages and catch up automatically.

## Glossary

### Concepts

| Term | Meaning |
|---|---|
| **Stream** | A named, Arrow-schema sequence of rows with one writer and any number of readers. |
| **Store** | A local directory or whole S3 bucket holding tickets, open checkpoints, finished `.parquet` files, and catalogs. No broker needed. |
| **Spool** | Local write-behind directory used with an S3 store to absorb network outages without blocking writes. |
| **Ticket** | Small JSON file in `<store>/tickets/<name>.json` advertising the live writer, its Flight endpoint, open checkpoints, and catalog version. |
| **Published / internal** | A published stream has a ticket and is served over Flight, so other processes find and read it. An internal stream has neither: only its own process reads it. |
| **Run** | One life of a stream's writer, named by its start time. A row is identified by its run and its row number in that run. |
| **Ring** | Fixed-size, recycling column-major buffer (`arrow_ring`) holding recent rows in memory for zero-copy Arrow reads. |
| **Span / Pin** | A contiguous range of committed rows in the ring. Readers pin a span to keep those rows immutable until released. |
| **Checkpoint** | Immutable, versioned footer (`.ckpt`) indexing the stored pieces of an in-progress file. Costs only the new rows since the previous checkpoint. |
| **Assembly** | Combining an in-progress file's chunks and checkpoint footer into a standard `.parquet` file (server-side multipart copy on S3). Closing and crash recovery use the same step. |
| **Cut** | End the open file at the last committed row; the next row begins a new file. No row is skipped or written twice. |
| **Seam** | The exact row boundary where `duckdb_all` switches from stored files to live memory—no gaps, no duplicates. |
| **Catalog** | Per-stream DuckLake catalog in a SQLite file (`catalog.sqlite`) indexing finished `.parquet` files with min/max stats for file skipping. |

### Methods

These are the Python names. Go has the same concepts: see [Python and Go: the same shape](#python-and-go-the-same-shape).

**Module**
- `create_stream(name, schema, store, spool, memory, parquet, publish)` — create and own a stream (one writer per stream). `parquet=None` writes no files until a `start_parquet`; `publish=False` makes an internal stream.
- `connect(name, store, memory, columns, parquet_store, parquet)` — attach to a stream for reading (zero-copy in-process; Arrow Flight over the network). A reader that starts before its writer waits for it; a path whose folder does not exist is an error at once. With `parquet_store`, a store of the reader's own, the reader can write the rows it receives to files.
- `list_streams(store)` — list stream names with a ticket in the store.
- `create_playback_stream(name, parquet_path, ...)` — replay a recorded `.parquet` file as a live stream.
- `cut(label)` / `start_parquet(label, options)` / `stop_parquet()` — roll, start, or pause archival across all local streams.
- `log_to_file(path)` — write the library's log lines to a file (see [Logging](#logging)).
- `close()` — flush, finish open files, and shut down Flight cleanly.

**Options**
- `MemoryOptions(rows)` — how many rows the ring holds. Default: two row groups.
- `ParquetOptions(checkpoint_seconds, row_group_rows, row_group_seconds, ship_seconds, compression, catalog_copy_seconds)` — how the files are written. Where they go is the store and the spool.

**Writing (`Stream`)**
- `get_mutable_row(block_s)` / `commit()` — fill one row directly in ring memory and publish it; slow work (Flight, archival, S3) runs asynchronously.
- `append(rows)` — bulk-ingest a PyArrow `RecordBatch`, `Table`, or `dict`.
- `wire_map(entries)` / `from_wire(wmap, frame)` — scatter packed binary frames directly into ring columns in one C call.
- `cut(label)` / `start_parquet(label, options)` / `stop_parquet()` — roll to a new file (with optional filename label), or toggle archival while keeping the stream live. `start_parquet` is a mark in the stream: the new file holds the rows committed after it.
- `publish()` / `unpublish()` — make the stream published or internal while it runs.

**A reader that writes files** (connected with `parquet_store`)
- `start_parquet(label, options)` / `stop_parquet()` / `cut(label)` — as on a stream, for the rows the reader receives. The files go to `<parquet_store>/streams/<name>/` and keep the writer's run and row numbers. No ticket and no catalog names them.
- `archive_state()` / `get_durable_row_number()` — the state of the reader's own files.

**Reading (`Stream` and connected readers)**
- `newest_row()` — read the latest committed row in memory.
- `cursor(since, columns)` — iterate unseen rows (or grab them as an Arrow table with `cursor.arrow_table()`).
- `arrow_table(columns, rows, since)` — zero-copy PyArrow `Table` over memory.
- `duckdb_live(con)` — live DuckDB view over memory only.
- `duckdb_archive(con)` — live DuckDB view over finished and in-progress stored files.
- `duckdb_all(con)` — unified DuckDB view across finished archive, open checkpoints, and live memory.
- `row_ids=True` on any of the three views adds two columns that identify a row for good: `sp_run_start_time` and `sp_run_row_number`.

**Health & status**
- `archive_state()` / `mirror_state()` / `memory_state()` — check durability errors, S3 mirror backlog, and ring utilization.
- `get_newest_row_number()` / `get_durable_row_number()` — the number of the last row committed, and of the newest row that is safe in a file. A program that must acknowledge its input only when the row is safe compares the two.
- `reason` / `is_connected()` — check a reader's connection state in plain words.

## Python and Go: the same shape

The rule: the two languages have the same concepts, the same words and the same defaults. Only the customs of each language differ (`snake_case` and keyword arguments in Python; `CamelCase`, option structs and returned errors in Go). The rules themselves are written once, in the C cores, so the two cannot decide differently.

| Concept | Python (`import streaming_parquet as sp`) | Go (`.../src/go/streaming`) |
|---|---|---|
| The store | `store=` on each call, else `$STREAMING_PARQUET_STORE` | `Store:` in the options of each call, else `$STREAMING_PARQUET_STORE` |
| Create a stream | `sp.create_stream(name, schema, ...)` | `streaming.CreateStream(name, schema, StreamOptions{...})` |
| Rows kept in memory | `memory=sp.MemoryOptions(rows=n)` | `Memory: &MemoryOptions{Rows: n}` |
| Parquet options | `parquet=sp.ParquetOptions(...)`; `None` = no files | `StreamOptions.Parquet *ParquetOptions`; `nil` = no files |
| Where the files go | the store, through the spool for S3 | the same |
| The spool | `spool=` (a directory; `None` = no spool); `ParquetOptions(ship_seconds=)` | `Spool:`, `NoSpool:`; `ParquetOptions{ShipSeconds:}` |
| Dated copies of the catalog | `ParquetOptions(catalog_copy_seconds=)`; `None` = none | `ParquetOptions{CatalogCopySeconds:}`; negative = none |
| Published or internal | `publish=True`; `s.publish()`, `s.unpublish()` | `StreamOptions.Internal`; `s.Publish()`, `s.Unpublish()`, `s.Published()` |
| Write a row | `row = s.get_mutable_row()`; `row.x = 1.0`; `s.commit()` | `row, err := s.MutableRow(block)`; `row.SetF64(x, 1.0)`; `s.Commit()` |
| A column, for the Go setters and getters | the attribute `row.x` | `x, err := s.Column("x")` |
| Write many rows | `s.append(table)` | `s.Append(record)` |
| Packed binary frames | `s.wire_map(entries)`; `s.from_wire(m, frame)` | `s.WireMap(entries)`; `row.FromWire(m, frame)` |
| End the file, begin the next | `s.cut(label)` | `s.Cut()`; `s.CutLabel(label)` |
| Parquet on and off | `s.start_parquet(label, options)`; `s.stop_parquet()` | `s.StartParquet(label, options)`; `s.StopParquet()` |
| The state of the files, the mirror and memory | `s.archive_state()`; `s.mirror_state()`; `s.memory_state()` | `s.ArchiveState()`; `s.MirrorState()`; `s.MemoryState()` |
| The last row committed, and the newest row safe in a file | `s.get_newest_row_number()`; `s.get_durable_row_number()` | `s.NewestRowNumber()`; `s.DurableRowNumber()` |
| Read a stream | `a = sp.connect(name, store=...)` | `a, err := streaming.Connect(name, ConnectOptions{...})` |
| A reader that writes files of its own | `parquet_store=`, `parquet=`; then `a.start_parquet`, `a.stop_parquet`, `a.cut`, `a.archive_state`, `a.get_durable_row_number` | `ParquetStore:`, `Parquet:`; then `a.StartParquet`, `a.StopParquet`, `a.Cut`, `a.CutLabel`, `a.ArchiveState`, `a.DurableRowNumber` |
| Read rows (a stream and a reader alike) | `a.newest_row()`; `a.cursor()`; `a.arrow_table()` | `a.NewestRow()`; `a.Cursor()`; `a.ArrowTable()`, `a.ArrowTableNewest(rows)` |
| One value of a row | `row.x` | `v, null, err := row.F64(x)` |
| A reader's state, and why it is not streaming | `a.state`; `a.reason` | `a.State()`; `a.Reason()` |
| DuckDB views | `duckdb_live`, `duckdb_archive`, `duckdb_all` | none: a Go program queries the store with DuckDB directly |
| List the streams | `sp.list_streams(store)` | `streaming.ListStreams(store)` |
| Log lines | logger `streaming_parquet`; `sp.log_to_file(path)` | `streaming.SetLogger(logger)`, a `*slog.Logger` |
| Close | `s.close()`; `sp.close()` | `s.Close()`; `streaming.Close()` |

The same in both, because one C core decides it:

- **The ticket**: its members, its text, and the rule that calls a writer dead.
- **One writer per stream**: the writer lock and the message that names its holder.
- **When a write is due**: row groups by rows or by seconds, checkpoints by seconds.
- **What the store holds**: which files are unfinished, which checkpoints are kept and for how long.
- **A reader's rules**: wait for a writer, refuse a path whose folder does not exist, say why it is not streaming.
- **The catalog**: `catalog.sqlite`, written by the same C code from either language, and when a dated copy of it is due.
- **Checkpoint versions**: their names, and when a replaced one is deleted with the objects only it named.
- **The mirror from a spool to a bucket**: what is sent, in which order, what is deleted, the steps that end a file, and what a start does with a file an earlier run left.
- **Defaults**: a row group of 122,880 rows, a ring of two row groups, zstd level 9, a file cut at 256 MB.
- **File formats**: a Python reader reads a Go writer's stream and the reverse; the test suite runs each against the other.

Not yet the same (each of these is a difference in words or in reach, not in the stored data):

- **Published**: Go's option is `Internal` (the reverse sense of `publish`).
- **Views**: Go has none.
- **A row's number**: Go's rows have `RowNumber()`; Python's rows do not show it.
- **Rows from a value on**: Python's `cursor` and `arrow_table` take `since=(column, value)`; Go's do not.
- **One flat row from a dict**: Python's `append` takes one; Go has no such form.
- **The Flight address**: Go has `streaming.SetFlightAddress`; Python always takes a free port.

## The C cores

Three cores in C11 hold the rules, and each header is its specification:

| Core | Header | What it decides |
|---|---|---|
| The ring | `src/core/arrow_ring.h` | Rows in memory: slots, commits, pins, record sets as Arrow. |
| The checkpoint format | `src/core/pqc.h` | The bytes of a checkpoint and of the objects of an open file; checkpoint versions; the mirror of a file from a local copy to a remote one; the assembly of a finished file from its checkpoint (which version, the checks, the order of the deletes); the parts of a multipart assembly in a bucket. |
| The stream's rules | `src/core/sp.h` | The ticket, ownership, when a write is due, what the store holds, a reader's and a view's rules, the catalog. |

A core does no input and no output, calls no function of its host, and never waits. A call returns a status; the reason for a failure and the lines for the host's log come back in a `Report` the caller passed ([`src/core/report.h`](src/core/report.h)). The host (Python, Go, the DuckDB extension) does the files, the network, the waiting and the logging. A test reads the three headers and holds them to this rule (`tests/core/run_header_tests.py`).

## Logging

The library logs the way each language does, and never prints by itself.

- **Python**: the standard `logging` module, loggers `streaming_parquet`, `parquet_checkpoint` and `arrow_ring`. No handler and no file by default. `sp.log_to_file(path)` writes INFO and above to a file; `$STREAMING_PARQUET_DIAG_LOG` does the same at import.
- **Go**: `streaming.SetLogger(logger)`, a `*slog.Logger`, before the first stream or reader. With none, warnings and errors go to standard error.
- **The C cores**: no output. Their lines reach the host's log through the `Report`, each with the time the core made it. A line made on a writer's thread is logged by another thread.

## Install

Python 3.11+. One distribution, `streaming-parquet`, holds the three packages; it depends on pyarrow and cffi. Extras: `duckdb` (the views) and `s3` (boto3: `S3Sink`, `s3://` stores). A uv project depends on a checkout like this:

```toml
dependencies = ["streaming-parquet[duckdb]"]

[tool.uv.sources]
streaming-parquet = { path = "../streaming-parquet", editable = true }
```

The packages load the C cores as libraries. `pip install` builds them with `make` and the system C compiler. The stream core also needs the SQLite library and header (on Ubuntu, `libsqlite3-dev`). In a checkout or an editable install, each package builds its library from `src/core/` into its own directory the first time it is imported, so that directory must be writable.

Go: module `github.com/neofactory/streaming-parquet/src/go`, packages `ring`, `pqc` and `streaming`, built with cgo over checked-in copies of the same cores.

## Layout

- `src/core/` — the three C cores, `report.h`, their C tests and one Makefile
- `src/python/` — the Python packages, each with its own README: [`arrow_ring`](src/python/arrow_ring/README.md), [`parquet_checkpoint`](src/python/parquet_checkpoint/README.md), [`streaming_parquet`](src/python/streaming_parquet/README.md)
- `src/go/` — the Go module (`src/go/sync_core.py` copies the cores into it)
- `src/duckdb/` — the [DuckDB extension](src/duckdb/README.md) that reads unfinished files
- `tests/` — the Python test scripts: unit, stress, and each language against the other
- `docs/` — [what a checkpoint costs](docs/checkpoint-cost.md), [failure modes](docs/failure-modes.md)

## Status & License

Active development (pre-1.0). The rules are in the shared C cores, and Python and Go bind them. See [docs/checkpoint-cost.md](docs/checkpoint-cost.md) for measured checkpoint write amplification across row rates.

Apache License 2.0 ([LICENSE](LICENSE)). The build scaffolding in `src/duckdb/` comes from DuckDB's extension template, under its MIT license ([src/duckdb/LICENSE](src/duckdb/LICENSE)).
