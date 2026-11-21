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

`streaming_parquet` ties these pieces together over a shared C core with thin bindings in Python and Go, plus a DuckDB extension. `arrow_ring` and `parquet_checkpoint` also stand on their own.

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
    archive=sp.Archive(checkpoint_seconds=15),
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
  streams/<name>/catalog.ducklake      # per-stream catalog of finished files
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
| **Ring** | Fixed-size, recycling column-major buffer (`arrow_ring`) holding recent rows in memory for zero-copy Arrow reads. |
| **Span / Pin** | A contiguous range of committed rows in the ring. Readers pin a span to keep those rows immutable until released. |
| **Checkpoint** | Immutable, versioned footer (`.ckpt`) indexing the stored pieces of an in-progress file. Costs only the new rows since the previous checkpoint. |
| **Assembly** | Combining an in-progress file's chunks and checkpoint footer into a standard `.parquet` file (server-side multipart copy on S3). Closing and crash recovery use the same step. |
| **Seam** | The exact row boundary where `duckdb_all` switches from stored files to live memory—no gaps, no duplicates. |
| **Catalog** | Per-stream DuckLake file (`catalog.ducklake`) indexing finished `.parquet` files with min/max stats for file skipping. |

### Methods

**Module**
- `create_stream(name, schema, store, spool, memory, archive)` — create and own a publishing stream (one writer per stream).
- `connect(name, store, memory, columns)` — attach to a stream for reading (zero-copy in-process; Arrow Flight over the network).
- `list_streams(store)` — list stream names with a ticket in the store.
- `create_playback_stream(name, parquet_path, ...)` — replay a recorded `.parquet` file as a live stream.
- `cut(label)` / `start_parquet(label)` / `stop_parquet()` — roll, start, or pause archival across all local streams.
- `close()` — flush, finish open files, and shut down Flight cleanly.

**Writing (`Stream`)**
- `get_mutable_row(block_s)` / `commit()` — fill one row directly in ring memory and publish it; slow work (Flight, archival, S3) runs asynchronously.
- `append(rows)` — bulk-ingest a PyArrow `RecordBatch`, `Table`, or `dict`.
- `wire_map(entries)` / `from_wire(wmap, frame)` — scatter packed binary frames directly into ring columns in one C call.
- `cut(label)` / `start_parquet(label)` / `stop_parquet()` — roll to a new file (with optional filename label), or toggle archival while keeping the stream live.

**Reading (`Stream` and connected readers)**
- `newest_row()` — read the latest committed row in memory.
- `cursor(since, columns)` — iterate unseen rows (or grab them as an Arrow table with `cursor.arrow_table()`).
- `arrow_table(columns, rows, since)` — zero-copy PyArrow `Table` over memory.
- `duckdb_live(con)` — live DuckDB view over memory only.
- `duckdb_archive(con)` — live DuckDB view over finished and in-progress stored files.
- `duckdb_all(con)` — unified DuckDB view across finished archive, open checkpoints, and live memory.

**Health & status**
- `archive_state()` / `mirror_state()` / `memory_state()` — check durability errors, S3 mirror backlog, and ring utilization.
- `standing()` / `is_connected()` — check a reader's connection state in plain words.

## Status & License

Active development (pre-1.0); Python 3.11+ with a shared C core being unified across Python and Go bindings. See [docs/checkpoint-cost.md](docs/checkpoint-cost.md) for measured checkpoint write amplification across row rates.

Apache License 2.0 ([LICENSE](LICENSE)).
