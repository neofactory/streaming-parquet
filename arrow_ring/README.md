# arrow_ring

A recycling builder over ring storage. Writers fill mutable slots;
`commit()` freezes them; frozen regions are honest zero-copy Arrow
record batches, because pins prevent the write cursor from recycling
a region while anyone holds it.

The contract is an **Arrow schema** — this is a parquet/Arrow-native
telemetry store. Storage is Arrow's own buffers (`pa.allocate_buffer`,
aligned, tracked by Arrow's memory pool); element access is stdlib
memoryview/struct. **The only dependency is pyarrow** — no numpy.
Heavy scans are DuckDB's job: the ring produces format, the query
engine does math. Standalone by construction: it imports nothing
but pyarrow.
Intended to be extractable as an Arrow-ecosystem library (`arrow-ring`
on PyPI) via subtree split.

## Type surface (and the deliberately drawn line)

| supported | notes |
|---|---|
| int8–64, uint8–64, float16/32/64 | one buffer, zero-copy |
| bool | bit-packed, zero-copy via Arrow bit offsets |
| timestamp/date32/64/time32/64/duration | stored as raw integer ticks |
| decimal128/256, fixed_size_binary | fixed-width byte slots |
| fixed_size_list\<fixed-width primitive\> | per-row vector (e.g. xyz) |
| struct (any depth, non-nullable) | flattened to leaf columns |
| nullable leaves | validity bitmap; non-nullable columns carry nothing |
| utf8/large_utf8, binary/large_binary | segment pool (below) |
| list/large_list\<fixed-width primitive\> | segment pool, typed elements |

**Not in this version** (the constructor refuses them with a
`TypeError`): map, union, dictionary (an encoding — the parquet writer
applies it transparently), run-end, interval, list of nested or
variable-width types, nullable structs (make the leaves nullable
instead). These were left out to keep the first implementation
tractable, not because they are unwanted; list of utf8 is the first
one owed.

## API

```
Ring(schema, capacity, rate_hint=None, segment_bytes=None, lease_margin_s=None)
ring.get_mutable_row(block_s=0) -> MutableRow      # row.field = v; row.servo.pos = v
ring.commit() -> n
ring.append_batch(batch) / ring.append_table(table)
ring.read_row(seq) -> Row            # MutableRow is its writable subclass
ring.oldest_seq / ring.newest_seq
ring.get_static_relation(duration_s) -> RecordSet
ring.pin(lo, hi) -> RecordSet                      # exactly these rows, no lease
ring.iter_chunks(lo=0, hi=None, chunk_rows=16384)  # -> (first_seq, RecordBatch)...: a pin per chunk
ring.chunked_reader(lo=0, hi=None, chunk_rows=16384) -> pyarrow.RecordBatchReader
ring.get_dynamic_relation(chunk_rows=16384) -> DynamicRelation
ring.wire_map([(path, offset, size), ...]) -> WireMap
ring.from_wire(wmap, frame) / ring.to_wire(wmap, seq, frame_len) -> bytes   # frame: bytes or any buffer
RecordSet: .table .lo .hi .release() .detach() ; context manager
DynamicRelation: .table ; __arrow_c_stream__       # register with DuckDB
errors: StaleReadError, PinStalledError
planned: Ring(..., base_seq=S) ; gap-advance append (attach alignment)
C core: ar_* mirror of the above (core/arrow_ring.h is the spec)
```

## The API (this README is the contract)

```python
import pyarrow as pa
import arrow_ring

schema = pa.schema([
    pa.field('tv', pa.float64(), nullable=False),
    pa.field('servo', pa.struct([pa.field('pos', pa.int32(), nullable=False),
                                 pa.field('vel', pa.int32(), nullable=False)]),
             nullable=False),
    pa.field('label', pa.utf8()),           # nullable by default
])
ring = arrow_ring.Ring(schema, capacity=600_000, rate_hint=10_000)
```

- `capacity` — rows. The total loss-free budget for every consumer
  (lookback + regions in flight + lease exclusions). Size it 2x what
  you think you need.
- `rate_hint` — expected rows/second; seeds the lease-exclusion rate
  until the observed rate takes over.

### Write side (single writer thread)

```python
row = ring.get_mutable_row()      # allocate next slot
row.tv = t
row.servo.pos = 100               # struct leaves: sub-handle, same slot
row.label = "coarse settled"      # bytes go to the segment pool
ring.commit()                     # freeze everything allocated so far
```

- `get_mutable_row(block_s=0.0)` — the privileged allocator call (the
  long name is deliberate). Allocating seq S recycles seq S−capacity;
  if a pin holds that seq the call blocks up to `block_s` then raises
  `PinStalledError` naming the overdue lease. On an origin ring treat
  the raise as fatal. Allocation resets nullable leaves to null and
  var-width leaves to empty.
- A store is one descriptor call and no call into C: `row.x = v`
  writes into a `memoryview` over the leaf's own ring buffer at the
  open slot and sets the validity bit itself (the core exposes the
  buffer addresses for exactly this; the slot is `seq % capacity`).
  memoryview is strict, so an out-of-range int, a float into an int
  leaf, or a str refuse with ValueError / TypeError, the same classes
  the core's refusals map to. Temporal fields accept raw integer ticks
  or stdlib datetime/date/timedelta (naive datetimes are treated as
  UTC); decimals accept `decimal.Decimal` or unscaled ints; `None`
  stores null (nullable leaves only — a non-nullable leaf raises
  TypeError). Strings, lists and float16 go through the core's
  setters. Measured (M-series Mac): a float64 store 72 ns, the
  allocator 190 ns; a 20-row, 7-column packet 14 µs, where the
  cgo-per-field path took 46 µs.
- The mutable handle is write-only and there is ONE per ring, reused
  for every row (struct sub-handles included); `get_mutable_row` moves
  it to the new slot. Read committed rows with `read_row(seq)`.
- `commit()` — pointer advance; returns rows frozen. Committed rows
  must never be written again through a retained handle.

### Read side (any thread)

```python
r = ring.read_row(seq)            # read-only view, no pin
r.servo.pos                       # StaleReadError if recycled
r.mark                            # None when null

with ring.get_static_relation(duration_s=2.0) as rs:
    con.register("hot", rs.table) # zero-copy pyarrow Table
    con.execute("SELECT ... FROM hot WHERE tv BETWEEN ? AND ?")
```

- `read_row(seq)` — unpinned single-row view for hot-path consumers.
  Consume immediately. Temporal/decimal leaves read back raw storage
  values; fixed_size_list and list leaves read back tuples; use a
  record set for logical values.
- `get_static_relation(duration_s)` — a pinned, immutable record set. The
  lease is the caller's honest hold estimate; the ring excludes the
  oldest `peak_rate x (duration_s + margin)` rows so an honest holder
  never blocks the writer. The pin, not the lease, is the guarantee.
  Pins also hold the referenced pool segments (see below).
- `pin(lo, hi)` — a pinned record set over exactly the rows named, and
  no lease: for a caller that knows which rows it needs (the publish
  and archive paths pin the rows they send or write, usually a few).
  The writer meets this pin only after a whole ring of writes, and
  then the holder is a leak. `lo` below the oldest row is raised to
  it (the set's `lo` says so); `hi` above committed is lowered.
- `iter_chunks(lo, hi, chunk_rows)` — the read for a consumer that
  pulls batches one at a time (a DuckDB scan): nothing pinned at open;
  the range fixed at the first chunk, not at open, so a consumer that
  reaches memory late reads memory as it is then; each batch pins its
  own rows and no others, dropping the pin when the batch is freed. A
  row the writer recycles before the read reaches it raises
  `StaleReadError` naming the rows: the read was lapped. `DynamicRelation`
  is this read behind `__arrow_c_stream__`; it needs no lease.
- `RecordSet.table` — zero-copy `pyarrow.Table`. Chunk boundaries:
  the physical ring wrap, plus segment boundaries / re-writes in
  var-width columns (Arrow offsets must be gap-free per chunk). String
  DATA and fixed-width columns are zero-copy; per-chunk offsets and
  var-column validity bitmaps are small fresh buffers.
- `ring.oldest_seq` / `ring.newest_seq` — the readable window.

### The segment pool (variable-width storage)

Per var-width leaf, lazy (a string-free stream allocates nothing):
1 MB fixed segments that **never move** — growth appends a segment, so
zero-copy views stay valid forever and the writer NEVER stalls on the
pool; pins only delay segment reuse (memory grows instead, which is the
deliberate trade). Element-unit allocation keeps offsets gap-free; no
value straddles a segment; a value bigger than a segment gets a
dedicated exact-size segment plus a diag print (a *stream* of oversized
values is a workload smell). Retired standard segments go to a
freelist; dedicated ones drop when their record sets release.

### Errors

- `StaleReadError` — seq recycled or not yet committed. Recoverable:
  resync from `oldest_seq`/`newest_seq`.
- `PinStalledError` — writer reached a pinned region: a consumer held
  a record set for a whole ring's worth of writes. Message carries the
  lease and hold time.
- `TypeError` at construction for anything outside the type surface.

## C core

The native core is defined in [`core/arrow_ring.h`](core/arrow_ring.h)
(the header is the spec; `core/README.md` explains the architecture):
one C library bound by Python/Go/Rust, Arrow C Data Interface at every
boundary, memcpy stream ingest, ArrowArrayStream egress, parquet as a
C-ABI shim over parquet-cpp. This Python package is the executable
reference; its test suite is the core's acceptance suite.

## Go host (go/ring)

A thin cgo binding over the same core (checked-in copies of
`core/arrow_ring.{c,h}`, synced and drift-checked by
`run_go_compat.py`), speaking arrow-go at the boundary. The leaf
setters store straight into the open row's slot with no cgo call for
fixed-width leaves (`direct.go`: the shape comes from the schema, the
buffer addresses from the core, the refusals are the core's with the
same status names): `SetF64` is 2.2 ns against 19.6 ns through cgo.
Strings, lists and float16 keep the core's setters.

```go
r, _ := ring.NewRing(schema, capacity, &ring.Options{RateHint: 5000})
seq, _ := r.RowBegin(time.Second)       // ar_row_begin
r.SetI64(leaf, v); r.SetString(leaf, s) // leaf setters
r.Commit()
r.AppendRecord(rec)                     // bulk memcpy ingest
r.AppendRecordAt(rec, firstSeq)         // attach-side seq alignment:
                                        //   gap = loud jump + floor
v, isNull, err := r.GetI64(seq, leaf)   // hot-path read; IsStale(err)
m, _ := r.NewWireMap(entries)           // FromWire / ToWire
rs, _ := r.StaticRelation(2*time.Second)
recs, _ := rs.Records()                 // zero-copy arrow-go records
rs.Release()                            // or rs.Detach()
```

Cross-language gate: `run_go_compat.py` — Go record sets written as
IPC read exactly in pyarrow (struct leaves and strings included);
pyarrow IPC bulk-ingests into a Go ring and reads exactly through the
row getters. Note Go `arrow.Field` defaults to non-nullable — match
nullability across languages or the ring refuses the schema loudly.

## Not yet

- list of struct / nested lists (excluded by design until a real need).

## Tests and tools

```sh
python3 packages/arrow_ring/run_tests.py           # acceptance suite (38)
python3 packages/arrow_ring/run_pipeline_tests.py  # parquet, DuckDB union
                                                   # view, Flight loopback
python3 packages/arrow_ring/run_go_compat.py       # Go <-> Python cross
python3 packages/arrow_ring/exerciser.py --seconds 15 --rate 20000
                                                   # endurance/throughput
(cd packages/arrow_ring/go && go test ./ring/)     # Go host suite
(cd packages/arrow_ring/core && make san)          # ASan+UBSan C soak
(cd packages/arrow_ring/core && make leaks)        # macOS leaks check
```
