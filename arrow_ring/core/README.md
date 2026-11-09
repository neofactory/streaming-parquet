# arrow-ring C core

`arrow_ring.h` is the spec. This document is how it hangs together.
Status 2026-08-19: v1 implemented and passing its smoke test.
`arrow_ring.c` (single file for v1; the module split below is the
target shape) covers ring lifecycle, leaf set/get, commit/pins,
the segment pool, and record-set export as ArrowArrayStream — verified
end-to-end against pyarrow via the C interfaces (`make` then
`python3 smoke_test.py`, 14/14: fixed/struct/nullable/utf8/list
columns, zero-copy buffer identity, pin-stall and release). Loud
not-implemented stubs: ar_append_batch/stream, ArWireMap,
ar_recordset_write_ipc, the parquet shim. nanoarrow not yet needed —
schema format strings are parsed directly; nanoarrow arrives with IPC.
Next: the CFFI binding so the Python package's 31-test suite runs on
this core (the acceptance gate); then ingest + wire maps.

## Why a C core

The ring's write path and stream ingest are the hot paths, and they
belong in a real language. One shared library, every language binds it:
Python (CFFI), Go (cgo), Rust (bindgen), C++ (directly). The row
ergonomics — MutableRow / ReadRow / record-set row iteration — are
binding-side sugar built on leaf indices and record-set batches; each
language gets its own idiomatic version, the core never knows.

## The boundary is Arrow's C interfaces, everywhere

- Schema in: `ArrowSchema` (C Data Interface). No invented type system.
- Batches in: `ArrowArray` -> `ar_append_batch` (the memcpy ingest).
- Record sets out: `ArrowArrayStream` -> DuckDB C API, Flight writers,
  IPC writers, pyarrow/arrow-go/arrow-rs importers, parquet.

This is what makes the core language-neutral without a wire format:
the C Data Interface IS the ABI the whole ecosystem already speaks.

## How streaming works (the part that must be native)

**Receive** (`ar_append_batch` / `ar_append_stream`): an IPC/Flight
batch is already columnar; "decode" is pointer arithmetic. Ingest per
fixed-width leaf = one memcpy (two on ring wrap); per var-width leaf =
one data memcpy into the segment pool + an O(n) offset rebase; bitmaps
memcpy when the landing slot is 8-aligned, shift-copy otherwise.
Nanoseconds per packet, no per-row work in any language.

**Send** (`ar_recordset_stream` / `ar_recordset_write_ipc`): the
batches ARE the ring's buffers — IPC encoding writes a small header
then the buffer bytes straight from ring memory. Frozen-after-commit
is what makes that safe; pins are what keep it safe while it happens.

**Parquet**: no pure-C parquet encoder exists, so parquet is a
companion module (`libarrow_ring_parquet`, a thin C-ABI shim over
parquet-cpp). The core does not link Arrow C++. Bindings may skip the
shim entirely and feed `ar_recordset_stream` to their host language's
parquet writer — same buffers either way.

## Modules

| file | contents | deps |
|---|---|---|
| `ring.c` | slots, cursors, commit, pins | none |
| `schema.c` | ArrowSchema walk -> flattened leaves, type surface | nanoarrow |
| `pool.c` | segmented var-width pool (immortal 1MB segments, no-straddle, oversized, freelist) | none |
| `ingest.c` | ar_append_batch/stream, ar_from_wire/to_wire | nanoarrow |
| `export.c` | record-set chunk cuts, ArrowArrayStream export, IPC write | nanoarrow (+ nanoarrow_ipc) |
| `parquet_shim.cc` | ArpWriter (separate .so) | Arrow C++ / parquet-cpp |

**nanoarrow** (Apache Arrow's single-file C library for the C Data
Interface + IPC) gets vendored when IPC lands — it exists for exactly
this embedding pattern. v1 has NO dependencies beyond libc/pthreads:
the C Data Interface structs are defined in arrow_ring.h and the
format strings are parsed directly.

## Semantics carried over from the reference implementation

Identical to the Python package (see its README): mutable -> frozen ->
recycled slot lifecycle; monotonic seqs, slot = seq % capacity;
nullable leaves reset to null on allocation, var leaves stamp the pool
cursor (keeps elem-starts cumulative for chunk math); the pin is the
guarantee, and a pin holds exactly its rows; writer-blocks-on-pin with the
caller owning the grace policy (AR_ERR_PIN_STALLED after block_ms);
pool growth never stalls the writer, pins only delay segment reuse;
chunk cuts at ring wrap + segment boundaries + rewrite
discontinuities; int32-offset overflow is a loud error naming the
field.

## Bindings plan

1. **Python first**: CFFI over `libarrow_ring.so`; the existing
   `arrow_ring` Python API keeps its exact surface (Ring,
   get_mutable_row, commit, read_row, get_relation, RecordSet) with
   descriptors dispatching to `ar_set_*`/`ar_get_*` by leaf index and
   `RecordSet.table` importing the ArrowArrayStream into pyarrow.
   The 31-test suite must pass unchanged — that is the acceptance
   gate, and the charter rules apply (no compat shims; the Python
   reference implementation is deleted only when the binding is green).
2. **Go**, when a native producer materializes: cgo over the same .so,
   Go-idiomatic row sugar.
3. **Rust/C++**: bindgen / direct, on demand.

## Future (explicitly out of scope for v1)

- Shared-memory control plane (atomic cursors + pin table in a mapped
  header) so multiple PROCESSES share one ring without Flight. The
  ABI version field in the header exists so this can arrive without
  breaking single-process users.
- Arrow C Device Interface (GPU) — not our problem.
