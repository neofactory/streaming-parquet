# streaming-parquet

Live telemetry streaming and parquet archival in one system: every
committed row is queryable from the moment it is written until the
end of the archive, through one SQL view.

    a = telemetry.attach("plant/voltages")
    view = a.duckdb_all(con)                 # register once
    con.sql(f"SELECT ... FROM {view.name}")   # every query current

Three packages, one contract, two languages (Python and Go over
shared C cores):

| package | role | docs |
|---|---|---|
| [`arrow_ring`](arrow_ring/README.md) | the hot tier: a recycling builder over ring storage, zero-copy Arrow out | its README is the contract |
| [`parquet_checkpoint`](parquet_checkpoint/README.md) | the durable format: crash-durable, queryable-while-open parquet (immutable objects + a checkpoint; a checkpoint costs what its new rows cost), local and S3 | includes the errata list |
| [`streaming_parquet`](streaming_parquet/README.md) | the composition: create/attach streams, Flight fan-out, archival, the timeline relations | the system README |

Each package stands alone: use the ring without parquet, or the
checkpoint writer without streaming. `streaming_parquet` composes
them.

## Install

Python 3.11+. One distribution, `streaming-parquet`, holds all three
packages; it depends on pyarrow and cffi. Extras: `duckdb` (the
timeline relations) and `s3` (boto3: `S3Sink`, `s3://` stores). A uv
project depends on a checkout like this:

    dependencies = ["streaming-parquet[duckdb]"]

    [tool.uv.sources]
    streaming-parquet = { path = "../streaming-parquet", editable = true }

`arrow_ring` builds its C core with `make` and the system C compiler
on first import, next to its sources (`arrow_ring/core/`), so that
directory must be writable; in an editable install it is the checkout.
`parquet_checkpoint` does the same for its core
(`parquet_checkpoint/core/`) the first time a file is written; reading
needs no build.

## Layout

- `<pkg>/` — the Python package (the reference implementation)
- `<pkg>/core/` — the C core (C11, libc(+pthreads) only; the header
  is the spec)
- `<pkg>/go/` — the Go host (cgo over the same core)
- `<pkg>/run_*.py` — standalone suites: unit, stress, cross-language
  parity
- `docs/` — documents that cross the packages:
  [what a checkpoint costs](docs/checkpoint-cost.md)

## Open question: native implementations

What exists today, top layer to bottom:

| language | streaming layer | ring + checkpoint cores |
|---|---|---|
| Python | native (the reference implementation) | binds the C cores (CFFI) |
| Go | native | binds the C cores (cgo) |
| C | — | the canonical cores (C11, libc-only; headers are the spec) |

The open question is native implementations beyond these — **Rust,
Java, C#, and a cgo-free native-Go core** — because the ecosystem
precedent (arrow-rs, parquet-java, Parquet.NET) is that data-plane
libraries get built natively per language, and we would rather those
implementations live in this repo, validated by the same parity and
stress suites, than appear beside it. The spec (the C headers, the
PQC2 format doc, the errata list) plus the cross-implementation test
harness exist precisely so a port can be written and PROVEN
equivalent.

Deliberately not yet: ports should target a mature contract, not a
moving one. They start making sense after the library has more
mileage — hardened tests, the bucket-based stream sharing, the
example streams — so that a new implementation validates against
settled behavior. If one of these is your itch, open an issue; the
harness is ready even though the invitation is early.

## Status

Working system under active development; APIs documented in each
package README. Suites: unit + crash gauntlets + C/Python byte
parity + Go/Python wire compatibility. A cross-stack chaos rig, kept
outside this repo, has verified 650M-row runs exact.

## License

Apache License 2.0 ([LICENSE](LICENSE)). The build scaffolding in
`duckdb-checkpoint/` comes from DuckDB's extension template, under its
MIT license ([duckdb-checkpoint/LICENSE](duckdb-checkpoint/LICENSE)).
