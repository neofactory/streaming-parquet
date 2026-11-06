# parquet_checkpoint (DuckDB extension)

A DuckDB file system for a Parquet file that is still being written.
An unfinished file in streaming-parquet is stored as chunk objects plus
checkpoint versions, each one object written once and never changed,
`<file>.parquet.<version>.ckpt` (the chunk list, the newest rows and a
footer; format in `../parquet_checkpoint/core/pqc.h`). This extension
presents those pieces as one ordinary Parquet file, so DuckDB's own
Parquet reader reads it unchanged: column selection, row-group skipping
and ranged reads all work.

    SELECT * FROM read_parquet('dir/live.parquet.000017.ckpt');            -- local
    SELECT * FROM read_parquet('pqc://s3://bucket/live.parquet.000017.ckpt'); -- any scheme

A bare local `.ckpt` path is claimed by its suffix. Any path with a
scheme is wrapped in `pqc://`, and the wrapped path is opened through
DuckDB's own file systems (httpfs for S3), so the extension never
claims a path another file system should open. The ticket lists each
stream's unfinished files by the path of their newest checkpoint
version. A version never changes once written, so DuckDB's caches and
its S3 reads, which remember an object by its name, can never mix two
versions.

## Loading

DuckDB has not signed the extension, so it loads only on a connection
opened with `allow_unsigned_extensions`; the setting cannot be turned
on for a connection already open.

    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute("LOAD '.../parquet_checkpoint.duckdb_extension'")

The command line: `duckdb -unsigned`.

## Building

A build serves one exact DuckDB version: the tag of the `duckdb`
submodule (today v2.0.0), with `extension-ci-tools` on the matching
branch (`v2.0-cyanoptera`). Every DuckDB update, patch releases
included, needs a rebuild for each platform.

    DUCKDB_VERSION=v2.0.0 GEN=ninja make release
    python3 run_extension_tests.py      # CKPT_TEST_STORE=s3://bucket adds the S3 case

`DUCKDB_VERSION` names the version the binary declares. Without it the
build takes it from `git describe`, and a shallow submodule (only the
tag fetched) yields the bare commit hash: DuckDB 2.0.0 then refuses the
binary ("built specifically for DuckDB version 'd54cbbe3df'").

The binary is `build/release/extension/parquet_checkpoint/parquet_checkpoint.duckdb_extension`.
A build folder left from another DuckDB version keeps its old settings
in `build/release/CMakeCache.txt`; delete that file first.

`docs/UPDATING.md` is the template's guide to moving to a new DuckDB
version.
