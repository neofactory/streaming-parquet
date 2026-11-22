# Old names and when they stop

A name in this table still works. It does what its new name does. Each
old name stays for three months after the day it was replaced, and is
then removed.

On its first use in a process, an old Python name logs one warning that
gives the new name and the day of removal. Go has no such warning: the
old name carries a `// Deprecated:` comment, which editors and
`staticcheck` show.

| Old name | New name | Language | Replaced | Removed on or after |
|---|---|---|---|---|
| `Memory` | `MemoryOptions` | Python | 2026-11-11 | 2027-02-11 |
| `Archive` | `ParquetOptions` | Python | 2026-11-11 | 2027-02-11 |
| `Archive(parquet=...)` | `ParquetOptions(compression=...)` | Python | 2026-11-11 | 2027-02-11 |
| `create_stream(archive=...)` | `create_stream(parquet=...)` | Python | 2026-11-11 | 2027-02-11 |
| `suppress_console` | `logging.getLogger("streaming_parquet").propagate = False` | Python | 2026-11-22 | 2027-02-22 |
| `set_verbosity` | `logging.getLogger("streaming_parquet").setLevel(...)` | Python | 2026-11-22 | 2027-02-22 |
| `ArchiveOptions` | `ParquetOptions` | Go | 2026-11-11 | 2027-02-11 |

## Changes with no old name

Go cannot keep an old field beside a new one of the same name, so these
changed in one step. Code that used the old form does not compile, and
the compiler names the line.

| Old form | New form | Changed |
|---|---|---|
| `StreamOptions{Parquet: true, Archive: ArchiveOptions{...}}` | `StreamOptions{Parquet: &ParquetOptions{...}}` | 2026-11-12 |
| `StreamOptions{Archive: ...}` then `StartParquet(label)` | `StartParquet(label, &ParquetOptions{...})` | 2026-11-12 |
| `Stream.StartParquet(label)` | `Stream.StartParquet(label, nil)` | 2026-11-12 |
| `AttachOptions{Archive: ArchiveOptions{...}}` | `AttachOptions{Parquet: &ParquetOptions{...}}` | 2026-11-12 |
| `s.Ring().Commit()` then `s.Publish()` | `s.Commit()`; the stream's goroutine writes the files | 2026-11-12 |
| the error returned by `s.Publish()` | `s.ArchiveState().Failing` and `.Error` | 2026-11-12 |
| `s.Publish()`, the step after a commit | removed; `s.Publish()` now makes an internal stream findable and readable | 2026-11-12 |

## Logging, changed 2026-11-22

The Python library no longer writes `telemetry-diag.log` by itself, and
no longer prints its lines on standard error by itself. It logs to the
logger `streaming_parquet` of `logging`, and the program decides where
the lines go. A program that wants the file calls
`sp.log_to_file("telemetry-diag.log")`. The logger's old name,
`telemetry.diag`, is gone: a handler a program put on it gets nothing.

In Go the lines go to `Config.Logger`, a `*slog.Logger`; with none,
warnings and errors go to standard error. Before, every line was
printed on standard error.

## The rule for names

A name that ends in `Options` is something you give: `MemoryOptions`,
`ParquetOptions`. A name that ends in `State` is something you read
back: `MemoryState`, `ArchiveState`, `MirrorState`.

## To replace a name

1. Give the new name its code. Make the old name call it.
2. Add a row here. In Python, add the same row to
   `streaming_parquet/_deprecations.py`; a test fails when the two
   differ.
3. On the day of removal, delete the old name, its row here, and its
   row in `_deprecations.py`.
