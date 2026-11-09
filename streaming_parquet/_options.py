"""The option objects and the named results of the public API.

Options, given once at create_stream / attach:

    Memory(lookback, maximum, segment_bytes)
    Archive(checkpoint_seconds, row_group_rows, row_group_seconds,
            ship_seconds, parquet)

Results, read whenever a host wants them:

    ArchiveState   the first copy's standing
    MirrorState    the S3 mirror's standing
    MemoryState    what the stream holds in memory
    View           a DuckDB view registered on a connection

Named objects with attributes, not dicts: a misspelled attribute is an
AttributeError at the call site, not a KeyError somewhere later.
"""

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass(frozen=True)
class Memory:
    """How much a stream keeps in memory.

    lookback: rows the ring holds (on this core a row count; the V2B
              core takes a duration). None = the library default, two
              parquet row groups (245,760 rows).
    maximum:  bytes at which the writer stalls. Not enforced on this
              core (the ring is a fixed capacity); accepted so a host
              can say it now.
    segment_bytes: the variable-width pool's segment size (tests).
    """
    lookback: Optional[int] = None
    maximum: Optional[int] = None
    segment_bytes: Optional[int] = None


@dataclass(frozen=True)
class Archive:
    """How a stream's rows are written to parquet. Where is the store
    and the spool, given to create_stream; this is the tuning.

    checkpoint_seconds: the tail-checkpoint cadence; crash loss is at
            most this many seconds. None = durability per row group.
    row_group_rows: rows per parquet row group. None = DuckDB's
            122,880. Also sizes the ring at two row groups when Memory
            gives no lookback.
    row_group_seconds: also end a row group after this long. None =
            size-bound only.
    ship_seconds: cadence of the pass that moves the spool's files to a
            bucket store.
    parquet: dict forwarded to the parquet writer (compression,
            compression_level, use_dictionary, column_encoding).
    catalog_copy_seconds: the interval of the catalog's dated copies,
            catalog.<YYYYMMDD-HHMMSS>.ducklake beside it, each made only
            if the catalog changed since the newest one. None = no copies.
    """
    checkpoint_seconds: Optional[float] = None
    row_group_rows: Optional[int] = None
    row_group_seconds: Optional[float] = None
    ship_seconds: float = 5.0
    parquet: dict = field(default_factory=dict)
    catalog_copy_seconds: Optional[float] = 86400.0


@dataclass(frozen=True)
class ArchiveState:
    """The first copy's standing. A failing first copy means rows are
    durable nowhere but the ring. lost_rows never resets. `file` is the
    name of the file open right now, or None when none is: the next
    committed row then opens a new one (a host stamps header state onto
    the rows it writes while that is so, so the state lands in the new
    file's first records). catalog_error: the catalog's last failure, None
    when it lists every finished file (or the stream keeps none); a
    failing catalog loses no rows, readers just see the newest files
    late."""
    failing: bool
    error: Optional[str]
    lost_rows: int
    location: Optional[str]
    file: Optional[str] = None
    catalog_error: Optional[str] = None


@dataclass(frozen=True)
class MirrorState:
    """The S3 mirror's standing. pending: files ended locally whose
    remote copy is not yet assembled."""
    failing: bool
    error: Optional[str]
    pending: list


@dataclass(frozen=True)
class MemoryState:
    """What the stream holds in memory right now.

    rows:            committed rows readable from memory
    rows_unarchived: of those, rows not yet written to the archive
                     (0 for a reader with no archive of its own)
    capacity:        rows the ring can hold
    """
    rows: int
    rows_unarchived: int
    capacity: int


class View:
    """A DuckDB view registered on a connection by a duckdb_* call:
    `con.sql(f"select ... from {view.name}")`. close() drops it."""

    def __init__(self, con, name, unregister=None):
        self.name = name
        self._con = con
        self._unregister = unregister
        self._closed = False

    def close(self):
        if self._closed:
            return
        self._closed = True
        try:
            self._con.execute(f'DROP VIEW IF EXISTS "{self.name}"')
        except Exception:
            pass
        if self._unregister is not None:
            try:
                self._unregister()
            except Exception:
                pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def __repr__(self):
        return f"View({self.name!r})"
