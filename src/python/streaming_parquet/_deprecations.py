"""The old names that still work, and the day they stop.

docs/deprecations.md lists the same rows for people; a test holds the
two together. An old name does what its new name does, and on its first
use in a process it logs one warning that gives the new name and the
day the old one is removed.
"""

import logging

_diag = logging.getLogger("telemetry.diag")

DEPRECATED_ON = "2026-11-11"
REMOVED_AFTER = "2027-02-11"

#: old name -> new name
OLD_NAMES = {
    "Memory": "MemoryOptions",
    "Archive": "ParquetOptions",
    "Archive(parquet=...)": "ParquetOptions(compression=...)",
    "create_stream(archive=...)": "create_stream(parquet=...)",
}

_told = set()


def old_name(old):
    """Say once that `old` is an old name. Raises KeyError for a name
    the table does not hold, so every old name is in the table."""
    new = OLD_NAMES[old]
    if old in _told:
        return
    _told.add(old)
    _diag.warning(f"[streaming-parquet] {old} is an old name for {new}. "
                  f"It works until {REMOVED_AFTER}; use {new}. "
                  f"docs/deprecations.md has the list.")
