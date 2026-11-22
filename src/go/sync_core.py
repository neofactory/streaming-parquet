"""The Go packages' copies of the C cores.

cgo compiles only the C files inside a package's own directory, so each
Go package has checked-in copies of the sources it needs from src/core.

    python3 src/go/sync_core.py            copy every source that differs
    python3 src/go/sync_core.py --check    exit 1 and name each copy that differs

The Python-against-Go tests (tests/*/run_go*_compat.py) copy first and
then check, so a run of them leaves the copies current.
"""
import filecmp
import os
import shutil
import sys

GO = os.path.dirname(os.path.abspath(__file__))
CORE = os.path.join(os.path.dirname(GO), "core")

#: (source in src/core, copy in src/go)
COPIES = [
    ("arrow_ring.c", "ring/arrow_ring_core.c"),
    ("arrow_ring.h", "ring/arrow_ring.h"),
    ("pqc.c", "pqc/pqc_core.c"),
    ("pqc.h", "pqc/pqc.h"),
    ("sp.c", "streaming/sp_core.c"),
    ("sp_catalog.c", "streaming/sp_catalog.c"),
    ("sp.h", "streaming/sp.h"),
]


def differing():
    """The copies that are not their source, as (source, copy) paths."""
    out = []
    for src, dst in COPIES:
        src, dst = os.path.join(CORE, src), os.path.join(GO, dst)
        if not os.path.exists(dst) or not filecmp.cmp(src, dst, shallow=False):
            out.append((src, dst))
    return out


def sync():
    """Copy every source that differs; returns the copies written."""
    done = differing()
    for src, dst in done:
        shutil.copyfile(src, dst)
    return [dst for _src, dst in done]


if __name__ == "__main__":
    if "--check" in sys.argv[1:]:
        bad = differing()
        for src, dst in bad:
            print(f"{dst} differs from {src}")
        sys.exit(1 if bad else 0)
    for dst in sync():
        print(f"copied {dst}")
