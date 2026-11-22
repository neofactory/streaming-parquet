"""Command-line face for parquet_checkpoint.

    python3 -m parquet_checkpoint status <dir-or-file>...
        Show each file's state (done / live+checkpointed / corrupt)
        with checkpointed row counts and chunk counts.

    python3 -m parquet_checkpoint mend <dir-or-file>...
        Finalize every checkpoint: the boot sweep, runnable by hand.

    python3 -m parquet_checkpoint read <file.parquet> [-n ROWS]
        Print the tail of a live or finished file (via the checkpoint
        reader) -- a quick look at a capture still being written.

    python3 -m parquet_checkpoint verify <file.parquet>...
        Deep check: every chunk CRC and every parquet page CRC.
"""

import os
import sys

from . import (CheckpointError, CheckpointReader, DirSink, ckpt_versions,
               finalize, live_metadata, live_table, parse_ckpt_object,
               recover_directory, verify)


def _targets(args):
    """Expand dir args to their .parquet members (final-path names)."""
    out = []
    for a in args:
        if os.path.isdir(a):
            for name in sorted(os.listdir(a)):
                got = parse_ckpt_object(name)
                if got is not None:
                    out.append(os.path.join(a, got[0]))
                elif name.endswith('.parquet'):
                    out.append(os.path.join(a, name))
        else:
            got = parse_ckpt_object(os.path.basename(a))
            out.append(os.path.join(os.path.dirname(a), got[0]) if got else a)
    return sorted(set(out))


def cmd_status(paths):
    for p in _targets(paths):
        sink = DirSink(os.path.dirname(p) or '.')
        name = os.path.basename(p)
        versions = ckpt_versions(sink, name)
        if sink.exists(name) and versions:
            print(f"  CONFLICT    {p}  (both final file and checkpoint "
                  f"exist -- refusing to guess)")
        elif sink.exists(name):
            md = live_metadata(p)
            print(f"  done        {p}  ({md.num_rows:,} rows, "
                  f"{md.num_row_groups} row groups)")
        elif versions:
            try:
                r = CheckpointReader(versions[-1][1], sink)
                print(f"  live/crash  {p}  ({r.num_rows:,} rows "
                      f"checkpointed, {len(r.manifest)} chunks, "
                      f"{r.logical_size:,} logical bytes)")
            except CheckpointError as e:
                print(f"  CORRUPT     {p}  ({e})")
        else:
            print(f"  missing     {p}")
    return 0


def cmd_mend(paths):
    rc = 0
    for a in paths:
        if os.path.isdir(a):
            results = recover_directory(a)
            for status, where in results:
                print(f"  {status:9s} {where}")
            if not results:
                print(f"  (nothing to mend in {a})")
        else:
            got = parse_ckpt_object(os.path.basename(a))
            p = os.path.join(os.path.dirname(a), got[0]) if got else a
            sink = DirSink(os.path.dirname(p) or '.')
            try:
                status, where = finalize(os.path.basename(p), sink)
                print(f"  {status:9s} {os.path.join(sink.root, where)}")
            except CheckpointError as e:
                print(f"  ERROR     {p}: {e}")
                rc = 1
    return rc


def cmd_read(paths):
    n_rows = 10
    if '-n' in paths:
        i = paths.index('-n')
        n_rows = int(paths[i + 1])
        paths = paths[:i] + paths[i + 2:]
    for p in paths:
        t = live_table(p)
        print(f"# {p}: {t.num_rows:,} rows")
        tail = t.slice(max(0, t.num_rows - n_rows))
        for row in tail.to_pylist():
            print(f"  {row}")
    return 0


def cmd_verify(paths):
    rc = 0
    for p in paths:
        try:
            n = verify(p)
            print(f"  OK        {p}  ({n:,} rows, every chunk and page "
                  f"CRC-verified)")
        except Exception as e:
            print(f"  CORRUPT   {p}: {e}")
            rc = 1
    return rc


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        return 2
    cmd, args = sys.argv[1], sys.argv[2:]
    if cmd == 'status':
        return cmd_status(args)
    if cmd == 'mend':
        return cmd_mend(args)
    if cmd == 'read':
        return cmd_read(args)
    if cmd == 'verify':
        return cmd_verify(args)
    print(__doc__)
    return 2


if __name__ == '__main__':
    sys.exit(main())
