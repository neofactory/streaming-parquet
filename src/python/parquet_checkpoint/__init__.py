"""parquet_checkpoint -- crash-durable, queryable-while-open parquet.

Three things, each usable alone:

  CheckpointWriter   write a parquet file as chunks + a checkpoint
  CheckpointReader   query a live or crashed file as ordinary parquet
  finalize           turn a checkpoint into an ordinary .parquet file

Artifacts (relative names, resolved by a sink -- local dir or S3):

  run.parquet.chunks/000007   an object: an immutable piece of the
      file, whole row groups, written once and never changed. The file
      while it is written is a list of objects in file order:
        chunks       >= chunk_bytes of final row groups each (5 MB:
                     the S3 multipart part minimum, a packaging size,
                     never a durability boundary)
        loose groups final row groups that do not yet fill a chunk,
                     one object each
        unfinished   the rows of the row group still being filled: one
                     object for each checkpoint's new rows, merged in
                     steps (src/core/pqc.h)
  run.parquet.000017.ckpt     a checkpoint: version 17 of the file's
      checkpoint, one object, written once and never changed. Each
      checkpoint is a new version; a version superseded for
      SUPERSEDED_KEEP_S (10 minutes) is deleted in the step that uploads
      a later one, so a reader told of an older version less than that
      long ago still finds it, and an object a reader has opened never
      changes under it. An object is deleted with the last kept version
      that names it. Layout:
        header    magic PQC2, version, CRC-32 of everything after the
                  CRC field, final file name, watermark (rows covered),
                  data offset
        manifest  the list of objects: name, byte length, CRC-32
        data      the footer of the whole [thrift+len+magic]; for a
                  file with no rows, a whole empty parquet file
      The objects in list order, then the data region, ARE the file:
      finalize joins them (on S3 server-side: a copy for each chunk,
      and one last part of whatever is smaller).
  run.parquet                 the finished ordinary parquet file.

The cost of a checkpoint follows its new rows, not the size of the
file: it stores one object with those rows and the checkpoint object,
never a row a stored object already holds.

Durability ordering: an object is stored before any checkpoint that
names it; a checkpoint version is stored whole before older versions
are deleted. Crash loss = rows since the last checkpoint. Recovery uses
the newest version whose CRC holds. write_table implies a checkpoint by
default.

Rows have ordinal identity: the checkpoint's watermark is its row
count; readers virtualize per-row ordinals (file_row_number) rather
than requiring a seq column.
"""

import io
import re
import struct
import threading
import time
import zlib

import pyarrow as pa
import pyarrow.parquet as pq

from . import _core
from ._sinks import DirSink, FakeS3, S3Sink, SubSink, SinkError  # noqa: F401

__all__ = ['CheckpointWriter', 'CheckpointReader', 'finalize',
           'recover_directory', 'dead_after',
           'live_table', 'live_metadata', 'verify',
           'CheckpointError', 'DirSink', 'FakeS3', 'S3Sink', 'SubSink', 'Shipper']

MAGIC = b'PAR1'
CKPT_MAGIC = b'PQC2'
CKPT_VERSION = 2


class CheckpointError(Exception):
    """Loud failure: inconsistent checkpoint state (never guessed away)."""


def _encode_mini(table, schema=None, **writer_options):
    """`table` as a complete parquet file of its own, offsets from 0: a
    "mini file", what the core takes a row group as. `schema` overrides
    the table's own (the writer's schema carries file-level KV metadata,
    e.g. sp_base_seq, that region slices from a ring do not)."""
    buf = io.BytesIO()
    schema = schema if schema is not None else table.schema
    if table.schema.metadata != schema.metadata:
        table = table.replace_schema_metadata(schema.metadata)
    w = pq.ParquetWriter(buf, schema, **writer_options)
    w.write_table(table)
    w.close()
    return buf.getvalue()


# ---- checkpoint versions --------------------------------------------------------

#: A checkpoint version superseded at least this long is deleted, in the
#: step that uploads a later one (nothing runs on a timer): a reader told
#: of a version less than this long ago still finds it. One rule for a
#: live file's versions (superseded by the next one) and a finished
#: file's checkpoints (superseded when the ticket drops the file).
SUPERSEDED_KEEP_S = 600.0

def _versions(keep_s):
    """The rule for a file's checkpoint versions in one place (the C
    core's, src/core/pqc.h: CHECKPOINT VERSIONS)."""
    from . import _core
    return _core.Versions(keep_s)


def _stored_version(versions, sink, obj, ckpt):
    """The version `obj` (bytes `ckpt`) is stored on `sink`: it replaces
    the newest, and what the rule says is due is deleted, a version
    first and then the objects only it named."""
    versions.stored(obj, ckpt)
    for name in versions.due():
        sink.delete(name)
        versions.deleted(name)


def ckpt_object(name, seq):
    """The object name of version `seq` of file `name`'s checkpoint."""
    from . import _core
    return _core.ckpt_object(name, seq)


def parse_ckpt_object(obj):
    """(name, seq) for a checkpoint object's name, else None. A
    checkpoint named without a version (x.parquet.ckpt, as written
    before versions) is version 0. The rule is the C core's; a
    directory in front of the name stays in front."""
    from . import _core
    head, slash, tail = obj.rpartition('/')
    got = _core.ckpt_object_parse(tail)
    if got is None:
        return None
    return head + slash + got[0], got[1]


def ckpt_versions(sink, name):
    """[(seq, object)] of file `name`'s checkpoint versions on `sink`,
    oldest first."""
    out = []
    for obj in sink.list(name + '.'):
        got = parse_ckpt_object(obj)
        if got is not None and got[0] == name:
            out.append((got[1], obj))
    return sorted(out)


def unfinished_files(sink, prefix=''):
    """{name: [(seq, object)]} for every file under `prefix` that has
    checkpoint versions on `sink`."""
    out = {}
    for obj in sink.list(prefix):
        got = parse_ckpt_object(obj)
        if got is not None:
            out.setdefault(got[0], []).append((got[1], obj))
    return {n: sorted(v) for n, v in out.items()}


class CheckpointWriter:
    """Write one crash-durable parquet file through a sink.

    w = CheckpointWriter('run.parquet', schema, sink=DirSink(d))
    w.write_table(t)          # final row groups; implies checkpoint()
    w.checkpoint(tail)        # durability point; `tail` = unwritten
                              #   rows (a Table), provisional
    w.close() -> 'run.parquet'

    The format work is the C core's (src/core/pqc.c through _core, the same
    core the Go host and the DuckDB extension use): this class encodes
    rows as parquet with pyarrow, hands the core the bytes, and stores
    what the core hands back. One owner at a time, like a file handle.
    """

    def __init__(self, name, schema, sink=None, chunk_bytes=5 * 1024 * 1024,
                 checkpoint_every_write=True,
                 superseded_keep_s=SUPERSEDED_KEEP_S, **writer_options):
        if not name.endswith('.parquet'):
            raise CheckpointError(f"name must end in .parquet: {name!r}")
        if sink is None:
            import os
            sink = DirSink(os.path.dirname(name) or '.')
            name = os.path.basename(name)
        self.name = name
        self.chunk_prefix = name + '.chunks/'
        self.sink = sink
        part_min = getattr(sink, 'part_min', 0)
        if chunk_bytes < part_min:
            raise CheckpointError(
                f"chunk_bytes {chunk_bytes} is below the sink's part "
                f"minimum {part_min}: finalize would be impossible")
        if sink.exists(name):
            raise CheckpointError(f"{name} already exists")
        left = ckpt_versions(sink, name)
        if left:
            raise CheckpointError(
                f"{left[-1][1]} exists -- a previous writer crashed or "
                f"is live; finalize() it first")
        self.schema = schema
        writer_options.setdefault('write_page_checksum', True)
        self._opts = writer_options
        self._auto = checkpoint_every_write
        from . import _core
        self._core_error = _core.CoreError
        try:
            self._w = _core.Writer(name, chunk_bytes)
        except _core.CoreError as e:
            raise CheckpointError(f"{name}: {e}") from None
        self._rows_checkpointed = 0
        self._seq = 0                # the newest checkpoint version's number
        self._keep_s = superseded_keep_s
        # the versions stored, and which are due for deletion
        self._versions = _versions(superseded_keep_s)
        self._ckpt_name = None      # the newest version's object name
        # what the newest version claims: (final rows, unfinished rows).
        # The same count of rows is not the same claim: a final row
        # group that replaces the unfinished objects is a new one.
        self._claimed = None
        self._closed = False

    # ---- write -----------------------------------------------------------

    def write_table(self, table):
        """Append final row group(s): `table` holds every row since the
        last write_table, and replaces what checkpoint(tail) stored of
        them. Implies checkpoint() by default."""
        if self._closed:
            raise CheckpointError("writer is closed")
        if table.num_rows:
            try:
                self._w.add_group(_encode_mini(table, schema=self.schema,
                                               **self._opts))
            except self._core_error as e:
                raise CheckpointError(f"{self.name}: {e}") from None
            self._store_objects()
        if self._auto:
            self.checkpoint()

    def _store_objects(self):
        """Store every object the core owes: durable BEFORE any claim.
        A put that fails leaves the object with the core, which gives
        it again at the next call."""
        while True:
            obj = self._w.next_object()
            if obj is None:
                return
            self.sink.put(*obj)
            self._w.object_stored()

    def checkpoint(self, tail=None):
        """Publish a durability claim covering all final rows plus the
        optional `tail`: a Table of EVERY row since the last
        write_table, in order (provisional: a later write_table of the
        same rows replaces them). A later tail begins with the rows of
        the one before. Only the rows no object holds yet are encoded
        and stored, or a merge the core asks for: the cost follows the
        new rows, not the tail. No tail: the claim covers the final
        rows only."""
        if self._closed:
            raise CheckpointError("writer is closed")
        w = self._w
        try:
            self._store_objects()       # one a failed put left with the core
            tail_rows = tail.num_rows if tail is not None else 0
            if tail_rows == 0:
                w.drop_unfinished()
                if self._claimed == (w.rows_final, 0):
                    return self._rows_checkpointed    # nothing new to claim
            else:
                have = w.rows_unfinished
                if tail_rows < have:
                    raise CheckpointError(
                        f"{self.name}: checkpoint(tail) with {tail_rows} "
                        f"rows after one with {have}: a tail holds every "
                        f"row since the last write_table, so it never "
                        f"gets shorter")
                if self._claimed == (w.rows_final, tail_rows):
                    return self._rows_checkpointed    # nothing new to claim
                first, rows = w.next_span(tail_rows - have)
                span = tail.slice(first - w.rows_final, rows)
                w.add_unfinished(_encode_mini(span, schema=self.schema,
                                              **self._opts))
                self._store_objects()
            empty = None
            if w.rows_final == 0 and w.rows_unfinished == 0:
                # zero rows anywhere: the data region is a complete,
                # valid, empty parquet file so finalize always yields a
                # readable file
                buf = io.BytesIO()
                pq.ParquetWriter(buf, self.schema, **self._opts).close()
                empty = buf.getvalue()
            ckpt, total_rows = w.build_checkpoint(empty)
        except self._core_error as e:
            raise CheckpointError(f"{self.name}: {e}") from None
        # A number is used once, also when its put fails: a put that
        # reported failure may have stored the object, and a version is
        # never written twice.
        self._seq += 1
        obj = ckpt_object(self.name, self._seq)
        self.sink.put(obj, ckpt)
        _stored_version(self._versions, self.sink, obj, ckpt)  # stored whole first
        self._ckpt_name = obj
        self._rows_checkpointed = total_rows
        self._claimed = (w.rows_final, w.rows_unfinished)
        return total_rows

    @property
    def ckpt_name(self):
        """The newest checkpoint version's object name; None before the
        first checkpoint."""
        return self._ckpt_name

    # ---- lifecycle ---------------------------------------------------------

    def close(self, keep_checkpoints=False):
        """Final checkpoint, assemble the ordinary file, clean up
        (`keep_checkpoints`: leave the chunks and versions;
        discard_checkpoints() later)."""
        if self._closed:
            return self.name
        self.checkpoint()
        if self._w.rows_final == 0:
            # zero final rows: write an ordinary (possibly empty) file
            buf = io.BytesIO()
            w = pq.ParquetWriter(buf, self.schema, **self._opts)
            w.close()
            self.sink.put(self.name, buf.getvalue())
            if not keep_checkpoints:
                for _seq, version in ckpt_versions(self.sink, self.name):
                    self.sink.delete(version)
                self._ckpt_name = None
        else:
            finalize(self.name, self.sink, keep_checkpoints=keep_checkpoints)
        self._closed = True
        self._rows_final = self._w.rows_final
        self._bytes_final = self._w.logical_end
        self._w.free()
        return self.name

    @property
    def rows_written(self):
        return self._rows_final if self._closed else self._w.rows_final

    @property
    def bytes_written(self):
        """The bytes of the file's final row groups so far: its size
        without the footer."""
        return self._bytes_final if self._closed else self._w.logical_end

    @property
    def rows_checkpointed(self):
        return self._rows_checkpointed

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()


class CheckpointReader:
    """Parse a checkpoint; query the live file as ordinary parquet."""

    def __init__(self, ckpt_bytes_or_name, sink=None, ckpt_name=None):
        if isinstance(ckpt_bytes_or_name, (bytes, bytearray)):
            raw = bytes(ckpt_bytes_or_name)
            # the object's name on `sink`, when the bytes came from there
            # (finalize assembles the final file from it)
            self._ckpt_name = ckpt_name
        else:
            self._ckpt_name = ckpt_bytes_or_name
            raw = sink.get(ckpt_bytes_or_name)
        self.sink = sink
        if raw[:4] != CKPT_MAGIC:
            raise CheckpointError(f"bad checkpoint magic {raw[:4]!r}")
        if raw[4] != CKPT_VERSION:
            raise CheckpointError(f"checkpoint version {raw[4]}; this "
                                  f"library speaks {CKPT_VERSION}")
        want = struct.unpack('<I', raw[5:9])[0]
        body = raw[9:]
        if zlib.crc32(body) != want:
            raise CheckpointError("checkpoint CRC mismatch -- corrupt, "
                                  "refusing")
        o = 0
        nlen = struct.unpack('<H', body[o:o + 2])[0]; o += 2
        self.name = body[o:o + nlen].decode('utf-8'); o += nlen
        self.watermark = struct.unpack('<Q', body[o:o + 8])[0]; o += 8
        self.data_offset = struct.unpack('<Q', body[o:o + 8])[0]; o += 8
        n_chunks = struct.unpack('<I', body[o:o + 4])[0]; o += 4
        self.manifest = []
        for _ in range(n_chunks):
            clen = struct.unpack('<H', body[o:o + 2])[0]; o += 2
            cname = body[o:o + clen].decode('utf-8'); o += clen
            length, crc = struct.unpack('<QI', body[o:o + 12]); o += 12
            self.manifest.append((cname, length, crc))
        self._data = body[o:]
        self.logical_size = sum(m[1] for m in self.manifest) + len(self._data)

    # -- the virtual file --

    def _compose(self):
        parts = []
        for cname, length, crc in self.manifest:
            b = self.sink.get(cname)
            if len(b) != length or zlib.crc32(b) != crc:
                raise CheckpointError(
                    f"{cname}: length/CRC mismatch vs manifest -- corrupt")
            parts.append(b)
        parts.append(self._data)
        return b''.join(parts)

    def table(self, **read_kwargs):
        return pq.read_table(pa.BufferReader(self._compose()), **read_kwargs)

    def open(self):
        """The live file as a FILE OBJECT (read/seek/tell): the
        pluggable-storage face. Hand it to anything that reads
        parquet from a file — pyarrow.parquet.read_table(r.open()) —
        and stock parquet code reads the live checkpoint, fetching
        only the byte ranges it touches (column pruning reaches the
        storage). The footer version is pinned at THIS reader's
        construction; open a fresh reader for a newer state."""
        return CheckpointFile(self)

    @property
    def metadata(self):
        # footer-only read: the virtual file serves it from the
        # in-memory tail — no chunk fetches
        return pq.read_metadata(self.open())

    @property
    def num_rows(self):
        return self.watermark

    def verify(self):
        """O(data): every chunk CRC + every parquet page CRC."""
        f = pq.ParquetFile(pa.BufferReader(self._compose()),
                           page_checksum_verification=True)
        return f.read().num_rows

    def assembly_plan(self):
        """[(name, offset, length), ...] for Sink.assemble: the chunks
        whole, then the checkpoint object's own data region by range."""
        plan = [(m[0], -1, -1) for m in self.manifest]
        abs_off = 9 + self.data_offset - (len(CKPT_MAGIC) + 1 + 4)
        plan.append((self._ckpt_name, abs_off, len(self._data)))
        return plan


class CheckpointFile:
    """A checkpoint state as a read-only seekable file.

    The file's bytes are: each object of the manifest in order, then
    the checkpoint's own data region (the footer). Reads map offsets
    onto those segments. An object holds WHOLE row groups, so its size
    tracks what it holds: a chunk, one row group, or the rows of a few
    checkpoints. Two fetch strategies:

      small object (<= WHOLE_FETCH_MAX): fetched whole on first
        touch, manifest CRC verified (corrupt object fails loudly),
        kept while the kept ones fit in CACHE_BYTES.
      large object: RANGED reads of exactly the requested bytes —
        this is what lets a columnar reader's pruning reach the
        storage (a 128 MB row group's chunk is never fetched whole
        to read one column). No manifest-CRC check on this path;
        parquet page checksums are the integrity guard
        (verify() / page_checksum_verification remain the
        CRC-checked reads).

    The tail segment is served from memory.
    """

    WHOLE_FETCH_MAX = 8 << 20  # ranged reads above this object size
    CACHE_BYTES = 4 * WHOLE_FETCH_MAX   # the whole objects kept, bounded

    def __init__(self, reader):
        self._r = reader
        self._pos = 0
        self._closed = False
        self._lock = threading.Lock()
        self._cache = {}       # cname -> bytes, the oldest first
        self._cached = 0       # their bytes
        # segment table: (start, end, cname or None=tail)
        self._segments = []
        off = 0
        for cname, length, _crc in reader.manifest:
            self._segments.append((off, off + length, cname))
            off += length
        self._segments.append((off, off + len(reader._data), None))
        self._size = reader.logical_size

    # -- file protocol ----------------------------------------------------

    def readable(self):
        return True

    def seekable(self):
        return True

    def writable(self):
        return False

    @property
    def closed(self):
        return self._closed

    def close(self):
        self._closed = True
        self._cache.clear()

    def tell(self):
        return self._pos

    def seek(self, offset, whence=0):
        if whence == 0:
            pos = offset
        elif whence == 1:
            pos = self._pos + offset
        elif whence == 2:
            pos = self._size + offset
        else:
            raise ValueError(f"seek: bad whence {whence}")
        if pos < 0:
            raise ValueError(f"seek: negative position {pos}")
        self._pos = pos
        return pos

    def read(self, n=-1):
        if self._closed:
            raise ValueError("read on closed CheckpointFile")
        if n < 0:
            n = self._size - self._pos
        n = min(n, max(0, self._size - self._pos))
        if n == 0:
            return b''
        out = self._read_at(self._pos, n)
        self._pos += len(out)
        return out

    def size(self):
        return self._size

    # -- segment mapping --------------------------------------------------

    def _chunk_bytes(self, cname, length, crc):
        with self._lock:
            b = self._cache.get(cname)
            if b is None:
                b = self._r.sink.get(cname)
                if len(b) != length or zlib.crc32(b) != crc:
                    raise CheckpointError(
                        f"{cname}: length/CRC mismatch vs manifest -- "
                        f"corrupt")
                self._cache[cname] = b
                self._cached += len(b)
                while self._cached > self.CACHE_BYTES and len(self._cache) > 1:
                    self._cached -= len(self._cache.pop(next(iter(self._cache))))
            return b

    def _read_at(self, pos, n):
        out = []
        want_end = pos + n
        for i, (start, end, cname) in enumerate(self._segments):
            if end <= pos or start >= want_end:
                continue
            lo = max(pos, start) - start
            hi = min(want_end, end) - start
            if cname is None:
                out.append(self._r._data[lo:hi])
            else:
                _n, length, crc = self._r.manifest[i]
                if length <= self.WHOLE_FETCH_MAX:
                    out.append(self._chunk_bytes(cname, length, crc)[lo:hi])
                else:
                    out.append(self._r.sink.get(cname, lo, hi - lo))
        return b''.join(out)


def live_file(path):
    """The live file at `path` as a read-only file object, whether it
    is still a checkpoint or already finalized. The file-object twin
    of live_table()."""
    r, sink, name = _reader_for(path)
    if r is None:
        return open(path, 'rb')
    return r.open()


# ---- finalize (= repair = close's last act = S3 assembly) --------------------

def finalize(name, sink, ckpt_bytes=None, ckpt_object=None,
             keep_checkpoints=False):
    """Turn a checkpoint into the ordinary .parquet. Idempotent; the
    checkpoint versions are deleted last. Runnable by anyone, anywhere,
    anytime: it assembles from the newest version whose CRC holds.
    `ckpt_bytes`, `ckpt_object`: the newest version's bytes and object
    name when the caller just put them there (a Shipper); the read-back
    is then skipped. `keep_checkpoints`: leave the chunks and versions
    for readers that were told of them; discard_checkpoints() later.

    The rules are the C core's (src/core/pqc.h, THE ASSEMBLY OF A
    FILE): which version, the checks, the sources and the order of the
    deletes. This does each step the core asks, on `sink`."""
    given = None if ckpt_bytes is None else (ckpt_object, bytes(ckpt_bytes))
    assembled = _finish(name, sink, _core.FINISH_ASSEMBLE_FILE,
                        keep_checkpoints, given)
    return ('recovered' if assembled else 'done', name)


def _finish(name, sink, mode, keep_checkpoints=False, given=None):
    """The steps of one PqcFinish on `sink`. True when the file was
    assembled now. CheckpointError for what the core refuses; a failure
    of the sink is raised as it is: what was done stays done."""
    import logging as _logging
    import time as _time
    log = _logging.getLogger('parquet_checkpoint')
    spent = {'verify': 0.0, 'assemble': 0.0, 'cleanup': 0.0}
    chunks = 0
    try:
        f = _core.Finish(name, mode, keep_checkpoints)
    except _core.CoreError as e:
        raise CheckpointError(str(e)) from None
    try:
        for _seq, obj in ckpt_versions(sink, name):
            f.version(obj)
        if given is not None:
            f.given(*given)
        while True:
            action, obj, size, flag = f.next()
            if action == _core.FINISH_DONE:
                break
            t0 = _time.monotonic()
            if action == _core.FINISH_STAT:
                exists = sink.exists(obj)
                if flag:                # the finished file
                    f.has(exists, 0)
                    continue
                chunks += 1
                try:
                    f.has(exists, sink.size(obj) if exists else 0)
                finally:
                    spent['verify'] += _time.monotonic() - t0
                continue
            if action == _core.FINISH_READ:
                for level, text in f.read(sink.get(obj)):
                    log.log(_logging.WARNING if level >= 2 else _logging.INFO,
                            text)
                continue
            if action == _core.FINISH_ASSEMBLE:
                sink.assemble(obj, f.sources(size))
                spent['assemble'] += _time.monotonic() - t0
            elif action == _core.FINISH_LIST:
                for cname in sink.list(obj):
                    f.listed(cname)
                spent['cleanup'] += _time.monotonic() - t0
            elif action == _core.FINISH_DELETE:
                sink.delete(obj)
                spent['cleanup'] += _time.monotonic() - t0
            f.did()
    except _core.CoreError as e:
        raise CheckpointError(str(e)) from None
    finally:
        f.free()
    total = sum(spent.values())
    if flag and (total >= FINALIZE_SLOW_S or getattr(sink, 'trace', False)):
        log.info(
            f"finalize {name} on {getattr(sink, 'uri', type(sink).__name__)}: "
            f"{total:.3f}s (verify {spent['verify']:.3f}s, {chunks} chunk(s); "
            f"assemble {spent['assemble']:.3f}s; cleanup "
            f"{spent['cleanup']:.3f}s)"
            f"{' SLOW' if total >= FINALIZE_SLOW_S else ''}")
    return bool(flag)


def discard_checkpoints(name, sink):
    """Delete a finished file's chunks and checkpoint versions, kept by
    finalize(keep_checkpoints=True) while readers were told of them.
    Refuses unless the final file exists. The order of the deletes is
    the C core's, the same as finalize's."""
    _finish(name, sink, _core.FINISH_DISCARD)


#: A finalize at or over this many seconds logs its phase breakdown.
FINALIZE_SLOW_S = 1.0


def dead_after(beat_seconds=0.0):
    """Seconds of silence after which a writer is dead: three of its
    beats, and never sooner than 30 seconds. The one rule for a dead
    writer (pqc_dead_after in the C core, shared with the Go host): a
    ticket's reader, a writer that takes a stream over, and the sweep
    below all use it. beat_seconds: how often the writer shows that it
    lives; 0 when it does not say."""
    from . import _core
    return _core.dead_after(beat_seconds)


def recover_directory(sink_or_dir, min_age_s=None):
    """The boot sweep: finalize every checkpoint under the sink —
    EXCEPT ones modified within min_age_s, whose writer is presumed
    alive; those come back as ('alive', name), untouched. min_age_s
    None: the dead-writer rule for a writer whose beat is not known
    (dead_after(), 30 seconds). Pass min_age_s=0 only when you know no
    writer is running."""
    import time
    if min_age_s is None:
        min_age_s = dead_after()
    sink = (DirSink(sink_or_dir) if isinstance(sink_or_dir, str)
            else sink_or_dir)
    results = []
    for name, versions in sorted(unfinished_files(sink).items()):
        if min_age_s > 0:
            age = time.time() - sink.mtime(versions[-1][1])
            if age < min_age_s:
                results.append(('alive', name))
                continue
        results.append(finalize(name, sink))
    return results


# ---- conveniences -------------------------------------------------------------

def _reader_for(path):
    import os
    sink = DirSink(os.path.dirname(path) or '.')
    name = os.path.basename(path)
    if sink.exists(name):
        return None, sink, name                 # already an ordinary file
    versions = ckpt_versions(sink, name)
    if not versions:
        raise CheckpointError(f"{path}: neither the file nor a checkpoint")
    return CheckpointReader(versions[-1][1], sink), sink, name


def live_table(path, **kw):
    r, sink, name = _reader_for(path)
    if r is None:
        return pq.read_table(path, **kw)
    return r.table(**kw)


def live_metadata(path):
    r, sink, name = _reader_for(path)
    if r is None:
        return pq.read_metadata(path)
    return r.metadata


def verify(path):
    r, sink, name = _reader_for(path)
    if r is None:
        return pq.ParquetFile(
            path, page_checksum_verification=True).read().num_rows
    return r.verify()


from ._shipper import Shipper  # noqa: E402  (needs the names above)
