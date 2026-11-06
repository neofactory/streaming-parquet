"""parquet_checkpoint -- crash-durable, queryable-while-open parquet.

Three things, each usable alone:

  CheckpointWriter   write a parquet file as chunks + a checkpoint
  CheckpointReader   query a live or crashed file as ordinary parquet
  finalize           turn a checkpoint into an ordinary .parquet file

Artifacts (relative names, resolved by a sink -- local dir or S3):

  run.parquet.chunks/000001   immutable pieces of the final file, cut
      at row-group boundaries, >= chunk_bytes (5 MB: the S3 multipart
      part minimum -- a packaging size, never a durability boundary).
  run.parquet.000017.ckpt     a checkpoint: version 17 of the file's
      checkpoint, one object, written once and never changed. Each
      checkpoint is a new version; a version superseded for
      SUPERSEDED_KEEP_S (10 minutes) is deleted in the step that uploads
      a later one, so a reader told of an older version less than that
      long ago still finds it, and an object a reader has opened never
      changes under it. Layout:
        header    magic PQC2, version, CRC-32 of everything after the
                  CRC field, final file name, watermark (rows covered),
                  data offset
        manifest  ordered chunk list: name, byte length, CRC-32
        data      [magic if no chunks][buffered final row groups]
                  [provisional tail row group][footer thrift+len+magic]
      The data region IS final-file bytes: finalize appends it verbatim
      after the chunks (on S3, a server-side range copy).
  run.parquet                 the finished ordinary parquet file.

Durability ordering: chunks are stored before any checkpoint that
references them; a checkpoint version is stored whole before older
versions are deleted. Crash loss = rows since the last checkpoint (zero
at any checkpoint instant: the tail rides inside it). Recovery uses the
newest version whose CRC holds. write_table implies a checkpoint by
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

from ._sinks import DirSink, FakeS3, S3Sink, SubSink, SinkError  # noqa: F401
from ._thrift_shift import shift_footer_offsets

__all__ = ['CheckpointWriter', 'CheckpointReader', 'finalize',
           'recover_directory', 'live_table', 'live_metadata', 'verify',
           'CheckpointError', 'DirSink', 'FakeS3', 'S3Sink', 'SubSink', 'Shipper']

MAGIC = b'PAR1'
CKPT_MAGIC = b'PQC2'
CKPT_VERSION = 2


class CheckpointError(Exception):
    """Loud failure: inconsistent checkpoint state (never guessed away)."""


def _encode_group(table, logical_pos, schema=None, **writer_options):
    """Encode `table` as row group(s) positioned at `logical_pos` in the
    final file. `schema` overrides the table's own (the writer's schema
    carries file-level KV metadata, e.g. sp_base_seq, that region
    slices from a ring do not). Returns (page_bytes, rebased_thrift,
    n_rows)."""
    buf = io.BytesIO()
    schema = schema if schema is not None else table.schema
    if table.schema.metadata != schema.metadata:
        table = table.replace_schema_metadata(schema.metadata)
    w = pq.ParquetWriter(buf, schema, **writer_options)
    w.write_table(table)
    w.close()
    m = buf.getvalue()
    flen = struct.unpack('<I', m[-8:-4])[0]
    pages = m[4:len(m) - 8 - flen]
    thrift = m[len(m) - 8 - flen:len(m) - 8]
    rebased = shift_footer_offsets(thrift, logical_pos - 4)
    return pages, rebased, table.num_rows


def _md_of(thrift):
    """Fresh FileMetaData parsed from rebased thrift bytes."""
    b = MAGIC + thrift + struct.pack('<I', len(thrift)) + MAGIC
    return pq.read_metadata(pa.BufferReader(b))


def _combine(thrift_list):
    """One FileMetaData covering every group, freshly parsed (no shared
    mutable state). Returns None for an empty list."""
    md = None
    for t in thrift_list:
        m = _md_of(t)
        if md is None:
            md = m
        else:
            md.append_row_groups(m)
    return md


def _serialize_footer(md):
    """[thrift][len][PAR1] for a FileMetaData."""
    import os
    import tempfile
    fd, path = tempfile.mkstemp(suffix='.md')
    os.close(fd)
    try:
        md.write_metadata_file(path)
        return open(path, 'rb').read()[len(MAGIC):]
    finally:
        os.unlink(path)


# ---- checkpoint versions --------------------------------------------------------

#: A checkpoint version superseded at least this long is deleted, in the
#: step that uploads a later one (nothing runs on a timer): a reader told
#: of a version less than this long ago still finds it. One rule for a
#: live file's versions (superseded by the next one) and a finished
#: file's checkpoints (superseded when the ticket drops the file).
SUPERSEDED_KEEP_S = 600.0

_CKPT_RE = re.compile(r'^(?P<name>.+\.parquet)(?:\.(?P<seq>\d{6,}))?\.ckpt$')


def _supersede(sink, versions, new, keep_s):
    """`new` (just stored) supersedes the newest of `versions`; delete
    every version superseded at least keep_s ago. Returns the list."""
    now = time.monotonic()
    if versions:
        versions[-1][1] = now
    kept = []
    for obj, at in versions:
        if at is not None and now - at >= keep_s:
            sink.delete(obj)
        else:
            kept.append([obj, at])
    kept.append([new, None])
    return kept


def ckpt_object(name, seq):
    """The object name of version `seq` of file `name`'s checkpoint."""
    return f"{name}.{seq:06d}.ckpt"


def parse_ckpt_object(obj):
    """(name, seq) for a checkpoint object's name, else None. A
    checkpoint named without a version (x.parquet.ckpt, as written
    before versions) is version 0."""
    m = _CKPT_RE.match(obj)
    if m is None:
        return None
    return m.group('name'), int(m.group('seq') or 0)


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
        self._chunk_bytes = chunk_bytes
        self._auto = checkpoint_every_write
        self._buffer = []            # (pages, thrift, n_rows) not yet chunked
        self._buffer_bytes = 0
        self._chunked_thrift = []    # thrift of every chunked group, in order
        self._manifest = []          # (chunk_name, length, crc32)
        self._logical_end = 4        # after the leading magic
        self._rows_final = 0
        self._rows_checkpointed = 0
        self._n_chunks = 0
        self._seq = 0                # the newest checkpoint version's number
        self._keep_s = superseded_keep_s
        # [object, when superseded (monotonic) or None], oldest first
        self._versions = []
        self._closed = False

    # ---- write -----------------------------------------------------------

    def write_table(self, table):
        """Append final row group(s). Implies checkpoint() by default."""
        if self._closed:
            raise CheckpointError("writer is closed")
        if table.num_rows:
            pages, thrift, n = _encode_group(table, self._logical_end,
                                             schema=self.schema,
                                             **self._opts)
            self._buffer.append((pages, thrift, n))
            self._buffer_bytes += len(pages)
            self._logical_end += len(pages)
            self._rows_final += n
            self._maybe_cut_chunks()
        if self._auto:
            self.checkpoint()

    def _maybe_cut_chunks(self):
        while self._buffer_bytes >= self._chunk_bytes:
            take, size = 0, 0
            while size < self._chunk_bytes and take < len(self._buffer):
                size += len(self._buffer[take][0])
                take += 1
            groups = self._buffer[:take]
            body = b''.join(g[0] for g in groups)
            if self._n_chunks == 0:
                body = MAGIC + body
            cname = f"{self.chunk_prefix}{self._n_chunks + 1:06d}"
            self.sink.put(cname, body)          # durable BEFORE any claim
            self._manifest.append((cname, len(body), zlib.crc32(body)))
            self._chunked_thrift += [g[1] for g in groups]
            self._n_chunks += 1
            del self._buffer[:take]
            self._buffer_bytes -= sum(len(g[0]) for g in groups)

    def checkpoint(self, tail=None):
        """Publish a durability claim covering all final rows plus the
        optional `tail` (a Table of rows not written anywhere yet --
        provisional: superseded by a later write_table)."""
        if self._closed:
            raise CheckpointError("writer is closed")
        prov = None
        if tail is not None and tail.num_rows:
            prov = _encode_group(tail, self._logical_end,
                                 schema=self.schema, **self._opts)
        total_rows = self._rows_final + (prov[2] if prov else 0)
        if (prov is None and total_rows == self._rows_checkpointed
                and self._versions):
            return self._rows_checkpointed        # nothing new to claim
        thrifts = (self._chunked_thrift
                   + [g[1] for g in self._buffer]
                   + ([prov[1]] if prov else []))
        md = _combine(thrifts)
        if md is None:
            # zero rows anywhere: the data region is a complete, valid,
            # empty parquet file so finalize always yields a readable file
            buf = io.BytesIO()
            pq.ParquetWriter(buf, self.schema, **self._opts).close()
            data = bytearray(buf.getvalue())
        else:
            data = bytearray()
            if self._n_chunks == 0:
                data += MAGIC
            for g in self._buffer:
                data += g[0]
            if prov:
                data += prov[0]
            data += _serialize_footer(md)
        self._seq += 1
        obj = ckpt_object(self.name, self._seq)
        self.sink.put(obj, _build_ckpt(self.name, total_rows, self._manifest,
                                       bytes(data)))
        self._versions = _supersede(self.sink, self._versions, obj,
                                    self._keep_s)   # stored whole first
        self._rows_checkpointed = total_rows
        return total_rows

    @property
    def ckpt_name(self):
        """The newest checkpoint version's object name; None before the
        first checkpoint."""
        return self._versions[-1][0] if self._versions else None

    # ---- lifecycle ---------------------------------------------------------

    def close(self, keep_checkpoints=False):
        """Final checkpoint, assemble the ordinary file, clean up
        (`keep_checkpoints`: leave the chunks and versions;
        discard_checkpoints() later)."""
        if self._closed:
            return self.name
        self.checkpoint()
        if self._rows_final == 0 and self._n_chunks == 0:
            # zero final rows: write an ordinary (possibly empty) file
            buf = io.BytesIO()
            w = pq.ParquetWriter(buf, self.schema, **self._opts)
            w.close()
            self.sink.put(self.name, buf.getvalue())
            if not keep_checkpoints:
                for obj, _at in self._versions:
                    self.sink.delete(obj)
                self._versions = []
        else:
            finalize(self.name, self.sink, keep_checkpoints=keep_checkpoints)
        self._closed = True
        return self.name

    @property
    def rows_written(self):
        return self._rows_final

    @property
    def rows_checkpointed(self):
        return self._rows_checkpointed

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()


# ---- checkpoint object encoding ---------------------------------------------

def _build_ckpt(name, watermark, manifest, data):
    nm = name.encode('utf-8')
    man = bytearray(struct.pack('<I', len(manifest)))
    for cname, length, crc in manifest:
        cb = cname.encode('utf-8')
        man += struct.pack('<H', len(cb)) + cb
        man += struct.pack('<QI', length, crc)
    # layout after the CRC field:
    #   [u16 name-len][name][u64 watermark][u64 data_offset][manifest][data]
    head = struct.pack('<H', len(nm)) + nm + struct.pack('<Q', watermark)
    data_offset = (len(CKPT_MAGIC) + 1 + 4      # magic, version, crc
                   + len(head) + 8 + len(man))  # head, data_offset, manifest
    body = head + struct.pack('<Q', data_offset) + man + data
    return (CKPT_MAGIC + bytes([CKPT_VERSION])
            + struct.pack('<I', zlib.crc32(body)) + body)


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

    The file's bytes are: each manifest chunk in order, then the
    checkpoint's own data region (provisional tail + footer). Reads
    map offsets onto those segments. A chunk holds WHOLE row groups,
    so its size tracks the row-group size; two fetch strategies:

      small chunk (<= WHOLE_FETCH_MAX): fetched whole on first
        touch, manifest CRC verified (corrupt chunk fails loudly),
        kept in a small LRU.
      large chunk: RANGED reads of exactly the requested bytes —
        this is what lets a columnar reader's pruning reach the
        storage (a 128 MB row group's chunk is never fetched whole
        to read one column). No manifest-CRC check on this path;
        parquet page checksums are the integrity guard
        (verify() / page_checksum_verification remain the
        CRC-checked reads).

    The tail segment is served from memory.
    """

    _CACHE_CHUNKS = 4          # ~4 x chunk size of memory, bounded
    WHOLE_FETCH_MAX = 8 << 20  # ranged reads above this chunk size

    def __init__(self, reader):
        self._r = reader
        self._pos = 0
        self._closed = False
        self._lock = threading.Lock()
        self._cache = {}       # cname -> bytes (LRU by insertion order)
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
                while len(self._cache) > self._CACHE_CHUNKS:
                    self._cache.pop(next(iter(self._cache)))
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
    for readers that were told of them; discard_checkpoints() later."""
    import logging as _logging
    versions = ckpt_versions(sink, name)
    if ckpt_bytes is None:
        if not versions:
            if sink.exists(name):
                return ('done', name)
            raise CheckpointError(f"finalize({name!r}): nothing there")
        if sink.exists(name):
            raise CheckpointError(
                f"{name}: BOTH final and checkpoint exist -- refusing to guess")
        r, refused = None, []
        for _seq, obj in reversed(versions):
            try:
                r = CheckpointReader(obj, sink)
                break
            except CheckpointError as e:
                refused.append(f"{obj}: {e}")
        if r is None:
            raise CheckpointError(
                f"finalize({name!r}): no checkpoint version holds -- "
                + "; ".join(refused))
        if refused:
            _logging.getLogger('parquet_checkpoint').warning(
                f"finalize {name}: newer version(s) refused, assembling "
                f"from {r._ckpt_name}: " + "; ".join(refused))
    else:
        r = CheckpointReader(bytes(ckpt_bytes), sink, ckpt_name=ckpt_object)
    used = r._ckpt_name
    if r.name != name.split('/')[-1]:
        raise CheckpointError(
            f"{used}: checkpoint names {r.name!r}, paired with "
            f"{name!r} -- mispaired, refusing")
    import time as _time
    t0 = _time.monotonic()
    for cname, length, crc in r.manifest:
        if not sink.exists(cname) or sink.size(cname) != length:
            raise CheckpointError(f"{cname}: missing or wrong size -- "
                                  f"cannot finalize")
    t_verify = _time.monotonic() - t0
    t0 = _time.monotonic()
    sink.assemble(name, r.assembly_plan())
    t_assemble = _time.monotonic() - t0
    # Delete every chunk under the prefix, including any newer chunk a
    # crash left unreferenced by this checkpoint (dead bytes).
    t0 = _time.monotonic()
    if not keep_checkpoints:
        _delete_checkpoints(name, sink, versions, used)
    t_cleanup = _time.monotonic() - t0
    total = t_verify + t_assemble + t_cleanup
    if total >= FINALIZE_SLOW_S or getattr(sink, 'trace', False):
        _logging.getLogger('parquet_checkpoint').info(
            f"finalize {name} on {getattr(sink, 'uri', type(sink).__name__)}: "
            f"{total:.3f}s (verify {t_verify:.3f}s, {len(r.manifest)} chunk(s); "
            f"assemble {t_assemble:.3f}s; cleanup {t_cleanup:.3f}s)"
            f"{' SLOW' if total >= FINALIZE_SLOW_S else ''}")
    return ('recovered', name)


def _delete_checkpoints(name, sink, versions, last):
    for cname in sink.list(name + '.chunks/'):
        sink.delete(cname)
    for _seq, obj in versions:
        if obj != last:
            sink.delete(obj)
    if last is not None:
        sink.delete(last)                # last act


def discard_checkpoints(name, sink):
    """Delete a finished file's chunks and checkpoint versions, kept by
    finalize(keep_checkpoints=True) while readers were told of them.
    Refuses unless the final file exists."""
    if not sink.exists(name):
        raise CheckpointError(
            f"discard_checkpoints({name!r}): the final file does not "
            f"exist -- the checkpoints are its only copy")
    versions = ckpt_versions(sink, name)
    _delete_checkpoints(name, sink, versions,
                        versions[-1][1] if versions else None)


#: A finalize at or over this many seconds logs its phase breakdown.
FINALIZE_SLOW_S = 1.0


#: A checkpoint younger than this is presumed to have a LIVE writer
#: (writers rewrite it on their checkpoint cadence); sweeping it would
#: finalize under a running process. Sized well above any sane cadence.
LIVE_SWEEP_THRESHOLD_S = 30.0


def recover_directory(sink_or_dir, min_age_s=LIVE_SWEEP_THRESHOLD_S):
    """The boot sweep: finalize every checkpoint under the sink —
    EXCEPT ones modified within min_age_s, whose writer is presumed
    alive; those come back as ('alive', name), untouched. Pass
    min_age_s=0 only when you know no writer is running."""
    import time
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
