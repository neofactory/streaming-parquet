"""The Python writer as it was before the writer moved onto the C core
(2026-11-09): a test oracle, not part of the package's work.

run_c_compat.py writes the same rows through this writer and through
the package's CheckpointWriter (a host over core/pqc.c) and compares
every object byte for byte. This is the proof that the move changed no
file. It encodes the footer with pyarrow and rebases offsets with
_thrift_shift, a second implementation of what the core does, kept only
so that the core has something to be checked against. It goes when the
writer's rules change (the core then has no equal to compare with).
"""

import io
import struct
import zlib

import pyarrow as pa
import pyarrow.parquet as pq

from . import (CKPT_MAGIC, CKPT_VERSION, MAGIC, SUPERSEDED_KEEP_S,
               CheckpointError, DirSink, _supersede, ckpt_object,
               ckpt_versions, finalize)
from ._thrift_shift import shift_footer_offsets


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


class ReferenceWriter:
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
