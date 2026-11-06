"""Thrift Compact Protocol round-tripper for parquet footers, with one
transform: shift every file-offset field by a constant delta.

Why this exists: parquet-checkpoint writes each segment through a stock
pyarrow ParquetWriter whose sink position starts at 0, so the segment's
footer carries offsets relative to the SEGMENT start, not the file.
This module decodes the footer's thrift tree, adds the segment's base
offset to the known offset fields (parquet.thrift):

    FileMetaData.row_groups[]           (field 4)
      RowGroup.file_offset              (field 5)
      RowGroup.columns[]                (field 1)
        ColumnChunk.file_offset         (field 2)
        ColumnChunk.offset_index_offset (field 4)
        ColumnChunk.column_index_offset (field 6)
        ColumnChunk.meta_data           (field 3, ColumnMetaData)
          .data_page_offset             (field 9)
          .index_page_offset            (field 10)
          .dictionary_page_offset       (field 11)
          .bloom_filter_offset          (field 14)

and re-encodes. Everything else round-trips verbatim (as values; varint
widths may legally differ). Unknown types are a loud error — better to
fail than to emit a subtly-wrong footer.
"""

# Compact protocol type ids
_T_BOOL_TRUE = 1
_T_BOOL_FALSE = 2
_T_I8 = 3
_T_I16 = 4
_T_I32 = 5
_T_I64 = 6
_T_DOUBLE = 7
_T_BINARY = 8
_T_LIST = 9
_T_SET = 10
_T_MAP = 11
_T_STRUCT = 12


class _Reader:
    __slots__ = ('b', 'i')

    def __init__(self, buf):
        self.b = buf
        self.i = 0

    def byte(self):
        v = self.b[self.i]
        self.i += 1
        return v

    def varint(self):
        result = 0
        shift = 0
        while True:
            v = self.byte()
            result |= (v & 0x7F) << shift
            if not (v & 0x80):
                return result
            shift += 7

    def zigzag(self):
        n = self.varint()
        return (n >> 1) ^ -(n & 1)

    def take(self, n):
        v = self.b[self.i:self.i + n]
        self.i += n
        return v


class _Writer:
    __slots__ = ('parts',)

    def __init__(self):
        self.parts = bytearray()

    def byte(self, v):
        self.parts.append(v & 0xFF)

    def varint(self, v):
        while True:
            if v <= 0x7F:
                self.parts.append(v)
                return
            self.parts.append((v & 0x7F) | 0x80)
            v >>= 7

    def zigzag(self, v):
        self.varint(_zigzag_encode(v))

    def raw(self, b):
        self.parts += b


def _zigzag_encode(v):
    return ((v << 1) ^ (v >> 63)) & 0xFFFFFFFFFFFFFFFF


# ---- decode to a tree -------------------------------------------------------
# struct  -> list of (field_id, type, value) in encoded order
# list    -> (elem_type, [values])
# i8..i64 -> int; double -> raw 8 bytes; binary -> bytes; bool -> True/False

def _read_value(r, t):
    if t == _T_BOOL_TRUE:
        return True
    if t == _T_BOOL_FALSE:
        return False
    if t == _T_I8:
        return r.byte()
    if t in (_T_I16, _T_I32, _T_I64):
        return r.zigzag()
    if t == _T_DOUBLE:
        return r.take(8)
    if t == _T_BINARY:
        return r.take(r.varint())
    if t == _T_LIST or t == _T_SET:
        head = r.byte()
        size = head >> 4
        elem_t = head & 0x0F
        if size == 15:
            size = r.varint()
        if elem_t in (_T_BOOL_TRUE, _T_BOOL_FALSE):
            vals = [r.byte() != 0 for _ in range(size)]
        else:
            vals = [_read_value(r, elem_t) for _ in range(size)]
        return (elem_t, vals)
    if t == _T_MAP:
        size = r.varint()
        if size == 0:
            return (0, 0, [])
        kv = r.byte()
        kt, vt = kv >> 4, kv & 0x0F
        pairs = [(_read_value(r, kt), _read_value(r, vt))
                 for _ in range(size)]
        return (kt, vt, pairs)
    if t == _T_STRUCT:
        return _read_struct(r)
    raise ValueError(f"thrift: unsupported compact type {t} at byte {r.i}")


def _read_struct(r):
    fields = []
    last_id = 0
    while True:
        head = r.byte()
        if head == 0:
            return fields
        delta = head >> 4
        t = head & 0x0F
        if delta:
            fid = last_id + delta
        else:
            fid = r.zigzag()
        last_id = fid
        if t in (_T_BOOL_TRUE, _T_BOOL_FALSE):
            fields.append((fid, _T_BOOL_TRUE, t == _T_BOOL_TRUE))
        else:
            fields.append((fid, t, _read_value(r, t)))


# ---- encode from the tree ---------------------------------------------------

def _write_value(w, t, v):
    if t == _T_BOOL_TRUE:      # container bool
        w.byte(1 if v else 0)
        return
    if t == _T_I8:
        w.byte(v)
        return
    if t in (_T_I16, _T_I32, _T_I64):
        w.varint(_zigzag_encode(v))
        return
    if t == _T_DOUBLE:
        w.raw(v)
        return
    if t == _T_BINARY:
        w.varint(len(v))
        w.raw(v)
        return
    if t == _T_LIST or t == _T_SET:
        elem_t, vals = v
        if len(vals) < 15:
            w.byte((len(vals) << 4) | elem_t)
        else:
            w.byte(0xF0 | elem_t)
            w.varint(len(vals))
        for item in vals:
            _write_value(w, elem_t, item)
        return
    if t == _T_MAP:
        kt, vt, pairs = v
        w.varint(len(pairs))
        if pairs:
            w.byte((kt << 4) | vt)
            for k, val in pairs:
                _write_value(w, kt, k)
                _write_value(w, vt, val)
        return
    if t == _T_STRUCT:
        _write_struct(w, v)
        return
    raise ValueError(f"thrift: cannot encode type {t}")


def _write_struct(w, fields):
    last_id = 0
    for fid, t, v in fields:
        if t == _T_BOOL_TRUE:          # struct-field bool: type IS the value
            wire_t = _T_BOOL_TRUE if v else _T_BOOL_FALSE
        else:
            wire_t = t
        delta = fid - last_id
        if 0 < delta <= 15:
            w.byte((delta << 4) | wire_t)
        else:
            w.byte(wire_t)
            w.varint(_zigzag_encode(fid))
        last_id = fid
        if t == _T_BOOL_TRUE:
            continue                    # no value bytes
        _write_value(w, t, v)
    w.byte(0)


# ---- the transform ----------------------------------------------------------

def _shift_fields(fields, ids, delta):
    out = []
    for fid, t, v in fields:
        if fid in ids and t == _T_I64:
            v = v + delta
        out.append((fid, t, v))
    return out


def _shift_column_chunk(cc_fields, delta):
    out = []
    for fid, t, v in cc_fields:
        if fid in (4, 5, 6, 7):
            # Drop page-index references (offset_index_offset/_length,
            # column_index_offset/_length). Concatenating segments
            # interleaves their index sections, and mainstream readers
            # (parquet-go) shortcut-read ALL indexes as one contiguous
            # region before the footer -- non-contiguous references
            # crash them. Unreferenced index bytes are dead bytes;
            # row-group statistics (and pruning) are unaffected.
            continue
        if fid == 2 and t == _T_I64:
            v = v + delta
        elif fid == 3 and t == _T_STRUCT:
            v = _shift_fields(v, {9, 10, 11, 14}, delta)
        out.append((fid, t, v))
    return out


def _shift_row_group(rg_fields, delta):
    out = []
    for fid, t, v in rg_fields:
        if fid == 5 and t == _T_I64:
            v = v + delta
        elif fid == 1 and t == _T_LIST:
            elem_t, chunks = v
            v = (elem_t, [_shift_column_chunk(c, delta) for c in chunks])
        out.append((fid, t, v))
    return out


def shift_footer_offsets(footer_thrift, delta):
    """Rebase a parquet footer's file offsets by `delta` bytes.

    footer_thrift: the raw thrift bytes of FileMetaData (no length
    suffix, no magic). Returns re-encoded thrift bytes. Round-trip
    integrity is the caller's test responsibility (and the suite's).
    """
    r = _Reader(footer_thrift)
    tree = _read_struct(r)
    if r.i != len(footer_thrift):
        raise ValueError(
            f"thrift: {len(footer_thrift) - r.i} trailing bytes after "
            f"FileMetaData -- refusing to rebase a footer I don't fully "
            f"understand")
    out = []
    for fid, t, v in tree:
        if fid == 4 and t == _T_LIST:              # row_groups
            elem_t, rgs = v
            v = (elem_t, [_shift_row_group(rg, delta) for rg in rgs])
        out.append((fid, t, v))
    w = _Writer()
    _write_struct(w, out)
    return bytes(w.parts)
