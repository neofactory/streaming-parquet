"""Schema layer: an Arrow schema flattened to storage leaves.

The ring's contract is an Arrow schema (pyarrow.Schema). Nested structs
flatten to leaf columns ('servo.pos'); each leaf gets one storage spec.
Storage is raw Arrow buffers; element access uses struct-format codes
(stdlib memoryview/struct) -- no numpy anywhere.

Supported (see README for the rationale and the drawn line):
  fixed-width  int8..64, uint8..64, float16/32/64, bool, timestamp,
               date32/64, time32/64, duration, decimal128/256,
               fixed_size_binary, fixed_size_list<fixed-width primitive>
  nested       struct (flattened; struct fields must be non-nullable,
               leaves may be nullable)
  var-width    utf8/large_utf8, binary/large_binary,
               list/large_list<fixed-width primitive>
Rejected loudly: map, union, dictionary, run-end, interval, null type,
list of nested/var types, nullable structs.
"""

import pyarrow as pa


class Leaf:
    """Storage spec for one flattened column.

    kind: 'prim'  fixed-width value, one element per row; `code` is the
                  struct/memoryview format char ('e' = float16, which
                  memoryview.cast cannot address -- stored via
                  struct.pack into the byte view)
          'bytes' fixed-width blob per row (decimal, fixed_size_binary)
          'bool'  bit-packed values
          'fsl'   fixed_size_list: width elements of `code` per row
          'var'   variable-width: per-row (elem_start, elem_len) into a
                  SegmentPool of `code` elements
    """
    __slots__ = ('path', 'type', 'nullable', 'kind', 'code', 'itemsize',
                 'width', 'offsets_code')

    def __init__(self, path, type_, nullable, kind, code=None,
                 itemsize=None, width=None, offsets_code=None):
        self.path = path
        self.type = type_
        self.nullable = nullable
        self.kind = kind
        self.code = code                # struct format char of one element
        self.itemsize = itemsize        # bytes per element
        self.width = width              # bytes/fsl: elements (or bytes) per row
        self.offsets_code = offsets_code  # var: 'i' (32-bit) or 'q' (64-bit)


_PRIM_CODE = {pa.int8(): ('b', 1), pa.int16(): ('h', 2),
              pa.int32(): ('i', 4), pa.int64(): ('q', 8),
              pa.uint8(): ('B', 1), pa.uint16(): ('H', 2),
              pa.uint32(): ('I', 4), pa.uint64(): ('Q', 8),
              pa.float16(): ('e', 2), pa.float32(): ('f', 4),
              pa.float64(): ('d', 8)}


def _fixed_prim_code(t):
    """(struct code, itemsize) for a fixed-width primitive Arrow type,
    or None. Temporal types store their raw integer ticks."""
    if t in _PRIM_CODE:
        return _PRIM_CODE[t]
    if pa.types.is_timestamp(t) or pa.types.is_time64(t) \
            or pa.types.is_duration(t) or pa.types.is_date64(t):
        return ('q', 8)
    if pa.types.is_date32(t) or pa.types.is_time32(t):
        return ('i', 4)
    return None


def _leaf_for(path, t, nullable):
    prim = _fixed_prim_code(t)
    if prim is not None:
        return Leaf(path, t, nullable, 'prim', code=prim[0], itemsize=prim[1])
    if pa.types.is_boolean(t):
        return Leaf(path, t, nullable, 'bool')
    if pa.types.is_decimal(t) or pa.types.is_fixed_size_binary(t):
        return Leaf(path, t, nullable, 'bytes', width=t.byte_width)
    if pa.types.is_fixed_size_list(t):
        child = _fixed_prim_code(t.value_type)
        if child is None:
            raise TypeError(
                f"field {path!r}: fixed_size_list of {t.value_type} "
                f"unsupported (fixed-width primitive elements only)")
        return Leaf(path, t, nullable, 'fsl', code=child[0],
                    itemsize=child[1], width=t.list_size)
    if pa.types.is_string(t) or pa.types.is_large_string(t) \
            or pa.types.is_binary(t) or pa.types.is_large_binary(t):
        big = pa.types.is_large_string(t) or pa.types.is_large_binary(t)
        return Leaf(path, t, nullable, 'var', code='B', itemsize=1,
                    offsets_code='q' if big else 'i')
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        child = _fixed_prim_code(t.value_type)
        if child is None:
            raise TypeError(
                f"field {path!r}: list of {t.value_type} unsupported "
                f"(list of fixed-width primitive only; nested/var element "
                f"types are out of scope)")
        big = pa.types.is_large_list(t)
        return Leaf(path, t, nullable, 'var', code=child[0],
                    itemsize=child[1], offsets_code='q' if big else 'i')
    raise TypeError(f"field {path!r}: type {t} unsupported (see "
                    f"arrow_ring README for the supported set)")


def flatten_schema(schema):
    """(leaves, tree) for a pyarrow Schema.

    leaves: list[Leaf] in schema order (struct leaves depth-first).
    tree:   nested dict mirroring the schema; leaf name -> leaf index,
            struct name -> subtree. Drives the row-handle classes.
    """
    if not isinstance(schema, pa.Schema):
        raise TypeError(f"schema must be a pyarrow.Schema, got "
                        f"{type(schema).__name__}")
    if len(schema) == 0:
        raise TypeError("schema has no fields")
    leaves = []
    tree = {}

    def walk(fields, prefix, node):
        for f in fields:
            path = f"{prefix}{f.name}"
            if pa.types.is_struct(f.type):
                if f.nullable:
                    raise TypeError(
                        f"field {path!r}: nullable struct unsupported "
                        f"(declare the struct non-nullable; leaves may "
                        f"be nullable)")
                sub = {}
                node[f.name] = sub
                walk(f.type, f"{path}.", sub)
            else:
                node[f.name] = len(leaves)
                leaves.append(_leaf_for(path, f.type, f.nullable))

    walk(schema, '', tree)
    return leaves, tree
