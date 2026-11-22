"""Row handles: descriptor-backed views into the ring's leaf storage.

Generated per ring from the flatten tree:

  Row        -- returned by Ring.read_row(). Getters through the core's
               checked readers; no setters, and __slots__ turns an
               accidental assignment into an AttributeError. Nullable
               leaves read back None when null. One shape everywhere:
               attribute per column, `row.tv`, `row.servo.pos`.
  MutableRow -- returned by Ring.get_mutable_row(): a subclass of Row
               whose leaves are writable. `row.field = value` is ONE
               descriptor call that stores straight into the leaf's
               ring buffer at the open slot (a memoryview over the
               core's own storage, no call into C per field); the
               setter functions come from Ring._make_setter. The slot
               is not committed yet, so its leaves are write-only
               until commit; read committed rows through the readers.

Struct fields produce sub-handles at the same slot: `row.servo.pos =
100` is one attribute fetch plus one leaf store. A ring has ONE
mutable handle tree, reused for every row: get_mutable_row() moves
every node of the tree to the new slot. Handles are not to be held
across commits.
"""


class _ReadField:
    __slots__ = ('_li',)

    def __init__(self, li):
        self._li = li

    def __get__(self, row, owner):
        if row is None:
            return self
        return row._ring._leaf_get(self._li, row._slot)


class _ReadSubHandle:
    """Struct-field accessor on a Row: a child handle at the same seq."""
    __slots__ = ('_cls',)

    def __init__(self, cls):
        self._cls = cls

    def __get__(self, row, owner):
        if row is None:
            return self
        return self._cls(row._ring, row._slot)

    def __set__(self, row, value):
        raise AttributeError(
            "cannot assign a struct field directly; set its leaves "
            "(row.servo.pos = ...)")


class _MutableSubHandle:
    """Struct-field accessor on the mutable tree: the one cached child."""
    __slots__ = ('_child',)

    def __init__(self, child):
        self._child = child

    def __get__(self, row, owner):
        if row is None:
            return self
        return self._child

    def __set__(self, row, value):
        raise AttributeError(
            "cannot assign a struct field directly; set its leaves "
            "(row.servo.pos = ...)")


def _mutable_leaf(setter):
    """A descriptor type whose __set__ IS the leaf's setter: one Python
    call from `row.x = v` to the store."""
    def _get(self, row, owner):
        if row is None:
            return self
        raise AttributeError("a mutable row's slot is not committed yet; "
                             "read committed rows through the readers")
    return type('_Leaf', (), {'__slots__': (), '__set__': setter,
                              '__get__': _get})()


def _build_read(tree, type_name):
    def _init(self, ring, slot):
        object.__setattr__(self, '_ring', ring)
        object.__setattr__(self, '_slot', slot)

    ns = {'__slots__': ('_ring', '_slot'), '__init__': _init}
    for name, node in tree.items():
        if isinstance(node, dict):
            ns[name] = _ReadSubHandle(_build_read(node, f"{type_name}_{name}"))
        else:
            ns[name] = _ReadField(node)
    return type(type_name, (), ns)


def _build_mutable(tree, type_name, setters, nodes, read_cls):
    """One handle instance per struct node, each a subclass of the
    matching read class with writable leaves; `nodes` collects them all
    so get_mutable_row can move the whole tree to a slot."""
    def _init(self):
        object.__setattr__(self, '_ring', None)
        object.__setattr__(self, '_slot', 0)

    ns = {'__slots__': (), '__init__': _init}
    for name, node in tree.items():
        if isinstance(node, dict):
            child_read = type.__getattribute__(read_cls, name)._cls
            child = _build_mutable(node, f"{type_name}_{name}", setters,
                                   nodes, child_read)
            ns[name] = _MutableSubHandle(child)
        else:
            ns[name] = _mutable_leaf(setters[node])
    handle = type(type_name, (read_cls,), ns)()
    nodes.append(handle)
    return handle


def make_row_classes(tree, type_name, setters):
    """(the mutable handle tree's root, every node of it, the Row class)
    for a flatten tree. `setters[leaf_index]` is a function
    (descriptor, row, value) that stores into the leaf at row._slot."""
    read_cls = _build_read(tree, f"{type_name}Row")
    nodes = []
    root = _build_mutable(tree, f"{type_name}MutableRow", setters, nodes,
                          read_cls)
    return root, nodes, read_cls
