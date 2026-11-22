"""CFFI binding: the arrow_ring Python API over the C core.

Same public surface as the retired pure-Python implementation (Ring,
get_mutable_row/commit/read_row/pin, RecordSet, the two
errors); the engine is libarrow_ring.dylib (src/core/arrow_ring.c). Python's jobs here:
schema metadata (leaf routing, handle classes via _schema/_row), value
conversion at the boundary (datetime -> ticks, Decimal -> bytes,
sequences -> packed elements), and pyarrow import of exported streams.
Everything stateful — slots, cursors, pins, pools — lives in C.
"""

import collections
import datetime
import decimal
import operator
import logging
import os
import re
import struct
import threading
import traceback
import time
import subprocess

import cffi
import pyarrow as pa

from ._row import make_row_classes
from ._schema import flatten_schema

INT64_MAX = 2 ** 63 - 1

_PKG_DIR = os.path.dirname(os.path.abspath(__file__))
# the library is beside this file; in a checkout its sources are in src/core
_CORE_DIR = os.path.join(os.path.dirname(os.path.dirname(_PKG_DIR)), 'core')
_LIB_PATH = os.path.join(_PKG_DIR, 'libarrow_ring.dylib')

ffi = cffi.FFI()
_CDEF = """
typedef struct {
    int32_t level;
    double wall;
    double mono;
    char text[1024];
} ReportLine;
typedef struct {
    int32_t status;
    int32_t lines;
    int32_t dropped;
    char error[512];
    ReportLine line[4];
} Report;
typedef struct ArRing ArRing;
typedef struct ArRecordSet ArRecordSet;
typedef struct ArRingOptions {
  int64_t segment_bytes;
} ArRingOptions;

int ar_ring_new(const void *schema, int64_t capacity,
                const ArRingOptions *opts, ArRing **out, Report *report);
void ar_ring_free(ArRing *ring);
int32_t ar_leaf_index(const ArRing *ring, const char *path);
int ar_row_begin(ArRing *ring, int64_t *out_seq, Report *report);
int ar_set_i64(ArRing *ring, int32_t leaf, int64_t v, Report *report);
int ar_set_u64(ArRing *ring, int32_t leaf, uint64_t v, Report *report);
int ar_set_f64(ArRing *ring, int32_t leaf, double v, Report *report);
int ar_set_bool(ArRing *ring, int32_t leaf, _Bool v, Report *report);
int ar_set_bytes(ArRing *ring, int32_t leaf, const void *d, int64_t n,
                 Report *report);
int ar_set_list(ArRing *ring, int32_t leaf, const void *e, int64_t n,
                Report *report);
int ar_set_null(ArRing *ring, int32_t leaf, Report *report);
int ar_commit(ArRing *ring, int64_t *out_count, Report *report);
int ar_get_i64(const ArRing *ring, int64_t seq, int32_t leaf,
               int64_t *out, _Bool *is_null, Report *report);
int ar_get_u64(const ArRing *ring, int64_t seq, int32_t leaf,
               uint64_t *out, _Bool *is_null, Report *report);
int ar_get_f64(const ArRing *ring, int64_t seq, int32_t leaf,
               double *out, _Bool *is_null, Report *report);
int ar_get_bool(const ArRing *ring, int64_t seq, int32_t leaf,
                _Bool *out, _Bool *is_null, Report *report);
int ar_get_var(const ArRing *ring, int64_t seq, int32_t leaf,
               const void **data, int64_t *n, _Bool *is_null, Report *report);
int64_t ar_oldest_seq(const ArRing *ring);
int64_t ar_newest_seq(const ArRing *ring);
uint64_t ar_leaf_data_address(const ArRing *ring, int32_t leaf);
uint64_t ar_leaf_valid_address(const ArRing *ring, int32_t leaf);
uint64_t ar_var_data_address(const ArRing *ring, int32_t leaf,
                             int64_t elem_seq);
int32_t ar_pool_segment_count(const ArRing *ring, int32_t leaf);
int64_t ar_pool_oversized_count(const ArRing *ring, int32_t leaf);
int ar_pin(ArRing *ring, int64_t lo, int64_t hi, ArRecordSet **out,
           Report *report);
int64_t ar_recordset_lo(const ArRecordSet *rs);
int64_t ar_recordset_hi(const ArRecordSet *rs);
int ar_recordset_stream(ArRecordSet *rs, void *out_stream, Report *report);
void ar_recordset_release(ArRecordSet *rs);
void ar_recordset_detach(ArRecordSet *rs);
struct ArrowArray {
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void **buffers;
    struct ArrowArray **children;
    struct ArrowArray *dictionary;
    void (*release)(struct ArrowArray *);
    void *private_data;
};
typedef struct ArChunkCursor ArChunkCursor;
int ar_chunks_open(ArRing *ring, int64_t lo, int64_t hi,
                   int64_t chunk_rows, ArChunkCursor **out, Report *report);
int ar_chunks_next(ArChunkCursor *cursor, struct ArrowArray *out,
                   int64_t *first_seq, Report *report);
void ar_chunks_free(ArChunkCursor *cursor);
int ar_append_batch(ArRing *ring, void *batch, Report *report);
int ar_append_batch_at(ArRing *ring, void *batch, int64_t first_seq,
                       Report *report);
typedef struct ArWireMap ArWireMap;
int ar_wiremap_new(const ArRing *ring, const int32_t *leaves,
                   const int32_t *offsets, const int32_t *sizes,
                   int32_t n, ArWireMap **out, Report *report);
void ar_wiremap_free(ArWireMap *map);
int ar_from_wire(ArRing *ring, const ArWireMap *map,
                 const void *frame, int64_t frame_len, Report *report);
int ar_to_wire(const ArRing *ring, const ArWireMap *map, int64_t seq,
               void *frame, int64_t frame_len, Report *report);
void ar_test_set_clock(int fixed, double wall, double mono);
"""
ffi.cdef(_CDEF)


#: The core's lines for the log (an oversized value, a jump in the row
#: numbers) go to this logger, as warnings, with the time the core made
#: them. The core does no output: each line comes back in the report of
#: the call that made it (src/core/report.h).
_log = logging.getLogger("arrow_ring")
_LEVELS = {1: logging.INFO, 2: logging.WARNING, 3: logging.ERROR}


def _log_line(level, wall, text):
    """Log one line of the core, stamped with the time the core made it."""
    level = _LEVELS.get(level, logging.WARNING)
    if not _log.isEnabledFor(level):
        return
    record = _log.makeRecord(_log.name, level, "(arrow_ring core)", 0, text,
                             None, None)
    record.created = wall
    record.msecs = (wall - int(wall)) * 1000.0
    _log.handle(record)


class _Lines:
    """Where a ring's lines go. A ring's writer makes them, and a writer
    is often a thread that must not do output. `hold` False (a ring on
    its own): each line is logged at once, by the thread that made it.
    `hold` True (a host with another thread for output): the lines are
    kept, at most MOST of them, until that thread calls Ring.log_lines().
    It has no reference to the ring: the setters keep one of these."""
    __slots__ = ("hold", "held", "lost")
    MOST = 256

    def __init__(self):
        self.hold = False
        self.held = collections.deque()
        self.lost = 0

    def take(self, r):
        """The lines of the call whose report is r."""
        for i in range(r.lines):
            line = r.line[i]
            self.add(line.level, line.wall,
                     ffi.string(line.text).decode("utf-8", "replace"))
        if r.dropped:
            self.lost += r.dropped

    def add(self, level, wall, text):
        if not self.hold:
            _log_line(level, wall, text)
        elif len(self.held) < self.MOST:
            self.held.append((level, wall, text))
        else:
            self.lost += 1

    def drain(self):
        """[(level, wall time, text)] kept so far, oldest first."""
        out = []
        while self.held:
            out.append(self.held.popleft())
        lost, self.lost = self.lost, 0
        if lost:
            out.append((2, time.time(), f"[arrow_ring] {lost} more lines of "
                                        f"the core were made and not kept"))
        return out


def _build_if_stale():
    """In a checkout, build the library from src/core into this package
    when a source is newer than it. An installed package has the
    library and no sources: nothing to do."""
    sources = [os.path.join(_CORE_DIR, n) for n in ('arrow_ring.c', 'arrow_ring.h', 'report.h')]
    if not all(os.path.exists(s) for s in sources):
        if os.path.exists(_LIB_PATH):
            return
        raise ImportError(
            f"{__package__}: no {_LIB_PATH} and no sources in {_CORE_DIR} "
            f"to build it from; install the package again")
    if (os.path.exists(_LIB_PATH) and os.path.getmtime(_LIB_PATH)
            >= max(os.path.getmtime(s) for s in sources)):
        return
    subprocess.run(['make', '-C', _CORE_DIR, f'OUT={_PKG_DIR}', 'libarrow_ring'],
                   check=True, capture_output=True)


_build_if_stale()
lib = ffi.dlopen(_LIB_PATH)
# Look up every function now. cffi finds a function on its first use
# under a lock of its own that is not reentrant, and it holds that same
# lock while parsing a C type the first time (ffi.new("...")). The
# garbage collector can run inside that parse; a destructor there
# (Ring.__del__ -> ar_ring_free, RecordSet -> ar_recordset_release,
# WireMap -> ar_wiremap_free) looking up its function for the first
# time then waited on the lock its own thread held, forever, and every
# later call into the ring waited behind it (2026-10-27, the library's
# suite hung in wire_map). Resolved here, no call ever takes the lock
# to find a function.
for _name in sorted(set(re.findall(r"\b(ar_\w+)\s*\(", _CDEF))):
    getattr(lib, _name)

# the statuses of src/core/report.h that this binding tells apart
REPORT_OK = 0
REPORT_STALE = 5
REPORT_BLOCKED = 6
REPORT_NOT_NULLABLE = 7
REPORT_WIDTH = 8

_thread = threading.local()


def _report():
    """This thread's Report, for a call that is not the writer's (the
    writer's calls use the ring's own: Ring._wr)."""
    r = getattr(_thread, "report", None)
    if r is None:
        r = _thread.report = ffi.new("Report *")
    return r


class StaleReadError(Exception):
    """The requested seq has been recycled (or was never committed)."""


class PinStalledError(Exception):
    """The writer needs to recycle a slot that a pin still holds."""


def _raise(status, r):
    """The exception for a call that failed, with the reason its report
    holds."""
    msg = ffi.string(r.error).decode("utf-8", "replace")
    if status == REPORT_STALE:
        raise StaleReadError(msg)
    if status == REPORT_BLOCKED:
        raise PinStalledError(msg)
    if status == REPORT_NOT_NULLABLE:
        raise TypeError(msg)
    if status == REPORT_WIDTH:
        raise ValueError(msg)
    raise RuntimeError(msg)


def _check(status, r):
    if status != REPORT_OK:
        _raise(status, r)


_EPOCH_DATE = datetime.date(1970, 1, 1).toordinal()
_UTC = datetime.timezone.utc
_UNIT_SCALE = {'s': 1, 'ms': 10**3, 'us': 10**6, 'ns': 10**9}


def _tick_converter(t):
    """int ticks pass through; stdlib datetime/date/timedelta convert
    (naive datetimes treated as UTC). None for non-temporal leaves."""
    if pa.types.is_timestamp(t) or pa.types.is_date64(t):
        scale = _UNIT_SCALE[t.unit] if pa.types.is_timestamp(t) else 10**3

        def conv_ts(value):
            if isinstance(value, int):
                return value
            if value.tzinfo is None:
                value = value.replace(tzinfo=_UTC)
            return round(value.timestamp() * scale)
        return conv_ts
    if pa.types.is_duration(t):
        scale = _UNIT_SCALE[t.unit]
        return lambda v: v if isinstance(v, int) \
            else round(v.total_seconds() * scale)
    if pa.types.is_date32(t):
        return lambda v: v if isinstance(v, int) \
            else v.toordinal() - _EPOCH_DATE
    return None


class Ring:
    """Column-major ring buffer producing zero-copy Arrow record sets.
    See the package README (the contract) — this class is the Python
    face of the C core."""

    def __init__(self, schema, capacity, segment_bytes=None):
        # Python-side flatten first: validates the type surface with the
        # documented error messages and yields the routing metadata.
        self._leaves, tree = flatten_schema(schema)
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self.schema = schema
        self.capacity = capacity

        cs = ffi.new("char[]", 128)
        schema._export_to_c(int(ffi.cast("uintptr_t", cs)))
        opts = ffi.new("ArRingOptions *")
        opts.segment_bytes = segment_bytes or 0
        out = ffi.new("ArRing **")
        # The writer's Report: a ring has one writer thread, and its
        # calls (a row, a set, a commit, an append) use this one.
        self._wr = wr = ffi.new("Report *")
        self._lines = _Lines()
        st = lib.ar_ring_new(cs, capacity, opts, out, wr)
        if st != REPORT_OK:
            raise TypeError(ffi.string(wr.error).decode("utf-8", "replace"))
        self._c = out[0]

        self._li = [lib.ar_leaf_index(self._c, lf.path.encode())
                    for lf in self._leaves]
        assert all(i >= 0 for i in self._li), "leaf index mismatch vs core"
        self._setters = [self._make_setter(i) for i in range(len(self._leaves))]
        self._getters = [self._make_getter(i) for i in range(len(self._leaves))]
        self._row, self._row_nodes, self._read_cls = make_row_classes(
            tree, 'Ring', self._setters)
        self._seq_out = ffi.new("int64_t *")
        # Every live pin, by RecordSet id: where it was taken, so a stall
        # names its holder (a stall is always an overheld pin; the
        # question is always whose).
        self._pins = {}

    def __del__(self):
        if getattr(self, '_c', None) is not None:
            lib.ar_ring_free(self._c)
            self._c = None

    # ---- the core's lines for the log -----------------------------------------

    @property
    def hold_lines(self):
        """False (the default): a line the core makes for the log (an
        oversized value, a jump in the row numbers) is logged at once,
        by the thread whose call made it. True: the lines are kept until
        log_lines(), for a host whose writer thread must do no output
        and which has another thread to call log_lines() from."""
        return self._lines.hold

    @hold_lines.setter
    def hold_lines(self, hold):
        self._lines.hold = bool(hold)

    @property
    def lines_held(self):
        """True when lines are kept and wait for log_lines()."""
        return bool(self._lines.held) or self._lines.lost > 0

    def take_lines(self):
        """The kept lines, oldest first, as (level, wall time, text);
        level is logging.INFO, WARNING or ERROR. They are no longer
        kept."""
        return [(_LEVELS.get(level, logging.WARNING), wall, text)
                for level, wall, text in self._lines.drain()]

    def log_lines(self):
        """Log the kept lines to the logger "arrow_ring", each with the
        time the core made it. Any thread but the writer's."""
        for level, wall, text in self._lines.drain():
            _log_line(level, wall, text)

    # ---- write side -------------------------------------------------------

    def get_mutable_row(self, block_s=0.0):
        """Allocate the next slot and return a MutableRow over it. Write-
        only handle; waits up to block_s then raises PinStalledError
        when a pin holds the slot being recycled. The waiting is done
        here: the core answers at once."""
        wr = self._wr
        st = lib.ar_row_begin(self._c, self._seq_out, wr)
        if st:
            if st == REPORT_BLOCKED and block_s > 0:
                deadline = time.monotonic() + block_s
                while st == REPORT_BLOCKED and time.monotonic() < deadline:
                    time.sleep(0.001)
                    st = lib.ar_row_begin(self._c, self._seq_out, wr)
        if st:
            if st == REPORT_BLOCKED:
                msg = ffi.string(wr.error).decode("utf-8", "replace")
                raise PinStalledError(msg + self._pins_report())
            _raise(st, wr)
        slot = self._seq_out[0] % self.capacity
        for node in self._row_nodes:
            node._slot = slot
        return self._row

    def commit(self):
        n = ffi.new("int64_t *")
        _check(lib.ar_commit(self._c, n, self._wr), self._wr)
        return n[0]

    # ---- read side ----------------------------------------------------------

    @property
    def newest_seq(self):
        return lib.ar_newest_seq(self._c)

    @property
    def oldest_seq(self):
        return lib.ar_oldest_seq(self._c)

    def read_row(self, seq):
        """Read-only view of one committed row (staleness checked now and
        again on every field access)."""
        if seq > lib.ar_newest_seq(self._c):
            raise StaleReadError(
                f"seq {seq} not committed (newest is "
                f"{lib.ar_newest_seq(self._c)})")
        oldest = lib.ar_oldest_seq(self._c)
        if seq < oldest:
            raise StaleReadError(
                f"seq {seq} recycled (oldest readable is {oldest})")
        return self._read_cls(self, seq)   # a Row's slot IS the seq

    def pin(self, lo, hi):
        """Pin the rows [lo, hi) and no others: a frozen record set
        for a caller that knows which rows it needs (the publish and
        archive paths: the rows they send or write). The writer meets
        this pin only after a whole ring of writes, and then the
        holder is a leak; a pin on the oldest row of a full ring makes
        the writer wait at once. lo below the oldest readable row is
        raised to it (rec.lo > lo says rows were recycled); hi above
        committed is lowered to it. Yours to release — release(),
        detach(), or use as a context manager."""
        out = ffi.new("ArRecordSet **")
        r = _report()
        _check(lib.ar_pin(self._c, int(lo), int(hi), out, r), r)
        rs = out[0]
        rec = RecordSet(self, rs, lib.ar_recordset_lo(rs),
                        lib.ar_recordset_hi(rs))
        # Noted with where it was taken, so that a stall names its holder.
        now = time.monotonic()
        self._pins[id(rec)] = [rec.lo, rec.hi, now, False,
                               traceback.extract_stack(limit=9)[:-1]]
        # A detached pin's end is not observable here (it dies with its
        # table's buffers); keep detached entries 30 s, so that a stall
        # still names a pin detached a moment ago.
        if len(self._pins) > 256:
            for k, p in list(self._pins.items()):
                if p[3] and now - p[2] > 30.0:
                    del self._pins[k]
        return rec

    def _pins_report(self):
        """The live pins, oldest first, with where each was taken: the
        tail of a PinStalledError message. A detached pin lives on in
        its table's batches until the last consumer drops them."""
        now = time.monotonic()
        lines = []
        for lo, hi, t0, detached, stack in sorted(self._pins.values(),
                                                  key=lambda p: p[2]):
            where = " <- ".join(f"{f.name} ({os.path.basename(f.filename)}:{f.lineno})"
                                for f in reversed(stack[-4:]))
            lines.append(f"  pin [{lo}, {hi}) age {now - t0:.1f}s"
                         f"{' DETACHED (lives in its table)' if detached else ''}"
                         f" taken at {where}")
        return ("\n live pins:\n" + "\n".join(lines)) if lines else "\n (no live pins recorded)"

    CHUNK_ROWS = 16384

    def iter_chunks(self, lo=0, hi=None, chunk_rows=CHUNK_ROWS):
        """Read [lo, hi) a chunk at a time, pinning only the chunk in
        hand: yields (first_seq, pyarrow.RecordBatch). Nothing is pinned
        before the first chunk is asked for, and the range is fixed
        then, not now: hi (None: no bound) is lowered to the committed
        row, lo raised to the oldest row. Each batch's pin drops when
        the batch is freed. A row the writer recycles before the read
        reaches it raises StaleReadError naming the rows: the read was
        lapped. This is what the DuckDB views read through, so a scan
        holds only its current chunk however long the query runs."""
        out = ffi.new("ArChunkCursor **")
        r = _report()
        _check(lib.ar_chunks_open(self._c, int(lo),
                                  INT64_MAX if hi is None else int(hi),
                                  int(chunk_rows), out, r), r)
        cur = out[0]
        try:
            seq = ffi.new("int64_t *")
            while True:
                arr = ffi.new("struct ArrowArray *")
                # this thread's report now: a generator may be pulled
                # by another thread than the one that made it
                r = _report()
                _check(lib.ar_chunks_next(cur, arr, seq, r), r)
                if arr.release == ffi.NULL:
                    return
                # Built inside the yield: this frame keeps no reference
                # to the batch across the suspension, so the consumer's
                # drop is the last one and the chunk's pin goes with it.
                yield seq[0], pa.RecordBatch._import_from_c(
                    int(ffi.cast("uintptr_t", arr)), self.schema)
        finally:
            lib.ar_chunks_free(cur)

    def chunked_reader(self, lo=0, hi=None, chunk_rows=CHUNK_ROWS):
        """iter_chunks as a pyarrow.RecordBatchReader, an Arrow stream a
        DuckDB view registers (it has __arrow_c_stream__). `map` rather
        than a generator: a generator's loop variable would hold each
        batch, and its pin, until the next pull."""
        return pa.RecordBatchReader.from_batches(
            self.schema,
            map(operator.itemgetter(1), self.iter_chunks(lo, hi, chunk_rows)))

    def get_dynamic_relation(self, chunk_rows=CHUNK_ROWS):
        """A standing view: register it once (DuckDB scans objects with
        __arrow_c_stream__); every scan reads memory as it is when the
        scan begins, a chunk at a time, pinning only the chunk in hand.
        Never release it — it has nothing to release."""
        return DynamicRelation(self, chunk_rows)

    # ---- diagnostics ----------------------------------------------------------

    def leaf_data_address(self, leaf):
        return lib.ar_leaf_data_address(self._c, leaf)

    def var_data_address(self, leaf, elem_seq):
        return lib.ar_var_data_address(self._c, leaf, elem_seq)

    def pool_segment_count(self, leaf):
        return lib.ar_pool_segment_count(self._c, leaf)

    def pool_oversized_count(self, leaf):
        return lib.ar_pool_oversized_count(self._c, leaf)

    # ---- ingest (Arrow batches in: the memcpy path) ----------------------------

    def append_batch(self, batch, at_seq=None):
        """Append a pyarrow RecordBatch with this ring's schema.

        at_seq places the batch at an absolute sequence number (the
        attach-side alignment primitive: keep the local ring on the
        publisher's seq axis). A forward jump is a loud GAP that
        raises the readable floor; a regression is an error. None =
        next expected."""
        ca = ffi.new("char[]", 128)
        batch._export_to_c(int(ffi.cast("uintptr_t", ca)))
        wr = self._wr
        st = lib.ar_append_batch_at(
            self._c, ca, -1 if at_seq is None else int(at_seq), wr)
        if wr.lines or wr.dropped:
            self._lines.take(wr)
        _check(st, wr)

    def append_table(self, table):
        for batch in table.to_batches():
            self.append_batch(batch)

    # ---- wire frames (packed PLC/probe packets) ----------------------------------

    def wire_map(self, entries):
        """Precompiled scatter/gather map for packed row-major frames.
        entries: [(leaf_path, byte_offset, byte_size), ...]. Sizes must
        equal each leaf's storage width; only fixed-width leaves map."""
        n = len(entries)
        leaves = ffi.new("int32_t[]", n)
        offsets = ffi.new("int32_t[]", n)
        sizes = ffi.new("int32_t[]", n)
        for i, (path, off, size) in enumerate(entries):
            li = lib.ar_leaf_index(self._c, path.encode())
            if li < 0:
                raise KeyError(f"no leaf {path!r}")
            leaves[i], offsets[i], sizes[i] = li, off, size
        out = ffi.new("ArWireMap **")
        r = _report()
        _check(lib.ar_wiremap_new(self._c, leaves, offsets, sizes, n, out, r),
               r)
        return WireMap(self, out[0])

    def from_wire(self, wmap, frame):
        """Scatter one packed frame into the open row (get_mutable_row
        first). One C call, no per-field dispatch. `frame` is bytes or
        any buffer (bytearray, memoryview, a ctypes struct): C reads it
        in place, no copy."""
        buf = ffi.from_buffer(frame)
        _check(lib.ar_from_wire(self._c, wmap._c, buf, len(buf), self._wr),
               self._wr)

    def to_wire(self, wmap, seq, frame_len):
        """Gather a committed row's mapped leaves into a frame (TX)."""
        buf = ffi.new("char[]", frame_len)
        r = _report()
        _check(lib.ar_to_wire(self._c, wmap._c, seq, buf, frame_len, r), r)
        return bytes(ffi.buffer(buf, frame_len))

    # ---- leaf dispatch (called by the _row.py descriptors) ----------------------

    def _leaf_get(self, li, seq):
        return self._getters[li](seq)

    def _leaf_view(self, li, nbytes):
        """A writable memoryview over a leaf's ring buffer (the core's
        own storage; the buffer lives as long as the ring)."""
        addr = lib.ar_leaf_data_address(self._c, li)
        return memoryview(ffi.buffer(ffi.cast("void *", addr), nbytes))

    def _make_setter(self, i):
        """The leaf's store, shaped as a descriptor __set__(self, row,
        value): fixed-width leaves write into a memoryview of the leaf's
        buffer at row._slot and set the validity bit themselves -- no
        call into C per field. memoryview is strict: an out-of-range
        int, a float into an int leaf, a str, or None into a
        non-nullable leaf all refuse (ValueError / TypeError, the same
        classes the core's refusals map to). Var-width leaves (strings,
        lists) go through the core's pool setters; float16 too (no
        memoryview format)."""
        leaf = self._leaves[i]
        li = self._li[i]
        c = self._c
        wr = self._wr               # these are the writer's calls
        lines = self._lines
        cap = self.capacity
        t = leaf.type
        path = leaf.path
        valid = None
        if leaf.nullable:
            vaddr = lib.ar_leaf_valid_address(c, li)
            valid = memoryview(ffi.buffer(ffi.cast("void *", vaddr),
                                          (cap + 7) // 8))

        def not_nullable():
            raise TypeError(f"field '{path}' is not nullable")

        def c_set(setter):
            """A core setter wrapped as the descriptor shape."""
            if valid is None:
                def set_(self_, row, value):
                    if value is None:
                        not_nullable()
                    setter(value)
            else:
                def set_(self_, row, value):
                    if value is None:
                        _check(lib.ar_set_null(c, li, wr), wr)
                        return
                    setter(value)
            return set_

        if leaf.kind == 'prim':
            conv = _tick_converter(t)
            if leaf.code == 'e':            # float16: no memoryview format
                return c_set(
                    lambda v: _check(lib.ar_set_f64(c, li, float(v), wr), wr))
            view = self._leaf_view(li, cap * leaf.itemsize).cast(leaf.code)
            if valid is None and conv is None:
                def set_(self_, row, value):
                    if value is None:
                        not_nullable()
                    view[row._slot] = value
            elif valid is None:
                def set_(self_, row, value):
                    if value is None:
                        not_nullable()
                    view[row._slot] = conv(value)
            elif conv is None:
                def set_(self_, row, value):
                    slot = row._slot
                    if value is None:
                        valid[slot >> 3] &= ~(1 << (slot & 7)) & 0xff
                        return
                    view[slot] = value
                    valid[slot >> 3] |= 1 << (slot & 7)
            else:
                def set_(self_, row, value):
                    slot = row._slot
                    if value is None:
                        valid[slot >> 3] &= ~(1 << (slot & 7)) & 0xff
                        return
                    view[slot] = conv(value)
                    valid[slot >> 3] |= 1 << (slot & 7)
            return set_
        if leaf.kind == 'bool':
            bits = self._leaf_view(li, (cap + 7) // 8)

            def set_(self_, row, value):
                slot = row._slot
                if value is None:
                    if valid is None:
                        not_nullable()
                    valid[slot >> 3] &= ~(1 << (slot & 7)) & 0xff
                    return
                if value:
                    bits[slot >> 3] |= 1 << (slot & 7)
                else:
                    bits[slot >> 3] &= ~(1 << (slot & 7)) & 0xff
                if valid is not None:
                    valid[slot >> 3] |= 1 << (slot & 7)
            return set_
        if leaf.kind in ('bytes', 'fsl'):
            # one fixed-width blob per row, written as bytes into the
            # byte view: decimals and fixed_size_binary (`width` bytes),
            # fixed_size_list (`width` elements of `code`)
            if leaf.kind == 'bytes':
                row_bytes = leaf.width
                if pa.types.is_decimal(t):
                    scale, width = t.scale, t.byte_width

                    def pack(v):
                        if isinstance(v, decimal.Decimal):
                            v = int(v.scaleb(scale).to_integral_exact())
                        return int(v).to_bytes(width, 'little', signed=True)
                else:
                    def pack(v):
                        if len(v) != row_bytes:
                            raise ValueError(
                                f"field '{path}': needs exactly {row_bytes} "
                                f"bytes, got {len(v)}")
                        return v
            else:
                row_bytes = leaf.width * leaf.itemsize
                fmt = f'<{leaf.width}{leaf.code}'
                n_el = leaf.width

                def pack(v):
                    if len(v) != n_el:
                        raise ValueError(
                            f"field '{path}' needs exactly {n_el} "
                            f"elements, got {len(v)}")
                    return struct.pack(fmt, *v)
            raw = self._leaf_view(li, cap * row_bytes)

            def set_(self_, row, value):
                slot = row._slot
                if value is None:
                    if valid is None:
                        not_nullable()
                    valid[slot >> 3] &= ~(1 << (slot & 7)) & 0xff
                    return
                off = slot * row_bytes
                raw[off:off + row_bytes] = pack(value)
                if valid is not None:
                    valid[slot >> 3] |= 1 << (slot & 7)
            return set_
        # var: the core owns the segment pool
        # (a value larger than a pool segment is taken, with a line for
        # the log in the call's report)
        def took(st):
            if wr.lines or wr.dropped:
                lines.take(wr)
            if st:
                _raise(st, wr)

        if pa.types.is_string(t) or pa.types.is_large_string(t):
            def set_str(v):
                b = v.encode('utf-8')
                took(lib.ar_set_bytes(c, li, b, len(b), wr))
            return c_set(set_str)
        if pa.types.is_binary(t) or pa.types.is_large_binary(t):
            return c_set(lambda v: took(lib.ar_set_bytes(c, li, v, len(v), wr)))
        code = leaf.code

        def set_lst(v):
            took(lib.ar_set_list(c, li, struct.pack(f'<{len(v)}{code}', *v),
                                 len(v), wr))
        return c_set(set_lst)

    def _make_getter(self, i):
        leaf = self._leaves[i]
        li = self._li[i]
        c = self._c
        t = leaf.type

        if leaf.kind == 'prim':
            if pa.types.is_floating(t):
                def get_f(seq):
                    out = ffi.new("double *"); nul = ffi.new("_Bool *")
                    r = _report()
                    _check(lib.ar_get_f64(c, seq, li, out, nul, r), r)
                    return None if nul[0] else out[0]
                return get_f
            if pa.types.is_unsigned_integer(t):
                def get_u(seq):
                    out = ffi.new("uint64_t *"); nul = ffi.new("_Bool *")
                    r = _report()
                    _check(lib.ar_get_u64(c, seq, li, out, nul, r), r)
                    return None if nul[0] else out[0]
                return get_u

            def get_i(seq):
                out = ffi.new("int64_t *"); nul = ffi.new("_Bool *")
                r = _report()
                _check(lib.ar_get_i64(c, seq, li, out, nul, r), r)
                return None if nul[0] else out[0]
            return get_i
        if leaf.kind == 'bool':
            def get_b(seq):
                out = ffi.new("_Bool *"); nul = ffi.new("_Bool *")
                r = _report()
                _check(lib.ar_get_bool(c, seq, li, out, nul, r), r)
                return None if nul[0] else bool(out[0])
            return get_b

        # bytes/fsl/var all read through the borrowed-pointer view.
        is_str = pa.types.is_string(t) or pa.types.is_large_string(t)
        is_bin = (pa.types.is_binary(t) or pa.types.is_large_binary(t)
                  or leaf.kind == 'bytes')
        code = leaf.code
        itemsize = leaf.itemsize

        def get_v(seq):
            d = ffi.new("const void **"); n = ffi.new("int64_t *")
            nul = ffi.new("_Bool *")
            r = _report()
            _check(lib.ar_get_var(c, seq, li, d, n, nul, r), r)
            if nul[0]:
                return None
            raw = bytes(ffi.buffer(d[0], n[0] * (itemsize or 1))) \
                if n[0] else b''
            if is_str:
                return raw.decode('utf-8')
            if is_bin:
                return raw
            return struct.unpack(f'<{n[0]}{code}', raw)
        return get_v


class RecordSet:
    """A pinned, immutable, zero-copy Arrow view of a ring region.
    Release promptly — release() or use as a context manager; do not
    read the table after release (memory-safe but recyclable)."""

    def __init__(self, ring, c_rs, lo, hi):
        self._ring = ring      # keeps the Ring (and so the C ring) alive
        self._c = c_rs
        self.lo = lo
        self.hi = hi
        self._table = None
        self._released = False

    def __len__(self):
        return self.hi - self.lo

    def clamp_hi(self, new_hi):
        """Narrow the visible end of the set (playback clocks). The pin
        keeps its original range (safe: wider); the table is sliced.
        Must be called before the table is materialized."""
        if self._table is not None:
            raise RuntimeError("clamp_hi after table materialization")
        if new_hi < self.lo:
            new_hi = self.lo
        if new_hi < self.hi:
            self.hi = new_hi

    @property
    def table(self):
        if self._table is None:
            if self._released:
                raise RuntimeError("record set released; table gone")
            stream = ffi.new("char[]", 80)
            r = _report()
            _check(lib.ar_recordset_stream(self._c, stream, r), r)
            reader = pa.RecordBatchReader._import_from_c(
                int(ffi.cast("uintptr_t", stream)))
            t = reader.read_all()
            if t.num_rows > self.hi - self.lo:      # clamped
                t = t.slice(0, self.hi - self.lo)
            self._table = t
        return self._table

    def _forget(self, detached):
        rec = self._ring._pins.get(id(self))
        if rec is not None:
            if detached:
                rec[3] = True
            else:
                self._ring._pins.pop(id(self), None)

    def release(self):
        """Drop the pin. Idempotent. Release means done."""
        if not self._released:
            self._forget(detached=False)
            self._released = True
            lib.ar_recordset_release(self._c)
            self._c = None

    def detach(self):
        """Materialize the table, then hand the pin to it: the pin
        releases itself when the last consumer of the table's batches
        does. Returns the table. For fire-and-forget publish paths."""
        t = self.table
        if not self._released:
            self._forget(detached=True)
            self._released = True
            lib.ar_recordset_detach(self._c)
            self._c = None
        return t

    def __del__(self):
        self.release()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.release()


class WireMap:
    """Owns an ArWireMap; build via Ring.wire_map()."""

    def __init__(self, ring, c_map):
        self._ring = ring
        self._c = c_map

    def __del__(self):
        if self._c is not None:
            lib.ar_wiremap_free(self._c)
            self._c = None


class DynamicRelation:
    """Standing always-current view over a Ring (or anything with
    chunked_reader). Register it once with DuckDB; every scan reads
    memory as it is when the scan begins, a chunk at a time, pinning
    only the chunk in hand (Ring.iter_chunks). Nothing is pinned
    between scans, or between chunks, or while the query around the
    scan does other work. Two scans in one query each read memory as
    of their own start."""

    def __init__(self, source, chunk_rows=Ring.CHUNK_ROWS):
        self._source = source
        self._chunk_rows = int(chunk_rows)

    def __arrow_c_stream__(self, requested_schema=None):
        return self._source.chunked_reader(
            chunk_rows=self._chunk_rows).__arrow_c_stream__(requested_schema)

    @property
    def table(self):
        """Memory now, as a pyarrow Table. Zero copy: the table pins
        the rows it holds until it is dropped."""
        return self._source.chunked_reader(chunk_rows=self._chunk_rows).read_all()
