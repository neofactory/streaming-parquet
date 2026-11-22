"""CFFI binding of the stream core: src/core/sp.c, whose header sp.h is the
spec.

The core holds the rules of streaming-parquet that every host language
shares, written once: formats and decisions, no input or output. How a call
reports is src/core/report.h. This module is what the Python host calls; the Go host
(src/go/streaming) calls the same core. A rule that is in the core is in no
host.

The library is built with `make` and the system C compiler the first
time this module is imported in a checkout, from its sources in
src/core into this package, like the ring's and the checkpoint core's;
`pip install` builds it at install.
"""

import os
import subprocess
import threading

import cffi

_PKG_DIR = os.path.dirname(os.path.abspath(__file__))
# the library is beside this file; in a checkout its sources are in src/core
_CORE_DIR = os.path.join(os.path.dirname(os.path.dirname(_PKG_DIR)), 'core')
_LIB_PATH = os.path.join(_PKG_DIR, 'libsp.dylib')

ffi = cffi.FFI()
ffi.cdef("""
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

typedef struct SpTicket SpTicket;
const char *sp_ticket_field_name(int field);
int sp_ticket_field_kind(int field);
int sp_ticket_new(SpTicket **out, Report *report);
void sp_ticket_free(SpTicket *t);
int sp_ticket_set_text(SpTicket *t, int field, const char *text,
                       Report *report);
int sp_ticket_set_integer(SpTicket *t, int field, int64_t value,
                          Report *report);
int sp_ticket_set_number(SpTicket *t, int field, double value,
                         Report *report);
void sp_ticket_clear(SpTicket *t, int field);
int sp_ticket_add_file(SpTicket *t, const char *checkpoint,
                       const int64_t *first_row, const int64_t *final_rows,
                       const int64_t *rows, Report *report);
const char *sp_ticket_text(const SpTicket *t, int field);
int sp_ticket_integer(const SpTicket *t, int field, int64_t *out);
int sp_ticket_number(const SpTicket *t, int field, double *out);
int32_t sp_ticket_file_count(const SpTicket *t);
int sp_ticket_file(const SpTicket *t, int32_t index, const char **checkpoint,
                   int64_t *first_row, int *has_first_row,
                   int64_t *final_rows, int *has_final_rows,
                   int64_t *rows, int *has_rows);
int sp_ticket_write(const SpTicket *t, char **out, size_t *out_len,
                    Report *report);
int sp_ticket_read(const char *text, size_t len, SpTicket **out,
                   Report *report);
double sp_ticket_stale_after(const SpTicket *t, double if_absent);
int sp_ticket_is_stale(const SpTicket *t, double if_absent);
void sp_free(void *p);
int sp_lock_record(int64_t pid, const char *host, int64_t port,
                   const char *run, char **out, size_t *out_len,
                   Report *report);
int sp_lock_refusal(const char *who, const char *lock_path,
                    const char *holder, size_t holder_len, Report *report);
int sp_ownership(const char *who, const char *name, const char *store,
                 const SpTicket *ticket, const char *host,
                 double if_absent, int *decision, Report *report);
int sp_ownership_unread(const char *name, const char *store,
                        const char *why, Report *report);
typedef struct SpWrites SpWrites;
typedef struct { int kind; int64_t lo, hi; int64_t lost; } SpWrite;
int sp_writes_new(SpWrites **out, Report *report);
void sp_writes_free(SpWrites *w);
int sp_writes_set_options(SpWrites *w, int64_t row_group_rows,
                          const double *row_group_seconds,
                          const double *checkpoint_seconds,
                          Report *report);
void sp_writes_start(SpWrites *w, int64_t committed);
void sp_writes_stop(SpWrites *w);
int sp_writes_due(const SpWrites *w, int64_t committed);
int sp_writes_commit(SpWrites *w, int64_t committed, int *begins);
void sp_writes_plan(SpWrites *w, int force_row_group, int64_t lo,
                    int64_t hi, SpWrite *out);
int sp_writes_on(const SpWrites *w);
int64_t sp_writes_cursor(const SpWrites *w);
int64_t sp_writes_planned(const SpWrites *w);
int64_t sp_writes_lost(const SpWrites *w);
typedef struct SpStore SpStore;
int sp_store_new(double keep_seconds, SpStore **out, Report *report);
void sp_store_free(SpStore *s);
int sp_store_set_keep_seconds(SpStore *s, double keep_seconds,
                              Report *report);
double sp_store_keep_seconds(const SpStore *s);
int sp_store_begins(SpStore *s, const char *file, int64_t first_row,
                    Report *report);
int sp_store_covers(SpStore *s, const char *file, int64_t version,
                    const char *checkpoint, int64_t final_rows, int64_t rows,
                    Report *report);
int sp_store_stored(SpStore *s, const char *file, int64_t version,
                    const char *checkpoint, int64_t rows, Report *report);
int sp_store_finished(SpStore *s, const char *file, Report *report);
int sp_store_awaits(SpStore *s, const char *file, Report *report);
int sp_store_listed(SpStore *s, const char *file);
int sp_store_catalog(SpStore *s, const char *version, Report *report);
int sp_store_dropped(SpStore *s);
const char *sp_store_due(const SpStore *s, int32_t index);
int sp_store_discarded(SpStore *s, const char *file, Report *report);
int64_t sp_store_stored_row(const SpStore *s);
int sp_store_stored_at(const SpStore *s, double *wall);
int sp_store_fill(const SpStore *s, SpTicket *t, const char *where,
                  Report *report);
int32_t sp_store_files(const SpStore *s);
typedef struct SpReader SpReader;
typedef struct {
    int action;
    int state;
    int fresh_ring;
    double wait_seconds;
} SpStep;
int sp_reader_new(const char *who, const char *name, const char *store,
                  int direct, SpReader **out, Report *report);
void sp_reader_free(SpReader *r);
const char *sp_reader_folder(const SpReader *r);
int sp_reader_poll(SpReader *r, int found, const SpTicket *ticket,
                   const char *why, double if_absent, SpStep *step,
                   Report *report);
int sp_reader_folder_is(SpReader *r, int exists, SpStep *step,
                        Report *report);
void sp_reader_connected(SpReader *r);
int sp_reader_lost(SpReader *r, const char *why, SpStep *step,
                   Report *report);
void sp_reader_close(SpReader *r);
int sp_reader_state(const SpReader *r);
const char *sp_reader_state_name(int state);
const char *sp_reader_reason(const SpReader *r);
const char *sp_reader_run(const SpReader *r);
typedef struct SpView SpView;
typedef struct { int build; int catalog; int64_t seam; int use_ring; } SpViewPlan;
int64_t sp_view_seam(const SpTicket *t, const int64_t *window, int *use_ring,
                     int *why);
int sp_view_new(const char *view, const char *stream, const char *memory,
                const char *empty, SpView **out, Report *report);
void sp_view_free(SpView *v);
int sp_view_plan(SpView *v, const SpTicket *t, const char *const *files,
                 const char *ring_run, const int64_t *window,
                 double if_absent, double keep_if_absent, SpViewPlan *out,
                 Report *report);
void sp_view_set_row_ids(SpView *v, int on);
int sp_view_live_sql(const char *view, const char *memory, char **out,
                     Report *report);
const char *sp_view_catalog_etag(const SpView *v);
int sp_view_sql(const SpView *v, const char *catalog, char **out,
                Report *report);
void sp_view_built(SpView *v);
int64_t sp_view_seam_row(const SpView *v);
const char *sp_view_ring_note(const SpView *v);
double sp_view_wait_seconds(const SpView *v);
int sp_view_failed(SpView *v, const char *why, double *wait_seconds,
                   Report *report);
int sp_view_followed(SpView *v, Report *report);
typedef struct SpCatalog SpCatalog;
int sp_parquet_footer_length(const uint8_t *tail, size_t len, int64_t *out,
                             Report *report);
int sp_catalog_open(const uint8_t *bytes, size_t len, const char *data_path,
                    SpCatalog **out, Report *report);
void sp_catalog_free(SpCatalog *c);
int sp_catalog_lists(const SpCatalog *c, const char *path);
int64_t sp_catalog_files(const SpCatalog *c);
int sp_catalog_file(const SpCatalog *c, int64_t index, char **out,
                    Report *report);
int sp_catalog_add(SpCatalog *c, const char *path, int64_t file_size,
                   const uint8_t *footer, size_t footer_len,
                   Report *report);
int64_t sp_catalog_staged(const SpCatalog *c);
int sp_catalog_bytes(SpCatalog *c, uint8_t **out, size_t *out_len,
                     Report *report);
void sp_test_set_clock(int fixed, double wall, double mono);
""")


def _build_if_stale():
    """In a checkout, build the library from src/core into this package
    when a source is newer than it. An installed package has the
    library and no sources: nothing to do."""
    sources = [os.path.join(_CORE_DIR, n) for n in ('sp.c', 'sp_catalog.c', 'sp.h', 'report.h')]
    if not all(os.path.exists(s) for s in sources):
        if os.path.exists(_LIB_PATH):
            return
        raise ImportError(
            f"{__package__}: no {_LIB_PATH} and no sources in {_CORE_DIR} "
            f"to build it from; install the package again")
    if (os.path.exists(_LIB_PATH) and os.path.getmtime(_LIB_PATH)
            >= max(os.path.getmtime(s) for s in sources)):
        return
    subprocess.run(['make', '-C', _CORE_DIR, f'OUT={_PKG_DIR}', 'libsp'],
                   check=True, capture_output=True)


_build_if_stale()
lib = ffi.dlopen(_LIB_PATH)

_TEXT, _INTEGER, _NUMBER = 0, 1, 2

#: member name -> (the core's number for it, its kind)
TICKET_FIELDS = {}
_i = 0
while lib.sp_ticket_field_name(_i) != ffi.NULL:
    TICKET_FIELDS[ffi.string(lib.sp_ticket_field_name(_i)).decode()] = (
        _i, lib.sp_ticket_field_kind(_i))
    _i += 1


# ---- the report (src/core/report.h) -------------------------------------------
#
# Every call that can fail, or that can have a line for the log, is given
# this thread's Report. A status that is not 0 becomes an exception with
# the report's error text; the lines come back to the caller, which logs
# them.

_NO_MEMORY = 4
LOG_INFO, LOG_WARNING, LOG_ERROR = 1, 2, 3

_thread = threading.local()


def _report():
    r = getattr(_thread, "report", None)
    if r is None:
        r = _thread.report = ffi.new("Report *")
    return r


def _error(r):
    return ffi.string(r.error).decode("utf-8", "replace")


def _lines(r):
    """The lines of the call just made: [(LOG_*, text)], oldest first."""
    out = [(r.line[i].level,
            ffi.string(r.line[i].text).decode("utf-8", "replace"))
           for i in range(r.lines)]
    if r.dropped:
        out.append((LOG_WARNING, f"{r.dropped} more lines of this call "
                                 f"did not fit in its report"))
    return out


def _check(rc, r, error):
    """Raise for a call that failed: MemoryError for no memory, else
    `error`, with the report's text."""
    if rc == 0:
        return
    if rc == _NO_MEMORY:
        raise MemoryError(_error(r))
    raise error(_error(r))


def _text_out(rc, r, out, error):
    """The text a call made in *out, as str; the core's copy is freed."""
    _check(rc, r, error)
    try:
        return ffi.string(out[0]).decode()
    finally:
        lib.sp_free(out[0])


def test_set_clock(wall, mono):
    """For tests only: the time the core sees, until the next call."""
    lib.sp_test_set_clock(1, wall, mono)


def test_system_clock():
    """For tests only: the core reads the system's clocks again."""
    lib.sp_test_set_clock(0, 0.0, 0.0)


class TicketError(ValueError):
    """The core refused a ticket; the text says which member and why."""


def _new_ticket():
    r, out = _report(), ffi.new("SpTicket **")
    _check(lib.sp_ticket_new(out, r), r, TicketError)
    return ffi.gc(out[0], lib.sp_ticket_free)


def _from_dict(ticket):
    """A core ticket from a ticket dict. `unfinished` is not taken: the
    core writes it from `files`. A member the core does not know is
    refused: the ticket's members are the core's list."""
    t = _new_ticket()
    r = _report()
    for key, value in ticket.items():
        if key in ("files", "unfinished"):
            continue
        if key not in TICKET_FIELDS:
            raise TicketError(f"ticket: no member named {key!r}")
        field, kind = TICKET_FIELDS[key]
        if value is None:
            continue
        if kind == _TEXT:
            if not isinstance(value, str):
                raise TicketError(f"ticket: {key} is text, and was given "
                                  f"{type(value).__name__}")
            rc = lib.sp_ticket_set_text(t, field, value.encode(), r)
        elif isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TicketError(f"ticket: {key} is a number, and was given "
                              f"{type(value).__name__}")
        elif isinstance(value, int):
            rc = lib.sp_ticket_set_integer(t, field, value, r)
        else:
            rc = lib.sp_ticket_set_number(t, field, value, r)
        _check(rc, r, TicketError)
    for f in ticket.get("files") or []:
        nums = [ffi.new("int64_t *", f[k]) if f.get(k) is not None else ffi.NULL
                for k in ("first_row", "final_rows", "rows")]
        _check(lib.sp_ticket_add_file(t, f["checkpoint"].encode(), nums[0],
                                      nums[1], nums[2], r), r, TicketError)
    return t


def _to_dict(t):
    """The ticket dict of a core ticket: every member the core writes,
    None for one that is unknown; `archive` only when it is known."""
    out = {}
    integer = ffi.new("int64_t *")
    number = ffi.new("double *")
    for key, (field, kind) in TICKET_FIELDS.items():
        if kind == _TEXT:
            s = lib.sp_ticket_text(t, field)
            value = None if s == ffi.NULL else ffi.string(s).decode()
        elif kind == _INTEGER:
            value = integer[0] if lib.sp_ticket_integer(t, field, integer) else None
        else:
            value = number[0] if lib.sp_ticket_number(t, field, number) else None
        if key == "archive" and value is None:
            continue
        out[key] = value
    files = []
    cp = ffi.new("const char **")
    vals = [ffi.new("int64_t *") for _ in range(3)]
    has = [ffi.new("int *") for _ in range(3)]
    for i in range(lib.sp_ticket_file_count(t)):
        lib.sp_ticket_file(t, i, cp, vals[0], has[0], vals[1], has[1],
                           vals[2], has[2])
        files.append({"checkpoint": ffi.string(cp[0]).decode(),
                      "first_row": vals[0][0] if has[0][0] else None,
                      "final_rows": vals[1][0] if has[1][0] else None,
                      "rows": vals[2][0] if has[2][0] else None})
    out["files"] = files
    out["unfinished"] = [f["checkpoint"] for f in files]
    return out


def ticket_text(ticket):
    """The JSON text of a ticket dict, as the core writes it."""
    t = _from_dict(ticket)
    r, out, n = _report(), ffi.new("char **"), ffi.new("size_t *")
    _check(lib.sp_ticket_write(t, out, n, r), r, TicketError)
    try:
        return ffi.buffer(out[0], n[0])[:].decode()
    finally:
        lib.sp_free(out[0])


def ticket_from_text(text):
    """The ticket dict of a ticket's JSON text (str or bytes), as the
    core reads it. Raises TicketError, naming the member, when the core
    refuses it."""
    raw = text.encode() if isinstance(text, str) else bytes(text)
    r, out = _report(), ffi.new("SpTicket **")
    _check(lib.sp_ticket_read(raw, len(raw), out, r), r, TicketError)
    try:
        return _to_dict(out[0])
    finally:
        lib.sp_ticket_free(out[0])


def ticket_stale_after(ticket, if_absent):
    """Seconds after its ts at which a ticket means a dead writer."""
    return lib.sp_ticket_stale_after(_from_dict(ticket), if_absent)


def ticket_is_stale(ticket, if_absent):
    """True when the ticket's writer is dead, by the wall clock now."""
    return bool(lib.sp_ticket_is_stale(_from_dict(ticket), if_absent))


# ---- the ownership decision (src/core/sp.h: THE OWNERSHIP DECISION) ----------

OWN, TAKE_OVER, REFUSED = 0, 1, 2
_REPORT_REFUSED = 2


def lock_record(pid, host, port, run):
    """The bytes a lock holder writes into its lock file."""
    r, out, n = _report(), ffi.new("char **"), ffi.new("size_t *")
    _check(lib.sp_lock_record(pid, str(host).encode(), port,
                              str(run).encode(), out, n, r), r, ValueError)
    try:
        return ffi.buffer(out[0], n[0])[:]
    finally:
        lib.sp_free(out[0])


def lock_refusal(who, lock_path, holder):
    """The refusal for a lock another process holds; `holder` is the
    bytes of the lock file."""
    r = _report()
    lib.sp_lock_refusal(who.encode(), str(lock_path).encode(), holder,
                        len(holder), r)
    return _error(r)


def ownership(who, name, store, ticket, host, if_absent):
    """The ticket check: (OWN | TAKE_OVER | REFUSED, message). ticket is
    the store's ticket dict, None for none. The message is the refusal,
    or the warning to log for a take-over."""
    t = _from_dict(ticket) if ticket is not None else ffi.NULL
    r, decision = _report(), ffi.new("int *")
    rc = lib.sp_ownership(who.encode(), name.encode(), str(store).encode(),
                          t, str(host).encode(), if_absent, decision, r)
    if rc == _REPORT_REFUSED:
        return REFUSED, _error(r)
    _check(rc, r, ValueError)
    return decision[0], " ".join(text for _, text in _lines(r))


def ownership_unread(name, store, why):
    """The warning for a ticket the host could not read."""
    r = _report()
    _check(lib.sp_ownership_unread(name.encode(), str(store).encode(),
                                   str(why).encode("utf-8", "replace"), r),
           r, ValueError)
    return " ".join(text for _, text in _lines(r))


# ---- the write plan (src/core/sp.h: THE WRITE PLAN) --------------------------

NOTHING, ROW_GROUP, CHECKPOINT = 0, 1, 2


class Writes:
    """One stream's write plan: which committed rows go to its files,
    and when. The caller's lock covers every call. The plan counts its
    times on the core's monotonic clock."""

    def __init__(self, row_group_rows, row_group_seconds, checkpoint_seconds):
        r, out = _report(), ffi.new("SpWrites **")
        _check(lib.sp_writes_new(out, r), r, ValueError)
        self._w = ffi.gc(out[0], lib.sp_writes_free)
        self._out = ffi.new("SpWrite *")
        self._begins = ffi.new("int *")
        self.set_options(row_group_rows, row_group_seconds, checkpoint_seconds)

    def set_options(self, row_group_rows, row_group_seconds, checkpoint_seconds):
        """The cadences from now on; None seconds = none."""
        r = _report()
        opt = lambda v: ffi.NULL if v is None else ffi.new("double *", v)
        _check(lib.sp_writes_set_options(self._w, row_group_rows,
                                         opt(row_group_seconds),
                                         opt(checkpoint_seconds), r),
               r, ValueError)

    def start(self, committed):
        lib.sp_writes_start(self._w, committed)

    def stop(self):
        lib.sp_writes_stop(self._w)

    def due(self, committed):
        return lib.sp_writes_due(self._w, committed)

    def commit(self, committed):
        """(the kind of write due, True when the pending rows begin now)"""
        kind = lib.sp_writes_commit(self._w, committed, self._begins)
        return kind, bool(self._begins[0])

    def plan(self, force_row_group, lo, hi):
        """(kind, lo, hi, lost) for a pin that holds [lo, hi)."""
        o = self._out
        lib.sp_writes_plan(self._w, int(force_row_group), lo, hi, o)
        return o.kind, o.lo, o.hi, o.lost

    @property
    def on(self):
        return bool(lib.sp_writes_on(self._w))

    @property
    def cursor(self):
        return lib.sp_writes_cursor(self._w)

    @property
    def planned(self):
        return lib.sp_writes_planned(self._w)

    @property
    def lost(self):
        return lib.sp_writes_lost(self._w)


# ---- what the store holds (src/core/sp.h: WHAT THE STORE HOLDS) --------------

class Store:
    """What the store holds of one stream's run, for its ticket, and
    when a file's old checkpoints are deleted. The caller's lock covers
    every call. The core stamps stored_at with its wall clock and counts
    the keep time on its monotonic clock."""

    def __init__(self, keep_seconds):
        r, out = _report(), ffi.new("SpStore **")
        _check(lib.sp_store_new(keep_seconds, out, r), r, ValueError)
        self._s = ffi.gc(out[0], lib.sp_store_free)

    @property
    def keep_seconds(self):
        return lib.sp_store_keep_seconds(self._s)

    @keep_seconds.setter
    def keep_seconds(self, seconds):
        r = _report()
        _check(lib.sp_store_set_keep_seconds(self._s, seconds, r), r,
               ValueError)

    def begins(self, file, first_row):
        r = _report()
        _check(lib.sp_store_begins(self._s, file.encode(), first_row, r),
               r, ValueError)

    def covers(self, file, version, checkpoint, final_rows, rows):
        r = _report()
        _check(lib.sp_store_covers(self._s, file.encode(), version,
                                   checkpoint.encode(), final_rows, rows, r),
               r, ValueError)

    def stored(self, file, version, checkpoint, rows):
        r = _report()
        _check(lib.sp_store_stored(self._s, file.encode(), version,
                                   checkpoint.encode(), rows, r),
               r, ValueError)

    def finished(self, file):
        r = _report()
        _check(lib.sp_store_finished(self._s, file.encode(), r), r, ValueError)

    def awaits(self, file):
        r = _report()
        _check(lib.sp_store_awaits(self._s, file.encode(), r), r, ValueError)

    def listed(self, file):
        """True when the file was awaiting its catalog: it leaves the
        ticket."""
        return bool(lib.sp_store_listed(self._s, file.encode()))

    def catalog(self, version):
        """The catalog version the ticket names from now on; in the same
        step as listed() for the files that version lists."""
        r = _report()
        _check(lib.sp_store_catalog(
            self._s, version.encode() if version is not None else ffi.NULL,
            r), r, ValueError)

    def dropped(self):
        return lib.sp_store_dropped(self._s)

    def due(self):
        """The files whose checkpoints are to delete now."""
        out, i = [], 0
        while True:
            p = lib.sp_store_due(self._s, i)
            if p == ffi.NULL:
                return out
            out.append(ffi.string(p).decode())
            i += 1

    def discarded(self, file):
        r = _report()
        _check(lib.sp_store_discarded(self._s, file.encode(), r), r,
               ValueError)

    def state(self, where=""):
        """(stored_row, stored_at, files, keep_seconds, catalog_etag) as
        the ticket holds them; each file's checkpoint is where + its
        object name."""
        t = _new_ticket()
        r = _report()
        _check(lib.sp_store_fill(self._s, t, where.encode(), r), r,
               TicketError)
        d = _to_dict(t)
        return (d["stored_row"], d["stored_at"], d["files"],
                d["keep_seconds"], d["catalog_etag"])

    @property
    def files(self):
        return lib.sp_store_files(self._s)


# ---- the reader (src/core/sp.h: THE READER) ----------------------------------

FOUND_NONE, FOUND_TICKET, FOUND_LOCAL, FOUND_UNREADABLE = 0, 1, 2, 3
DO_WAIT, DO_CONNECT, DO_LOCAL, DO_LIST, DO_FAIL = 0, 1, 2, 3, 4


class Step:
    """One step of a reader: what to do (DO_*), the state's name after
    it, whether to drop the ring in hand first, and for DO_WAIT how
    long."""
    __slots__ = ("action", "state", "fresh_ring", "wait_seconds")

    def __init__(self, st):
        self.action = st.action
        self.state = ffi.string(lib.sp_reader_state_name(st.state)).decode()
        self.fresh_ring = bool(st.fresh_ring)
        self.wait_seconds = st.wait_seconds


def _why(why):
    return (str(why).encode("utf-8", "replace") if why is not None
            else ffi.NULL)


class Reader:
    """A reader's rules: where it finds the writer, when it waits, when
    it starts again. One thread makes the calls (the reader's own); the
    properties may be read from any."""

    def __init__(self, who, name, store, direct):
        r, out = _report(), ffi.new("SpReader **")
        _check(lib.sp_reader_new(who.encode(), name.encode(),
                                 str(store).encode() if store is not None
                                 else ffi.NULL, 1 if direct else 0, out, r),
               r, ValueError)
        self._r = ffi.gc(out[0], lib.sp_reader_free)
        self._st = ffi.new("SpStep *")
        self._said = []

    def _step(self, rc, r):
        self._said += _lines(r)
        _check(rc, r, ValueError)
        return Step(self._st)

    @property
    def folder(self):
        return ffi.string(lib.sp_reader_folder(self._r)).decode()

    def poll(self, found, ticket, why, if_absent):
        t = (_from_dict(ticket) if found == FOUND_TICKET and ticket is not None
             else ffi.NULL)
        r = _report()
        return self._step(lib.sp_reader_poll(self._r, found, t, _why(why),
                                             if_absent, self._st, r), r)

    def folder_is(self, exists):
        r = _report()
        return self._step(lib.sp_reader_folder_is(
            self._r, 1 if exists else 0, self._st, r), r)

    def connected(self):
        lib.sp_reader_connected(self._r)

    def lost(self, why):
        r = _report()
        return self._step(lib.sp_reader_lost(self._r, _why(why), self._st, r),
                          r)

    def close(self):
        lib.sp_reader_close(self._r)

    @property
    def state(self):
        return ffi.string(lib.sp_reader_state_name(
            lib.sp_reader_state(self._r))).decode()

    @property
    def reason(self):
        """Why the reader is not streaming, in words; None while it is."""
        text = ffi.string(lib.sp_reader_reason(self._r)).decode(
            "utf-8", "replace")
        return text or None

    @property
    def run(self):
        p = lib.sp_reader_run(self._r)
        return ffi.string(p).decode() if p != ffi.NULL else None

    def messages(self):
        """The lines for the log that the steps since the last call
        made: [(LOG_*, text)]."""
        out, self._said = self._said, []
        return out


# ---- the view (src/core/sp.h: THE VIEW) ---------------------------------------

SEAM_WHY = (None, 'tight', 'short', 'late')
CATALOG_NONE, CATALOG_EXACT, CATALOG_CURRENT = 0, 1, 2


def _window(window):
    return ffi.new("int64_t[3]", list(window)) if window is not None else ffi.NULL


def view_seam(ticket, window):
    """(seam, use_ring, why) for a ticket dict (or None) and a ring's
    (oldest row, newest row, capacity) (or None): where a view changes
    from the store to the ring. why: None, 'tight', 'short' or 'late'."""
    use, why = ffi.new("int *"), ffi.new("int *")
    seam = lib.sp_view_seam(_from_dict(ticket) if ticket is not None
                            else ffi.NULL, _window(window), use, why)
    return seam, bool(use[0]), SEAM_WHY[why[0]]


#: The two row id columns (src/core/sp.h: ROW IDS).
ROW_ID_RUN = "sp_run_start_time"
ROW_ID_ROW = "sp_run_row_number"


def view_live_sql(view, memory):
    """The statement that makes a view of the ring alone with row ids."""
    r, out = _report(), ffi.new("char **")
    return _text_out(lib.sp_view_live_sql(view.encode(), memory.encode(),
                                          out, r), r, out, ValueError)


class ViewPlan:
    """What a look decided: build (the text must change), catalog
    (CATALOG_*), etag (for CATALOG_EXACT), seam, use_ring."""
    __slots__ = ("build", "catalog", "etag", "seam", "use_ring")


class View:
    """A view's rules: what its text names, the text, when to look
    again. The calls are made under the host's lock for the view."""

    def __init__(self, view, stream, memory, empty, row_ids=False):
        def text(s):
            return s.encode() if s is not None else ffi.NULL
        r, out = _report(), ffi.new("SpView **")
        _check(lib.sp_view_new(view.encode(), stream.encode(), text(memory),
                               text(empty), out, r), r, ValueError)
        self._v = ffi.gc(out[0], lib.sp_view_free)
        if row_ids:
            lib.sp_view_set_row_ids(self._v, 1)
        self._plan = ffi.new("SpViewPlan *")
        self._said = []

    def plan(self, ticket, files, ring_run, window, if_absent, keep_if_absent):
        """files: for each of the ticket's files, the path of the file
        its checkpoint belongs to."""
        keys = [ffi.new("char[]", f.encode()) for f in files]
        r = _report()
        rc = lib.sp_view_plan(
            self._v,
            _from_dict(ticket) if ticket is not None else ffi.NULL,
            ffi.new("const char *[]", keys) if keys else ffi.NULL,
            ring_run.encode() if ring_run is not None else ffi.NULL,
            _window(window), if_absent, keep_if_absent, self._plan, r)
        self._said += _lines(r)
        _check(rc, r, ValueError)
        out = ViewPlan()
        out.build = bool(self._plan.build)
        out.catalog = self._plan.catalog
        e = lib.sp_view_catalog_etag(self._v)
        out.etag = ffi.string(e).decode() if e != ffi.NULL else None
        out.seam = self._plan.seam
        out.use_ring = bool(self._plan.use_ring)
        return out

    def sql(self, catalog):
        """The statement for the plan; ValueError when there is nothing
        to read the columns from."""
        r, out = _report(), ffi.new("char **")
        return _text_out(lib.sp_view_sql(
            self._v, catalog.encode() if catalog is not None else ffi.NULL,
            out, r), r, out, ValueError)

    def built(self):
        lib.sp_view_built(self._v)

    @property
    def seam(self):
        return lib.sp_view_seam_row(self._v)

    @property
    def ring_note(self):
        return ffi.string(lib.sp_view_ring_note(self._v)).decode() or None

    def wait_seconds(self):
        return lib.sp_view_wait_seconds(self._v)

    def failed(self, why):
        """A look failed: the seconds to wait before the next."""
        r, wait = _report(), ffi.new("double *")
        rc = lib.sp_view_failed(self._v, _why(why), wait, r)
        self._said += _lines(r)
        _check(rc, r, ValueError)
        return wait[0]

    def followed(self):
        r = _report()
        rc = lib.sp_view_followed(self._v, r)
        self._said += _lines(r)
        _check(rc, r, ValueError)

    def messages(self):
        """The lines for the log that the calls since the last one
        made: [(LOG_*, text)]."""
        out, self._said = self._said, []
        return out


# ---- the catalog (src/core/sp.h: THE CATALOG) ----------------------------------

class CatalogError(ValueError):
    """The core refused a catalog's bytes or a file."""


def footer_length(tail):
    """The length of a parquet file's footer, from its last 8 bytes or
    more: the footer is that many bytes before those 8."""
    r, out = _report(), ffi.new("int64_t *")
    _check(lib.sp_parquet_footer_length(tail, len(tail), out, r), r,
           CatalogError)
    return out[0]


class Catalog:
    """A stream's catalog of finished files, in memory: opened from the
    bytes the store holds (b"": a new one), given each new file's
    footer, written back as bytes. One thread at a time."""

    def __init__(self, data, data_path):
        r, out = _report(), ffi.new("SpCatalog **")
        _check(lib.sp_catalog_open(data or ffi.NULL, len(data or b""),
                                   data_path.encode(), out, r), r,
               CatalogError)
        self._c = ffi.gc(out[0], lib.sp_catalog_free)

    def lists(self, path):
        return bool(lib.sp_catalog_lists(self._c, path.encode()))

    def files(self):
        """The full paths of the files the catalog lists."""
        r, p = _report(), ffi.new("char **")
        return [_text_out(lib.sp_catalog_file(self._c, i, p, r), r, p,
                          CatalogError)
                for i in range(lib.sp_catalog_files(self._c))]

    def add(self, path, file_size, footer):
        """List a finished file; CatalogError when it is refused (the
        catalog is as it was)."""
        r = _report()
        _check(lib.sp_catalog_add(self._c, path.encode(), file_size, footer,
                                  len(footer), r), r, CatalogError)

    @property
    def staged(self):
        return lib.sp_catalog_staged(self._c)

    def to_bytes(self):
        """The catalog's bytes, the files added since the last call in
        one snapshot, of the time of this call."""
        r, out, n = _report(), ffi.new("uint8_t **"), ffi.new("size_t *")
        _check(lib.sp_catalog_bytes(self._c, out, n, r), r, CatalogError)
        try:
            return bytes(ffi.buffer(out[0], n[0]))
        finally:
            lib.sp_free(out[0])
