"""CFFI binding of the stream core: core/sp.c, whose header sp.h is the
spec.

The core holds the rules of streaming-parquet that every host language
shares, written once: formats and decisions, no input or output, no
clock. This module is what the Python host calls; the Go host
(go/streaming) calls the same core. A rule that is in the core is in no
host.

The library is built with `make` and the system C compiler the first
time this module is imported, next to its sources (core/), like the
ring's and the checkpoint core's.
"""

import os
import subprocess

import cffi

_CORE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'core')
_LIB_PATH = os.path.join(_CORE_DIR, 'libsp.dylib')
_ERRLEN = 256

ffi = cffi.FFI()
ffi.cdef("""
typedef struct SpTicket SpTicket;
const char *sp_ticket_field_name(int field);
int sp_ticket_field_kind(int field);
SpTicket *sp_ticket_new(void);
void sp_ticket_free(SpTicket *t);
int sp_ticket_set_text(SpTicket *t, int field, const char *text,
                       char *err, size_t errlen);
int sp_ticket_set_integer(SpTicket *t, int field, int64_t value,
                          char *err, size_t errlen);
int sp_ticket_set_number(SpTicket *t, int field, double value,
                         char *err, size_t errlen);
int sp_ticket_clear(SpTicket *t, int field);
int sp_ticket_add_file(SpTicket *t, const char *checkpoint,
                       const int64_t *first_row, const int64_t *final_rows,
                       const int64_t *rows, char *err, size_t errlen);
const char *sp_ticket_text(const SpTicket *t, int field);
int sp_ticket_integer(const SpTicket *t, int field, int64_t *out);
int sp_ticket_number(const SpTicket *t, int field, double *out);
int32_t sp_ticket_file_count(const SpTicket *t);
int sp_ticket_file(const SpTicket *t, int32_t index, const char **checkpoint,
                   int64_t *first_row, int *has_first_row,
                   int64_t *final_rows, int *has_final_rows,
                   int64_t *rows, int *has_rows);
char *sp_ticket_write(const SpTicket *t, size_t *out_len);
SpTicket *sp_ticket_read(const char *text, size_t len,
                         char *err, size_t errlen);
double sp_ticket_stale_after(const SpTicket *t, double if_absent);
int sp_ticket_is_stale(const SpTicket *t, double now, double if_absent);
void sp_free(void *p);
char *sp_lock_record(int64_t pid, const char *host, int64_t port,
                     const char *run, size_t *out_len);
void sp_lock_refusal(const char *who, const char *lock_path,
                     const char *holder, size_t holder_len,
                     char *msg, size_t msglen);
int sp_ownership(const char *who, const char *name, const char *store,
                 const SpTicket *ticket, const char *host,
                 double now, double if_absent, char *msg, size_t msglen);
typedef struct SpWrites SpWrites;
typedef struct { int kind; int64_t lo, hi; int64_t lost; } SpWrite;
SpWrites *sp_writes_new(void);
void sp_writes_free(SpWrites *w);
int sp_writes_set_options(SpWrites *w, int64_t row_group_rows,
                          const double *row_group_seconds,
                          const double *checkpoint_seconds,
                          char *err, size_t errlen);
void sp_writes_start(SpWrites *w, int64_t committed);
void sp_writes_stop(SpWrites *w);
int sp_writes_due(const SpWrites *w, int64_t committed, double now);
int sp_writes_commit(SpWrites *w, int64_t committed, double now,
                     int *begins);
void sp_writes_plan(SpWrites *w, int force_row_group, int64_t lo,
                    int64_t hi, double now, SpWrite *out);
int sp_writes_on(const SpWrites *w);
int64_t sp_writes_cursor(const SpWrites *w);
int64_t sp_writes_planned(const SpWrites *w);
int64_t sp_writes_lost(const SpWrites *w);
typedef struct SpStore SpStore;
SpStore *sp_store_new(double keep_seconds);
void sp_store_free(SpStore *s);
int sp_store_set_keep_seconds(SpStore *s, double keep_seconds);
double sp_store_keep_seconds(const SpStore *s);
int sp_store_begins(SpStore *s, const char *file, int64_t first_row);
int sp_store_covers(SpStore *s, const char *file, int64_t version,
                    const char *checkpoint, int64_t final_rows, int64_t rows);
int sp_store_stored(SpStore *s, const char *file, int64_t version,
                    const char *checkpoint, int64_t rows, double wall);
int sp_store_finished(SpStore *s, const char *file);
int sp_store_awaits(SpStore *s, const char *file);
int sp_store_listed(SpStore *s, const char *file);
int sp_store_dropped(SpStore *s, double now);
const char *sp_store_due(const SpStore *s, double now, int32_t index);
int sp_store_discarded(SpStore *s, const char *file);
int64_t sp_store_stored_row(const SpStore *s);
int sp_store_stored_at(const SpStore *s, double *wall);
int sp_store_fill(const SpStore *s, SpTicket *t, const char *where,
                  char *err, size_t errlen);
int32_t sp_store_files(const SpStore *s);
void sp_ownership_unread(const char *name, const char *store,
                         const char *why, char *msg, size_t msglen);
""")


def _build_if_stale():
    sources = [os.path.join(_CORE_DIR, n) for n in ('sp.c', 'sp.h')]
    if (os.path.exists(_LIB_PATH) and os.path.getmtime(_LIB_PATH)
            >= max(os.path.getmtime(s) for s in sources)):
        return
    subprocess.run(['make', '-C', _CORE_DIR, 'libsp.dylib'], check=True,
                   capture_output=True)


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


class TicketError(ValueError):
    """The core refused a ticket; the text says which member and why."""


def _err():
    return ffi.new("char[]", _ERRLEN)


def _raise(err):
    raise TicketError(ffi.string(err).decode(errors="replace"))


def _from_dict(ticket):
    """A core ticket from a ticket dict. `unfinished` is not taken: the
    core writes it from `files`. A member the core does not know is
    refused: the ticket's members are the core's list."""
    t = ffi.gc(lib.sp_ticket_new(), lib.sp_ticket_free)
    if t == ffi.NULL:
        raise MemoryError("ticket")
    err = _err()
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
            rc = lib.sp_ticket_set_text(t, field, value.encode(), err, _ERRLEN)
        elif isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TicketError(f"ticket: {key} is a number, and was given "
                              f"{type(value).__name__}")
        elif isinstance(value, int):
            rc = lib.sp_ticket_set_integer(t, field, value, err, _ERRLEN)
        else:
            rc = lib.sp_ticket_set_number(t, field, value, err, _ERRLEN)
        if rc:
            _raise(err)
    for f in ticket.get("files") or []:
        nums = [ffi.new("int64_t *", f[k]) if f.get(k) is not None else ffi.NULL
                for k in ("first_row", "final_rows", "rows")]
        if lib.sp_ticket_add_file(t, f["checkpoint"].encode(), nums[0],
                                  nums[1], nums[2], err, _ERRLEN):
            _raise(err)
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
    n = ffi.new("size_t *")
    p = lib.sp_ticket_write(t, n)
    if p == ffi.NULL:
        raise MemoryError("ticket text")
    try:
        return ffi.buffer(p, n[0])[:].decode()
    finally:
        lib.sp_free(p)


def ticket_from_text(text):
    """The ticket dict of a ticket's JSON text (str or bytes), as the
    core reads it. Raises TicketError, naming the member, when the core
    refuses it."""
    raw = text.encode() if isinstance(text, str) else bytes(text)
    err = _err()
    t = lib.sp_ticket_read(raw, len(raw), err, _ERRLEN)
    if t == ffi.NULL:
        _raise(err)
    try:
        return _to_dict(t)
    finally:
        lib.sp_ticket_free(t)


def ticket_stale_after(ticket, if_absent):
    """Seconds after its ts at which a ticket means a dead writer."""
    return lib.sp_ticket_stale_after(_from_dict(ticket), if_absent)


def ticket_is_stale(ticket, now, if_absent):
    """True when, at `now`, the ticket's writer is dead."""
    return bool(lib.sp_ticket_is_stale(_from_dict(ticket), now, if_absent))


# ---- the ownership decision (core/sp.h: THE OWNERSHIP DECISION) ----------

OWN, TAKE_OVER, REFUSED = 0, 1, 2
_MSGLEN = 2048


def lock_record(pid, host, port, run):
    """The bytes a lock holder writes into its lock file."""
    n = ffi.new("size_t *")
    p = lib.sp_lock_record(pid, str(host).encode(), port, str(run).encode(), n)
    if p == ffi.NULL:
        raise MemoryError("lock record")
    try:
        return ffi.buffer(p, n[0])[:]
    finally:
        lib.sp_free(p)


def lock_refusal(who, lock_path, holder):
    """The refusal for a lock another process holds; `holder` is the
    bytes of the lock file."""
    msg = ffi.new("char[]", _MSGLEN)
    lib.sp_lock_refusal(who.encode(), str(lock_path).encode(), holder,
                        len(holder), msg, _MSGLEN)
    return ffi.string(msg).decode("utf-8", "replace")


def ownership(who, name, store, ticket, host, now, if_absent):
    """The ticket check: (OWN | TAKE_OVER | REFUSED, message). ticket is
    the store's ticket dict, None for none."""
    msg = ffi.new("char[]", _MSGLEN)
    t = _from_dict(ticket) if ticket is not None else ffi.NULL
    got = lib.sp_ownership(who.encode(), name.encode(), str(store).encode(),
                           t, str(host).encode(), now, if_absent,
                           msg, _MSGLEN)
    return got, ffi.string(msg).decode("utf-8", "replace")


def ownership_unread(name, store, why):
    """The warning for a ticket the host could not read."""
    msg = ffi.new("char[]", _MSGLEN)
    lib.sp_ownership_unread(name.encode(), str(store).encode(),
                            str(why).encode(), msg, _MSGLEN)
    return ffi.string(msg).decode("utf-8", "replace")


# ---- the write plan (core/sp.h: THE WRITE PLAN) --------------------------

NOTHING, ROW_GROUP, CHECKPOINT = 0, 1, 2


class Writes:
    """One stream's write plan: which committed rows go to its files,
    and when. The caller's lock covers every call; `now` is
    time.monotonic()."""

    def __init__(self, row_group_rows, row_group_seconds, checkpoint_seconds):
        self._w = ffi.gc(lib.sp_writes_new(), lib.sp_writes_free)
        if self._w == ffi.NULL:
            raise MemoryError("write plan")
        self._out = ffi.new("SpWrite *")
        self._begins = ffi.new("int *")
        self.set_options(row_group_rows, row_group_seconds, checkpoint_seconds)

    def set_options(self, row_group_rows, row_group_seconds, checkpoint_seconds):
        """The cadences from now on; None seconds = none."""
        err = _err()
        opt = lambda v: ffi.NULL if v is None else ffi.new("double *", v)
        if lib.sp_writes_set_options(self._w, row_group_rows,
                                     opt(row_group_seconds),
                                     opt(checkpoint_seconds),
                                     err, _ERRLEN) != 0:
            raise ValueError(ffi.string(err).decode(errors="replace"))

    def start(self, committed):
        lib.sp_writes_start(self._w, committed)

    def stop(self):
        lib.sp_writes_stop(self._w)

    def due(self, committed, now):
        return lib.sp_writes_due(self._w, committed, now)

    def commit(self, committed, now):
        """(the kind of write due, True when the pending rows begin now)"""
        kind = lib.sp_writes_commit(self._w, committed, now, self._begins)
        return kind, bool(self._begins[0])

    def plan(self, force_row_group, lo, hi, now):
        """(kind, lo, hi, lost) for a pin that holds [lo, hi)."""
        o = self._out
        lib.sp_writes_plan(self._w, int(force_row_group), lo, hi, now, o)
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


# ---- what the store holds (core/sp.h: WHAT THE STORE HOLDS) --------------

class Store:
    """What the store holds of one stream's run, for its ticket, and
    when a file's old checkpoints are deleted. The caller's lock covers
    every call. `wall` is the time the ticket shows; `now` is
    time.monotonic()."""

    def __init__(self, keep_seconds):
        self._s = ffi.gc(lib.sp_store_new(keep_seconds), lib.sp_store_free)
        if self._s == ffi.NULL:
            raise ValueError(f"store: keep_seconds {keep_seconds!r}")

    @staticmethod
    def _ok(rc):
        if rc != 0:
            raise MemoryError("store")

    @property
    def keep_seconds(self):
        return lib.sp_store_keep_seconds(self._s)

    @keep_seconds.setter
    def keep_seconds(self, seconds):
        if lib.sp_store_set_keep_seconds(self._s, seconds) != 0:
            raise ValueError(f"store: keep_seconds {seconds!r}")

    def begins(self, file, first_row):
        self._ok(lib.sp_store_begins(self._s, file.encode(), first_row))

    def covers(self, file, version, checkpoint, final_rows, rows):
        self._ok(lib.sp_store_covers(self._s, file.encode(), version,
                                     checkpoint.encode(), final_rows, rows))

    def stored(self, file, version, checkpoint, rows, wall):
        self._ok(lib.sp_store_stored(self._s, file.encode(), version,
                                     checkpoint.encode(), rows, wall))

    def finished(self, file):
        self._ok(lib.sp_store_finished(self._s, file.encode()))

    def awaits(self, file):
        self._ok(lib.sp_store_awaits(self._s, file.encode()))

    def listed(self, file):
        """True when the file was awaiting its catalog: it leaves the
        ticket."""
        return bool(lib.sp_store_listed(self._s, file.encode()))

    def dropped(self, now):
        return lib.sp_store_dropped(self._s, now)

    def due(self, now):
        """The files whose checkpoints are to delete now."""
        out, i = [], 0
        while True:
            p = lib.sp_store_due(self._s, now, i)
            if p == ffi.NULL:
                return out
            out.append(ffi.string(p).decode())
            i += 1

    def discarded(self, file):
        self._ok(lib.sp_store_discarded(self._s, file.encode()))

    def state(self, where=""):
        """(stored_row, stored_at, files, keep_seconds) as the ticket
        holds them; each file's checkpoint is where + its object name."""
        t = ffi.gc(lib.sp_ticket_new(), lib.sp_ticket_free)
        err = _err()
        if lib.sp_store_fill(self._s, t, where.encode(), err, _ERRLEN) != 0:
            _raise(err)
        d = _to_dict(t)
        return d["stored_row"], d["stored_at"], d["files"], d["keep_seconds"]

    @property
    def files(self):
        return lib.sp_store_files(self._s)
