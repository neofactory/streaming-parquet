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
