"""One writer per stream: the ownership check create_stream makes first.

Two checks, each covering what it can see:

  the lock file    a second process on THIS server. An exclusive lock on
                   <dir>/streams/<name>/writer.lock, held for the
                   stream's life; the operating system releases it the
                   moment the process ends, crash included. The file
                   holds the holder's pid, host and run, so a refusal
                   names who holds the stream.
  the ticket's IP  another SERVER. A fresh ticket naming a different
                   host means that server writes the stream; refused,
                   naming it. A stale one means that server is gone; the
                   writer takes over.

The decisions and their messages are the stream core's (src/core/sp.h, THE
OWNERSHIP DECISION), the same for every host language; this module makes
the lock file call and reads the ticket.

A writer that passes both owns the stream: every leftover checkpoint
under it is its own previous run's, and its start sweep finishes them
all, whatever their age.

The lock lives on local disk: in a directory store's own directory, else
in the spool (the default spool when the stream has none).
"""

import fcntl
import os

from . import _core, _tickets
from ._diag import _diag


class Ownership:
    """The held lock. release() at close; the operating system releases
    it anyway when the process ends."""

    def __init__(self, path, fd):
        self.path = path
        self._fd = fd

    def release(self):
        if self._fd is not None:
            os.close(self._fd)            # closing the descriptor drops the lock
            self._fd = None


def claim(name, lock_dir, store, host, port, run, who=None):
    """Own the stream `name`, or raise naming who does.

    lock_dir: the local directory the lock file lives under.
    store:    the stream's store, for the ticket check; None = no ticket.
    host, port, run: this writer's identity, written into the lock file.
    """
    who = who or f"create_stream({name!r})"
    path = os.path.join(lock_dir, "streams", name, "writer.lock")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        holder = os.pread(fd, 4096, 0)
        os.close(fd)
        raise RuntimeError(_core.lock_refusal(who, path, holder)) from None
    os.ftruncate(fd, 0)
    os.pwrite(fd, _core.lock_record(os.getpid(), host, port, run), 0)
    owned = Ownership(path, fd)
    if store is None:
        return owned

    try:
        ticket = _tickets.get_ticket(store, name)
    except Exception as e:
        # The ticket check needs the store; an unreachable bucket at start
        # delays the mirror and never the stream. This server's lock still
        # holds; the other-server check runs again at the next start.
        _diag.warning(_core.ownership_unread(name, store, e))
        return owned
    decision, message = _core.ownership(who, name, store, ticket, host,
                                        _tickets.stale_after({}))
    if decision == _core.REFUSED:
        owned.release()
        raise RuntimeError(message)
    if decision == _core.TAKE_OVER:
        _diag.warning(message)
    return owned
