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

A writer that passes both owns the stream: every leftover checkpoint
under it is its own previous run's, and its start sweep finishes them
all, whatever their age.

The lock lives on local disk: in a directory store's own directory, else
in the spool (the default spool when the stream has none).
"""

import fcntl
import json
import os
import time

from . import _tickets
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


def claim(name, lock_dir, store, host, port, run):
    """Own the stream `name`, or raise naming who does.

    lock_dir: the local directory the lock file lives under.
    store:    the stream's store, for the ticket check; None = no ticket.
    host, port, run: this writer's identity, written into the lock file.
    A ticket is a dead writer's after its own stale_after
    (_tickets.stale_after).
    """
    path = os.path.join(lock_dir, "streams", name, "writer.lock")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        holder = _holder(fd)
        os.close(fd)
        raise RuntimeError(
            f"create_stream({name!r}): another process on this server "
            f"writes this stream: pid {holder.get('pid', '?')}, run "
            f"{holder.get('run', '?')} holds {path}. One writer per "
            f"stream: stop that process first.") from None
    me = json.dumps({"pid": os.getpid(), "host": host, "port": port,
                     "run": run}).encode()
    os.ftruncate(fd, 0)
    os.pwrite(fd, me, 0)
    owned = Ownership(path, fd)
    if store is None:
        return owned

    try:
        ticket = _tickets.get_ticket(store, name)
    except Exception as e:
        # The ticket check needs the store; an unreachable bucket at start
        # delays the mirror and never the stream. This server's lock still
        # holds; the other-server check runs again at the next start.
        _diag.warning(
            f"[{name}] ownership: the ticket in {store} could not be "
            f"read ({e}); starting on this server's lock alone -- another "
            f"server writing this stream would not be seen")
        return owned
    if ticket is None or ticket.get("host") == host:
        return owned
    age = time.time() - float(ticket.get("ts", 0))
    stale_s = _tickets.stale_after(ticket)
    who = f"{ticket.get('host')}:{ticket.get('port')}"
    if age < stale_s:
        owned.release()
        raise RuntimeError(
            f"create_stream({name!r}): the server at {who} writes this "
            f"stream: its ticket in {store} is {age:.0f}s old (run "
            f"{ticket.get('run', '?')}; a ticket is stale after "
            f"{stale_s:.0f}s). One writer per stream: stop it there, or "
            f"wait until its ticket is stale.")
    _diag.warning(
        f"[{name}] taking over from {who}: its ticket in {store} is "
        f"{age:.0f}s old (run {ticket.get('run', '?')}); stale after "
        f"{stale_s:.0f}s")
    return owned


def _holder(fd):
    try:
        return json.loads(os.pread(fd, 4096, 0) or b"{}")
    except ValueError:
        return {}
