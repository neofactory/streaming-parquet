"""The process stall monitor: one thread that waits half a second, over
and over, and reports when a wait comes back more than a second late.

A late wake means the whole process was held: another thread kept the
Python lock (a C call that does not release it), or the process was
paused. Its timestamps line up with other slow things in the log (an
S3 upload taking 5 s in one attempt, 2026-10-27 on the box). Two wakes
a second, nothing else.
"""

import threading
import time

from ._diag import _diag

INTERVAL_S = 0.5
REPORT_S = 1.0

_started = False
_lock = threading.Lock()


def start():
    """Start the monitor once per process."""
    global _started
    with _lock:
        if _started:
            return
        _started = True
    threading.Thread(target=_loop, name="stall-monitor", daemon=True).start()


def _loop():
    tick = threading.Event()             # never set: a plain timed wait
    while True:
        t0 = time.monotonic()
        tick.wait(INTERVAL_S)
        took = time.monotonic() - t0
        if took - INTERVAL_S >= REPORT_S:
            _diag.warning(f"process stall: a {INTERVAL_S} s wait took "
                          f"{took:.1f} s -- the whole process was held "
                          f"(another thread kept the Python lock, or the "
                          f"process was paused)")
