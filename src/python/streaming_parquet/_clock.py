"""The library's wall clock -- one callable, injectable.

The library never generates DATA values (column timestamps are the
caller's); it generates only metadata times: file-open stamps and
ticket freshness. Those read this clock. Hosts with a disciplined
clock (PTP, replay) override it once at startup:

    import streaming_parquet as sp
    sp.set_clock(my_clock)     # any callable -> float seconds

Playback visibility uses this clock by default too;
create_playback_stream(clock=...) overrides per stream.
"""

import time as _time

_clock = _time.time


def set_clock(fn):
    global _clock
    _clock = fn


def now():
    return _clock()


def run_start():
    """A run's identity: the time this writer started the stream, UTC,
    to the microsecond, as text ("2026-10-26T20:31:12.123456Z"). The
    ticket carries it as "run" and every file it writes records it as
    sp_run; text of fixed width sorts in time order."""
    from datetime import datetime, timezone
    return datetime.fromtimestamp(now(), timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ")
