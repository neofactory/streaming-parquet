"""Shared per-stream data object: the arrow_ring Ring + read API +
lifecycle flag.

Owned by exactly one of:
  - A local Stream (its writes feed the ring).
  - A remote Attached's receive thread (Flight batches feed the ring).

Referenced by zero or more Attached read faces. Reads are lock-free
against writes (the ring's committed cursor is an atomic); the only way
a reader gets stale data is falling more than `capacity` rows behind,
which raises StaleReadError. There is NO back-pressure on the producer
from readers that use read_row (unpinned); a held static relation CAN
stall the producer via the ring's pin — that is deliberate and loud
(PinStalledError names the overdue lease), and on an origin ring it is
fatal territory. Producers run free; consumers keep up, skip ahead, or
get named in the stall message. Trade chosen because a silently stalled
producer on a machine connector would blow its fieldbus deadlines.

PLAYBACK STREAMS (replay mode) — set via set_playback_clock(col, fn):
  The stream marks itself "time-gated on read." Readers only see rows
  whose `row[col] <= fn()`. The publisher pre-stocks the ring in one
  bulk write at startup; the reader-side clamp keeps future rows
  invisible until the clock catches up. No worker thread spinning.
"""

import threading

import arrow_ring
from arrow_ring import StaleReadError

# A full ring evicts its oldest row on every allocation. A seq search that
# peeks the oldest edge can lose a microsecond race to that eviction
# (StaleReadError). That is transient -- re-reading the bounds and searching
# again converges at once (committed rows are immutable; only the oldest
# edge moved). 8 consecutive races is statistically impossible, so reaching
# the cap means a genuine problem.
_SEQ_SEARCH_RETRIES = 8


class _StreamData:
    """The data backing a telemetry stream: one arrow_ring Ring plus the
    playback clamp and the seq-search helpers. One per stream-name per
    process; held jointly by the producer and every Attached."""

    __slots__ = ("name", "schema", "ring", "_closed", "_lock",
                 "_playback_clock")

    def __init__(self, name, schema, capacity, rate_hint=None,
                 segment_bytes=None):
        self.name = name
        self.schema = schema
        self.ring = arrow_ring.Ring(schema, capacity, rate_hint=rate_hint,
                                    segment_bytes=segment_bytes)
        self._closed = False
        self._lock = threading.Lock()  # only for the closed flag
        # Optional playback clock: (timestamp_col_name, clock_fn); None
        # for live streams. When set, reader visibility clamps to rows
        # with row[col] <= clock_fn(). See module docstring.
        self._playback_clock = None

    def set_playback_clock(self, timestamp_col_name, clock_fn):
        """Mark this stream as a playback stream (see module docstring)."""
        if timestamp_col_name not in self.schema.names:
            raise ValueError(
                f"set_playback_clock: column {timestamp_col_name!r} not in "
                f"stream schema fields {self.schema.names}")
        self._playback_clock = (timestamp_col_name, clock_fn)

    # --- read API (Attached faces call these) ---------------------------

    @property
    def oldest_seq(self):
        return self.ring.oldest_seq

    @property
    def newest_seq(self):
        """Highest committed seq (absolute, ignoring any playback clock)."""
        return self.ring.newest_seq

    def read_row(self, seq):
        """Read-only row view; playback-clamped when a clock is set."""
        if self._playback_clock is not None and seq > self.newest_visible_seq():
            raise StaleReadError(
                f"seq {seq} not yet visible (playback clock)")
        return self.ring.read_row(seq)

    def get_static_relation(self, duration_s):
        """Pinned record set over the visible window."""
        rs = self.ring.get_static_relation(duration_s)
        if self._playback_clock is not None:
            visible_hi = self.newest_visible_seq() + 1
            if visible_hi < rs.hi:
                rs.clamp_hi(visible_hi)
        return rs

    def pin(self, lo, hi):
        """Pinned record set over exactly [lo, hi), clamped to the
        visible window: the publish and archive paths' pin."""
        rs = self.ring.pin(lo, hi)
        if self._playback_clock is not None:
            visible_hi = self.newest_visible_seq() + 1
            if visible_hi < rs.hi:
                rs.clamp_hi(visible_hi)
        return rs

    # --- visibility (playback-aware) -----------------------------------

    def newest_visible_seq(self):
        """Largest seq currently visible to readers.
        - Live (no playback clock): newest_seq.
        - Playback: binary-search for the largest seq whose
          row[timestamp_col] <= clock_fn(); -1 when nothing visible.
        O(log capacity), no copies. Relies on timestamps being
        non-decreasing in seq order (true for tv/arrival-ordered rows)."""
        if self._playback_clock is None:
            return self.ring.newest_seq
        newest = self.ring.newest_seq
        if newest < 0:
            return -1
        ts_col, clock_fn = self._playback_clock
        target = float(clock_fn())
        oldest = self.ring.oldest_seq
        if float(getattr(self.ring.read_row(oldest), ts_col)) > target:
            return -1
        if float(getattr(self.ring.read_row(newest), ts_col)) <= target:
            return newest
        lo, hi = oldest, newest
        while lo < hi:
            mid = (lo + hi + 1) // 2
            if float(getattr(self.ring.read_row(mid), ts_col)) <= target:
                lo = mid
            else:
                hi = mid - 1
        return lo

    def seq_at_or_after(self, column, value):
        """First currently-visible seq whose row[column] >= value, or -1
        when the stream is empty or has not reached `value` yet. `column`
        must be non-decreasing in seq order (tv, arrival).

        Race-safe against the producer: a full ring evicts its oldest row
        on every allocation, so a binary search can have a peeked seq
        evicted out from under it (StaleReadError). That is TRANSIENT --
        re-read the bounds and search again; committed rows are immutable
        and only the oldest edge advances, so a fresh pass converges
        immediately. Handled HERE so callers never mistake the race for
        a missing window."""
        value = float(value)
        for _ in range(_SEQ_SEARCH_RETRIES):
            try:
                lo = self.ring.oldest_seq
                hi = self.newest_visible_seq()
                if hi < 0 or float(getattr(self.ring.read_row(hi), column)) < value:
                    return -1
                if float(getattr(self.ring.read_row(lo), column)) >= value:
                    return lo
                while hi - lo > 1:
                    mid = (lo + hi) // 2
                    if float(getattr(self.ring.read_row(mid), column)) >= value:
                        hi = mid
                    else:
                        lo = mid
                return hi
            except StaleReadError:
                continue   # producer evicted a peeked seq mid-search
        return -1

    # --- lifecycle ------------------------------------------------------

    def mark_closed(self):
        with self._lock:
            self._closed = True

    @property
    def closed(self):
        return self._closed
